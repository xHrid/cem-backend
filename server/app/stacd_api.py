"""
STACD / Airflow synchronous algorithm API — the ONLY API surface.

Model:
  * The web app uploads audio DIRECTLY to the server (POST /api/v1/datasets/audio).
    That call mints a fresh job_id, saves the WAVs under
    data/<job_id>/input/audio/, and returns the job_id.
  * The front-end hands that job_id to an Airflow layer. Airflow runs each DAG
    node by calling ONE algorithm endpoint
        POST /api/v1/jobs/{job_id}/{algo}
    and BLOCKS until it returns — the HTTP response IS the completion signal
    (synchronous; no polling).
  * Each pipeline step is one algorithm node. SEVEN explicit named wrappers, one
    per algorithm, plus a generic fallback:

        POST /api/v1/jobs/{job_id}/birdnet
        POST /api/v1/jobs/{job_id}/heatmaps
        POST /api/v1/jobs/{job_id}/temporal_stickiness
        POST /api/v1/jobs/{job_id}/spatial_stickiness
        POST /api/v1/jobs/{job_id}/migratory_classification
        POST /api/v1/jobs/{job_id}/solar_correlation
        POST /api/v1/jobs/{job_id}/daily_timeseries
        POST /api/v1/jobs/{job_id}/{algo}      (generic, registered last)

    birdnet reads the job's audio dir and appends to the job's aggregate; the six
    analyses read that aggregate.

Responses match the STACD contract:
    200 -> {status, Success, message, task_id, asset_id[, asset_ids], stac}
    400 -> bad input        (skipped)        {status, error, message, task_id}
    404 -> no data / no job (skipped)        {status, error, message, task_id}
    500 -> pipeline failure  (failed)        {status, error, message, task_id}

Read-only endpoints (job summary, results, logs, downloads) are kept for
debugging and result retrieval; they live under the same /api/v1/jobs/{job_id}.
"""
import os
import uuid
import zipfile
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Body, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse

from . import jobs as jobstore
from . import pipeline_meta as meta
from . import runner
from . import stac
from .settings import get_settings

router = APIRouter(prefix="/api/v1", tags=["stacd"])

# Non-audio single-file override kinds (optional; uploaded into a job's input/).
_FIXED_NAMES = {
    "aggregate": "aggregate.csv",
    "processed": "processed_files.txt",
    "ebird": "ebird_checklist.txt",
    "static_noise": "static_noise.wav",
    "rain_noise": "rain_noise.wav",
}


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _require_job(job_id: str) -> jobstore.Job:
    job = jobstore.get_job(job_id)
    if job is None:
        raise HTTPException(404, f"Job '{job_id}' not found.")
    return job


def _safe_name(name: str) -> str:
    base = os.path.basename(name or "").strip()
    if not base or base in (".", ".."):
        raise HTTPException(400, "Invalid filename.")
    return base


def _save_upload(dest: Path, upload: UploadFile) -> int:
    s = get_settings()
    dest.parent.mkdir(parents=True, exist_ok=True)
    max_bytes = s.MAX_UPLOAD_MB * 1024 * 1024
    written = 0
    with open(dest, "wb") as out:
        while True:
            chunk = upload.file.read(1024 * 1024)
            if not chunk:
                break
            written += len(chunk)
            if written > max_bytes:
                out.close()
                dest.unlink(missing_ok=True)
                raise HTTPException(413, f"Upload exceeds {s.MAX_UPLOAD_MB} MB limit.")
            out.write(chunk)
    return written


# --------------------------------------------------------------------------- #
# steps catalogue
# --------------------------------------------------------------------------- #
@router.get("/steps")
def steps():
    """Catalogue of runnable algorithms — full manifest entries so the UI can
    render script-specific parameters, inputs, and dependency info."""
    man = meta.load_manifest()
    return {
        sid: {
            "name": man.get(sid, {}).get("name", sid),
            "description": man.get(sid, {}).get("description", ""),
            "is_analysis": meta.is_analysis(sid),
            "depends_on": man.get(sid, {}).get("depends_on", []),
            "parameters": man.get(sid, {}).get("parameters", []),
            "inputs": man.get(sid, {}).get("inputs", []),
            "aggregate_file": man.get(sid, {}).get("aggregate_file", ""),
            "version": meta.step_version(sid),
            "api_version": get_settings().API_VERSION,
        }
        for sid in meta.SCRIPTS
    }


# --------------------------------------------------------------------------- #
# Upload: mint a job and save audio into it (front-end calls this directly)
# --------------------------------------------------------------------------- #
@router.post("/datasets/audio")
def upload_audio(files: list[UploadFile] = File(...)):
    """Create a fresh job and upload WAVs into data/<job_id>/input/audio/.

    Returns the minted job_id; the front-end hands this to Airflow, which then
    calls POST /api/v1/jobs/{job_id}/{algo}."""
    if not files:
        raise HTTPException(400, "No files provided.")
    job = jobstore.create_job()
    saved = []
    for up in files:
        name = _safe_name(up.filename)
        size = _save_upload(job.audio_dir / name, up)
        saved.append({"filename": name, "size_bytes": size})
    return {
        "status": "ok",
        "job_id": job.id,
        "uploaded": saved,
        "audio_dir": str(job.audio_dir),
    }


@router.post("/jobs/{job_id}/datasets/{kind}")
def upload_override(job_id: str, kind: str, files: list[UploadFile] = File(...)):
    """Optional: add more audio, or a single-file override to an existing job.

    kind = audio | aggregate | processed | ebird | static_noise | rain_noise.
    audio appends WAVs; the override kinds save to a fixed name in input/."""
    job = _require_job(job_id)
    if kind != "audio" and kind not in _FIXED_NAMES:
        raise HTTPException(400, f"Unknown kind '{kind}'. One of: "
                                 f"{['audio', *sorted(_FIXED_NAMES)]}")
    if not files:
        raise HTTPException(400, "No files provided.")
    saved = []
    for up in files:
        original = _safe_name(up.filename)
        if kind == "audio":
            dest = job.audio_dir / original
        else:
            dest = job.input_dir / _FIXED_NAMES[kind]
        size = _save_upload(dest, up)
        saved.append({"filename": original, "kind": kind, "size_bytes": size})
    return {"status": "ok", "job_id": job.id, "uploaded": saved}


# --------------------------------------------------------------------------- #
# Shared helpers for the algorithm runners
# --------------------------------------------------------------------------- #
def _parse_params(p: dict, step: str | None = None) -> tuple[dict, list]:
    """Pull pipeline params + spot geolocation out of the request body.

    Airflow sends every DAG param as a string, so ``spots`` may arrive
    comma-separated; normalize to a list. When a tunable is absent from the
    request we leave it out — the script falls through to config.py defaults.
    The frontend SHOULD always send explicit values (populated from the
    manifest's default field), but the API never breaks on an empty body."""
    rp: dict = {}
    spots = p.get("spots")
    if isinstance(spots, str):
        spots = [x for x in spots.split(",") if x]
    if spots:
        rp["spots"] = spots
    if p.get("start_date"):
        rp["start_date"] = str(p["start_date"])
    if p.get("end_date"):
        rp["end_date"] = str(p["end_date"])
    if p.get("snr_db") not in (None, ""):
        rp["snr_db"] = p["snr_db"]
    for k in ("min_confidence", "top_n_species", "top_n_temporal",
              "sci_threshold", "kurtosis_threshold", "pmr_threshold",
              "window_size", "min_solar_days", "max_timeseries_species"):
        if p.get(k) not in (None, ""):
            rp[k] = p[k]
    geo = p.get("spots_geo") or []
    return rp, geo


def _classify_error(err: str) -> tuple[int, str]:
    """Map a task error string to a STACD HTTP code."""
    e = (err or "").lower()
    if "bad date" in e or "expected yyyy" in e:
        return 400, "BAD_REQUEST"
    if "no audio" in e or "no aggregate" in e or "skipped" in e or "no detections" in e:
        return 404, "NO_DATA"
    return 500, "PIPELINE_ERROR"


def _browse_href(job: jobstore.Job, rel: str) -> Optional[str]:
    s = get_settings()
    if s.STAC_ASSET_BASE_URL:
        return f"{s.STAC_ASSET_BASE_URL}/jobs/{job.id}/{rel}"
    if s.FILE_BROWSER_BASE_URL:
        return f"{s.FILE_BROWSER_BASE_URL}/jobs/{job.id}/{rel}"
    return None


# --------------------------------------------------------------------------- #
# Shared synchronous core (behind every algorithm wrapper)
# --------------------------------------------------------------------------- #
def _run_algorithm_sync(job_id: str, algo: str, params: dict | None):
    """Run one algorithm node on one job to completion; return the STACD response.

    Blocking — the returned value IS the completion signal Airflow waits on."""
    if not meta.is_valid_step(algo):
        raise HTTPException(404, f"Unknown algorithm '{algo}'. One of: {list(meta.SCRIPTS)}")

    job = _require_job(job_id)
    run_params, geo = _parse_params(params or {}, step=algo)
    if geo:
        job.set_geo(geo)  # also stamps geometry onto the on-disk sidecars

    task_id = uuid.uuid4().hex
    task = runner.run_sync(job, algo, run_params)

    if task["status"] != "success":
        code, etype = _classify_error(task.get("error"))
        return JSONResponse(status_code=code, content={
            "status": "skipped" if code in (400, 404) else "failed",
            "error": etype,
            "message": task.get("error") or "Algorithm failed.",
            "task_id": task_id,
        })

    # Success: build a STAC 1.1.0 item per real output file (skip logs, STAC
    # sidecars, and the processed-files bookkeeping list).
    rels = [r for r in task.get("results", [])
            if not r.endswith(".stac.json")
            and not r.endswith("_run.log")
            and not r.endswith("processed_files.txt")]
    drange = f"{run_params.get('start_date', 'all')}_{run_params.get('end_date', 'all')}"
    prefix = get_settings().STACD_ASSET_ID_PREFIX
    items, asset_ids = [], []
    for rel in rels:
        fname = rel.split("/")[-1]
        asset_id = f"{prefix}/{job.id}/{algo}/{drange}/{fname}"
        items.append(stac.build_stacd_item(
            asset_id, algo, job.root / rel, run_params, geo, browse_href=_browse_href(job, rel)))
        asset_ids.append(asset_id)

    msg = f"{meta.load_manifest().get(algo, {}).get('name', algo)} completed"
    body = {
        "status": "completed",
        "Success": msg,
        "message": msg,
        "task_id": task_id,
        "job_id": job.id,
    }
    if len(items) == 1:
        body["asset_id"] = asset_ids[0]
        body["stac"] = items[0]
    else:
        body["asset_id"] = asset_ids
        body["asset_ids"] = asset_ids
        body["stac"] = items
    return body


# --------------------------------------------------------------------------- #
# OpenAPI example payloads (Swagger docs only — the live shape is built above).
# --------------------------------------------------------------------------- #
def _stac_example(asset_id: str, name: str) -> dict:
    s = get_settings()
    fname = asset_id.split("/")[-1]
    return {
        "type": "Feature",
        "stac_version": s.STACD_STAC_VERSION,
        "stac_extensions": [],
        "id": asset_id.replace("/", "_"),
        "geometry": {"type": "Point", "coordinates": [77.1897, 28.5635]},
        "bbox": [77.1897, 28.5635, 77.1897, 28.5635],
        "properties": {
            "title": name,
            "description": f"{name} output.",
            "datetime": "2026-06-10T09:27:38Z",
            "start_datetime": "2025-11-01T00:00:00Z",
            "end_datetime": "2025-12-31T00:00:00Z",
            "cem:algorithm_version": "1.0.0",
            "cem:api_version": s.API_VERSION,
            "cem:parameters": {"start_date": "20251101", "end_date": "20251231"},
            "cem:spots": [{"name": "SPOTA", "lat": 28.5635, "lon": 77.1897}],
        },
        "links": [
            {"rel": "collection", "href": "../collection.json",
             "type": "application/json", "title": s.STAC_COLLECTION},
            {"rel": "parent", "href": "../collection.json",
             "type": "application/json", "title": s.STAC_COLLECTION},
        ],
        "assets": {"data": {"href": fname, "type": "text/csv",
                            "title": fname, "roles": ["data"]}},
        "collection": s.STAC_COLLECTION,
    }


def _responses_for(algo: str, name: str) -> dict:
    s = get_settings()
    prefix = s.STACD_ASSET_ID_PREFIX
    a1 = f"{prefix}/job_abc123/{algo}/20251101_20251231/{algo}_summary.csv"
    a2 = f"{prefix}/job_abc123/{algo}/20251101_20251231/{algo}_plot.png"
    single = {
        "status": "completed",
        "Success": f"{name} completed",
        "message": f"{name} completed",
        "task_id": "f5e2b620fdc440678b7a58295c02c1c4",
        "job_id": "job_abc123",
        "asset_id": a1,
        "stac": _stac_example(a1, name),
    }
    multi = {
        "status": "completed",
        "Success": f"{name} completed",
        "message": f"{name} completed",
        "task_id": "d50a922f2e024f6b9da6977cbb66e9fa",
        "job_id": "job_abc123",
        "asset_id": [a1, a2],
        "asset_ids": [a1, a2],
        "stac": [_stac_example(a1, name), _stac_example(a2, name)],
    }
    return {
        200: {
            "description": f"{name} completed; asset(s) produced and described as "
                           f"STAC {s.STACD_STAC_VERSION} item(s).",
            "content": {"application/json": {"examples": {
                "single_output": {"summary": "One output file (scalar asset_id + object stac)",
                                   "value": single},
                "multiple_outputs": {"summary": "Several output files (asset_id/asset_ids arrays + stac array)",
                                     "value": multi},
            }}},
        },
        400: {
            "description": "Bad input (e.g. malformed date) — Airflow marks the task skipped.",
            "content": {"application/json": {"example": {
                "status": "skipped", "error": "BAD_REQUEST",
                "message": "Bad date '2025-13-01', expected YYYY-MM-DD or YYYYMMDD",
                "task_id": "b1c2d3e4f5a60718293a4b5c6d7e8f90",
            }}},
        },
        404: {
            "description": "No data (unknown job, no audio, empty aggregate, or no "
                           "detections) — Airflow marks the task skipped.",
            "content": {"application/json": {"example": {
                "status": "skipped", "error": "NO_DATA",
                "message": "No aggregate available. Run birdnet first, or upload an aggregate CSV.",
                "task_id": "0f1e2d3c4b5a69788796a5b4c3d2e1f0",
            }}},
        },
        500: {
            "description": "Pipeline/computation error — Airflow marks the task failed.",
            "content": {"application/json": {"example": {
                "status": "failed", "error": "PIPELINE_ERROR",
                "message": "Script exited with code 1. See _run.log.",
                "task_id": "9a8b7c6d5e4f3021123445566778899a",
            }}},
        },
    }


# Request-body example shown in Swagger (same body for every wrapper; each algo
# only reads its own tunables).
_BODY_EXAMPLE = {
    "spots": "04213SPOT1,71301SPOT2",
    "start_date": "20251101",
    "end_date": "20251231",
    "snr_db": 18,
    "min_confidence": 0.25,
    "top_n_species": 25,
    "top_n_temporal": 80,
    "sci_threshold": 0.9,
    "kurtosis_threshold": 15,
    "pmr_threshold": 50,
    "window_size": 60,
    "min_solar_days": 5,
    "max_timeseries_species": 50,
    "spots_geo": [{"name": "04213SPOT1", "lat": 28.5635, "lon": 77.1897}],
}


# --------------------------------------------------------------------------- #
# Algorithm wrappers (synchronous) — SEVEN explicit named routes, per job.
# Registered before the generic fallback so the named path always wins.
# --------------------------------------------------------------------------- #
@router.post("/jobs/{job_id}/birdnet",
             responses=_responses_for("birdnet", "BirdNET Species Detection"),
             summary="BirdNET species detection (synchronous)")
def run_birdnet_sync(job_id: str, params: dict = Body(default=None, examples=[_BODY_EXAMPLE])):
    """Run BirdNET over the job's uploaded audio to completion, append detections
    to the job aggregate, and return the STACD asset response. Reads tunables
    `snr_db`, `min_confidence`. Needs audio uploaded for this job first."""
    return _run_algorithm_sync(job_id, "birdnet", params)


@router.post("/jobs/{job_id}/heatmaps",
             responses=_responses_for("heatmaps", "Species Activity Heatmaps"),
             summary="Species activity heatmaps (synchronous)")
def run_heatmaps_sync(job_id: str, params: dict = Body(default=None, examples=[_BODY_EXAMPLE])):
    """Per-spot hourly activity heatmaps from the aggregate. Reads tunable
    `top_n_species`. Needs the BirdNET aggregate."""
    return _run_algorithm_sync(job_id, "heatmaps", params)


@router.post("/jobs/{job_id}/temporal_stickiness",
             responses=_responses_for("temporal_stickiness", "Activity Regularity (Temporal)"),
             summary="Temporal stickiness (synchronous)")
def run_temporal_stickiness_sync(job_id: str, params: dict = Body(default=None, examples=[_BODY_EXAMPLE])):
    """Consecutive-day temporal activity correlation per species. Reads tunable
    `top_n_temporal`. Needs the BirdNET aggregate."""
    return _run_algorithm_sync(job_id, "temporal_stickiness", params)


@router.post("/jobs/{job_id}/spatial_stickiness",
             responses=_responses_for("spatial_stickiness", "Habitat Affinity (Spatial)"),
             summary="Spatial stickiness (synchronous)")
def run_spatial_stickiness_sync(job_id: str, params: dict = Body(default=None, examples=[_BODY_EXAMPLE])):
    """Consecutive-day spatial distribution correlation. Needs the BirdNET
    aggregate with >=2 spots (returns 404-skip otherwise)."""
    return _run_algorithm_sync(job_id, "spatial_stickiness", params)


@router.post("/jobs/{job_id}/migratory_classification",
             responses=_responses_for("migratory_classification", "Migratory vs Resident"),
             summary="Migratory vs resident classification (synchronous)")
def run_migratory_classification_sync(job_id: str, params: dict = Body(default=None, examples=[_BODY_EXAMPLE])):
    """Classify species migratory vs resident. Reads tunables `sci_threshold`,
    `kurtosis_threshold`, `pmr_threshold`, `window_size`. Needs the aggregate."""
    return _run_algorithm_sync(job_id, "migratory_classification", params)


@router.post("/jobs/{job_id}/solar_correlation",
             responses=_responses_for("solar_correlation", "Solar Event Correlation"),
             summary="Solar event correlation (synchronous)")
def run_solar_correlation_sync(job_id: str, params: dict = Body(default=None, examples=[_BODY_EXAMPLE])):
    """Correlate daily peak activity hour with sunrise/sunset. Reads tunable
    `min_solar_days`. Needs the BirdNET aggregate."""
    return _run_algorithm_sync(job_id, "solar_correlation", params)


@router.post("/jobs/{job_id}/daily_timeseries",
             responses=_responses_for("daily_timeseries", "Daily Call Time Series"),
             summary="Daily call time series (synchronous)")
def run_daily_timeseries_sync(job_id: str, params: dict = Body(default=None, examples=[_BODY_EXAMPLE])):
    """Per-species daily call-count time series + data-availability heatmap. Reads
    tunable `max_timeseries_species`. Needs the BirdNET aggregate."""
    return _run_algorithm_sync(job_id, "daily_timeseries", params)


# --------------------------------------------------------------------------- #
# Read-only: job summary, results, logs, downloads (debugging / retrieval).
# --------------------------------------------------------------------------- #
@router.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = _require_job(job_id)
    meta_d = job.read()
    s = get_settings()
    return {
        "job_id": job.id,
        "created_at": meta_d["created_at"],
        "inputs": {
            "audio_files": sorted(p.name for p in job.audio_dir.glob("*")) if job.audio_dir.is_dir() else [],
            "uploaded_aggregate": job.uploaded_aggregate.is_file(),
        },
        "has_aggregate": job.resolve_aggregate() is not None,
        "tasks": meta_d.get("tasks", []),
        "results": job.list_results(),
        "browse_url": s.file_browser_url(job.id),
        "api_version": s.API_VERSION,
    }


@router.get("/jobs/{job_id}/results")
def list_results(job_id: str):
    job = _require_job(job_id)
    s = get_settings()
    return {
        "job_id": job.id,
        "results": job.list_results(),
        "browse_url": s.file_browser_url(job.id),
        "api_version": s.API_VERSION,
    }


@router.get("/jobs/{job_id}/tasks/{task_id}/log",
            response_class=PlainTextResponse)
def get_task_log(job_id: str, task_id: str):
    job = _require_job(job_id)
    t = job.get_task(task_id)
    if t is None:
        raise HTTPException(404, f"Task '{task_id}' not found.")
    log = job.step_results_dir(t["step"]) / "_run.log"
    if not log.is_file():
        return PlainTextResponse("(no log yet)")
    return PlainTextResponse(log.read_text(errors="replace"))


def _zip_paths(job: jobstore.Job, rel_paths: list[str], zip_name: str) -> Path:
    if not rel_paths:
        raise HTTPException(404, "No result files to download yet.")
    dl_dir = job.root / "_downloads"
    dl_dir.mkdir(parents=True, exist_ok=True)
    zip_path = dl_dir / zip_name
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for rel in rel_paths:
            abs_p = job.root / rel
            if abs_p.is_file():
                zf.write(abs_p, arcname=rel)
    return zip_path


@router.get("/jobs/{job_id}/download")
def download_all(job_id: str):
    job = _require_job(job_id)
    zip_path = _zip_paths(job, job.list_results(), f"{job.id}_all_results.zip")
    return FileResponse(zip_path, media_type="application/zip", filename=zip_path.name)


@router.get("/jobs/{job_id}/download/{step}")
def download_step(job_id: str, step: str):
    if not meta.is_valid_step(step):
        raise HTTPException(404, f"Unknown step '{step}'.")
    job = _require_job(job_id)
    rels = [r for r in job.list_results() if r.startswith(f"results/{step}/") or
            (step == meta.BIRDNET and r.startswith("work/"))]
    zip_path = _zip_paths(job, rels, f"{job.id}_{step}.zip")
    return FileResponse(zip_path, media_type="application/zip", filename=zip_path.name)


@router.get("/jobs/{job_id}/file")
def download_file(job_id: str, path: str = Query(..., description="Result path relative to job root")):
    job = _require_job(job_id)
    rel = path.replace("\\", "/").lstrip("/")
    target = (job.root / rel).resolve()
    if not str(target).startswith(str(job.root.resolve()) + os.sep):
        raise HTTPException(400, "Path escapes job directory.")
    if not target.is_file():
        raise HTTPException(404, f"File '{path}' not found.")
    return FileResponse(target, filename=target.name)


# --------------------------------------------------------------------------- #
# Generic fallback (registered LAST so the seven named routes match first).
# --------------------------------------------------------------------------- #
@router.post("/jobs/{job_id}/{algo}",
             responses=_responses_for("solar_correlation", "Algorithm"),
             summary="Run any algorithm synchronously (generic fallback)")
def run_algorithm(job_id: str, algo: str, params: dict = Body(default=None, examples=[_BODY_EXAMPLE])):
    """Generic synchronous runner. The seven named routes above are preferred and
    self-documented; this catch-all keeps the contract working for any valid
    `algo` id (`birdnet` or one of the six analyses) and for any future step."""
    return _run_algorithm_sync(job_id, algo, params)
