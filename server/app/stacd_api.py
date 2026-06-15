"""
STACD / Airflow synchronous algorithm API (additive — the async /jobs API is
unchanged and still used by the web app directly).

Model (confirmed with the professor):
  * The web app (Render) triggers DAGs in STACD/Airflow.
  * Airflow runs each DAG node by calling ONE of these endpoints and BLOCKS until
    it returns — the HTTP response IS the completion signal (no polling here).
  * Audio is a REGISTERED dataset: it is always uploaded into one fixed workspace
    audio dir (`POST /api/v1/datasets/audio`); Airflow does not pass a path.
  * Responses match the STACD contract (README §11) and the example payloads:
        200 -> {status, Success, message, task_id, asset_id[, asset_ids], stac}
        400 -> bad input        (skipped)        {error, message}
        404 -> no data          (skipped)        {error, message}
        500 -> pipeline failure  (failed)        {error, message}

Each pipeline step is one algorithm node. There are SEVEN explicit, named
synchronous wrappers — one per algorithm:

    POST /api/v1/birdnet
    POST /api/v1/heatmaps
    POST /api/v1/temporal_stickiness
    POST /api/v1/spatial_stickiness
    POST /api/v1/migratory_classification
    POST /api/v1/solar_correlation
    POST /api/v1/daily_timeseries

Each is a thin wrapper over a shared core (`_run_algorithm_sync`). A generic
`POST /api/v1/{algo}` fallback is kept (registered LAST, so the named routes win)
for forward-compat and parity with the async API's generic run route. birdnet
reads the registered audio dir and appends to the workspace aggregate; the six
analyses read that aggregate. These are the endpoints the algorithm-repo YAML
points each DAG node at.
"""
import os
import uuid

from fastapi import APIRouter, Body, Depends, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from . import jobs as jobstore
from . import pipeline_meta as meta
from . import runner
from . import stac
from .auth import require_api_key
from .settings import get_settings

router = APIRouter(prefix="/api/v1", tags=["stacd"])


def _workspace() -> jobstore.Job:
    """The single persistent registered workspace (fixed job dir)."""
    return jobstore.ensure_job(get_settings().STACD_WORKSPACE_ID)


# --------------------------------------------------------------------------- #
# Registered dataset: audio upload (always into the one fixed audio dir)
# --------------------------------------------------------------------------- #
@router.post("/datasets/audio", dependencies=[Depends(require_api_key)])
def upload_audio(files: list[UploadFile] = File(...)):
    """Upload WAVs into the registered audio dataset. Called by the web app
    before triggering the DAG (Airflow then reads this dir)."""
    job = _workspace()
    s = get_settings()
    max_bytes = s.MAX_UPLOAD_MB * 1024 * 1024
    saved = []
    for up in files:
        name = os.path.basename(up.filename or "").strip()
        if not name or name in (".", ".."):
            raise HTTPException(400, "Invalid filename.")
        dest = job.audio_dir / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        written = 0
        with open(dest, "wb") as out:
            while True:
                chunk = up.file.read(1024 * 1024)
                if not chunk:
                    break
                written += len(chunk)
                if written > max_bytes:
                    out.close()
                    dest.unlink(missing_ok=True)
                    raise HTTPException(413, f"Upload exceeds {s.MAX_UPLOAD_MB} MB limit.")
                out.write(chunk)
        saved.append(name)
    return {"status": "ok", "uploaded": saved, "audio_dir": str(job.audio_dir)}


# --------------------------------------------------------------------------- #
# Shared helpers
# --------------------------------------------------------------------------- #
def _parse_params(p: dict) -> tuple[dict, list]:
    """Pull pipeline params + spot geolocation out of the STACD request body.

    STACD/Airflow sends every DAG param as a string (the DAG yaml types them
    `string`), so `spots` may arrive comma-separated; normalize it to a list and
    forward only the keys each step actually reads.
    """
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
    # Per-step algorithm tunables (each consumed by exactly one algo; runner
    # forwards only the ones relevant to the step being run).
    for k in ("min_confidence", "top_n_species", "top_n_temporal",
              "sci_threshold", "kurtosis_threshold", "pmr_threshold",
              "window_size", "min_solar_days", "max_timeseries_species"):
        if p.get(k) not in (None, ""):
            rp[k] = p[k]
    geo = p.get("spots_geo") or []
    return rp, geo


def _classify_error(err: str) -> tuple[int, str]:
    """Map a task error string to a STACD HTTP code (README §11)."""
    e = (err or "").lower()
    if "bad date" in e or "expected yyyy" in e:
        return 400, "BAD_REQUEST"
    if "no audio" in e or "no aggregate" in e or "skipped" in e or "no detections" in e:
        return 404, "NO_DATA"
    return 500, "PIPELINE_ERROR"


def _browse_href(job: jobstore.Job, rel: str) -> str | None:
    s = get_settings()
    if s.STAC_ASSET_BASE_URL:
        return f"{s.STAC_ASSET_BASE_URL}/jobs/{job.id}/{rel}"
    if s.FILE_BROWSER_BASE_URL:
        return f"{s.FILE_BROWSER_BASE_URL}/jobs/{job.id}/{rel}"
    return None


# --------------------------------------------------------------------------- #
# Shared synchronous core (behind every algorithm wrapper)
# --------------------------------------------------------------------------- #
def _run_algorithm_sync(algo: str, params: dict | None):
    """Run one algorithm node to completion and return the STACD response.

    Blocking — the returned value IS the completion signal Airflow waits on.
    On success returns the 200 STACD body (dict: status/Success/message/task_id
    + asset_id[/asset_ids] + stac). On skip/failure returns a JSONResponse with
    the mapped 400/404/500 code and {status, error, message, task_id}.

    Body (all optional): {spots, start_date, end_date, snr_db, spots_geo} plus the
    per-step algorithm tunables (min_confidence, top_n_species, top_n_temporal,
    sci_threshold, kurtosis_threshold, pmr_threshold, window_size, min_solar_days,
    max_timeseries_species) — each applied only by the algo that reads it.
    """
    if not meta.is_valid_step(algo):
        raise HTTPException(404, f"Unknown algorithm '{algo}'. One of: {list(meta.SCRIPTS)}")

    job = _workspace()
    run_params, geo = _parse_params(params or {})
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
        asset_id = f"{prefix}/{algo}/{drange}/{fname}"
        items.append(stac.build_stacd_item(
            asset_id, algo, job.root / rel, run_params, geo, browse_href=_browse_href(job, rel)))
        asset_ids.append(asset_id)

    msg = f"{meta.load_manifest().get(algo, {}).get('name', algo)} completed"
    body = {
        "status": "completed",
        "Success": msg,
        "message": msg,
        "task_id": task_id,
    }
    # Match the example payloads: single output -> scalar asset_id + object stac;
    # multiple outputs -> asset_id array + asset_ids + stac array.
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
    """A compact, faithful STAC 1.1.0 item (shape build_stacd_item emits)."""
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
    """OpenAPI `responses=` block: success(200) single+multi examples and the
    skip(400/404)/fail(500) examples, matching the uploaded api-response files."""
    s = get_settings()
    prefix = s.STACD_ASSET_ID_PREFIX
    a1 = f"{prefix}/{algo}/20251101_20251231/{algo}_summary.csv"
    a2 = f"{prefix}/{algo}/20251101_20251231/{algo}_plot.png"
    single = {
        "status": "completed",
        "Success": f"{name} completed",
        "message": f"{name} completed",
        "task_id": "f5e2b620fdc440678b7a58295c02c1c4",
        "asset_id": a1,
        "stac": _stac_example(a1, name),
    }
    multi = {
        "status": "completed",
        "Success": f"{name} completed",
        "message": f"{name} completed",
        "task_id": "d50a922f2e024f6b9da6977cbb66e9fa",
        "asset_id": [a1, a2],
        "asset_ids": [a1, a2],
        "stac": [_stac_example(a1, name), _stac_example(a2, name)],
    }
    return {
        200: {
            "description": f"{name} completed; asset(s) produced, registered and "
                           f"described as STAC {s.STACD_STAC_VERSION} item(s).",
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
            "description": "No data (no audio uploaded, empty aggregate, or no detections) "
                           "— Airflow marks the task skipped.",
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


# Request-body example shown in Swagger (the body is the same for every wrapper;
# each algo only reads its own tunables).
_BODY_EXAMPLE = {
    "spots": "04213SPOT1,71301SPOT2",
    "start_date": "20251101",
    "end_date": "20251231",
    "spots_geo": [{"name": "04213SPOT1", "lat": 28.5635, "lon": 77.1897}],
}


# --------------------------------------------------------------------------- #
# Algorithm wrappers (synchronous) — SEVEN explicit named routes.
# Registered before the generic fallback so the named path always wins.
# --------------------------------------------------------------------------- #
@router.post("/birdnet", responses=_responses_for("birdnet", "BirdNET Species Detection"),
             summary="BirdNET species detection (synchronous)", dependencies=[Depends(require_api_key)])
def run_birdnet_sync(params: dict = Body(default=None, examples=[_BODY_EXAMPLE])):
    """Run BirdNET over the registered audio to completion, append detections to
    the workspace aggregate, and return the STACD asset response. Reads tunables
    `snr_db`, `min_confidence`. Needs audio (POST /api/v1/datasets/audio first)."""
    return _run_algorithm_sync("birdnet", params)


@router.post("/heatmaps", responses=_responses_for("heatmaps", "Species Activity Heatmaps"),
             summary="Species activity heatmaps (synchronous)", dependencies=[Depends(require_api_key)])
def run_heatmaps_sync(params: dict = Body(default=None, examples=[_BODY_EXAMPLE])):
    """Per-spot hourly activity heatmaps from the aggregate. Reads tunable
    `top_n_species`. Needs the BirdNET aggregate."""
    return _run_algorithm_sync("heatmaps", params)


@router.post("/temporal_stickiness",
             responses=_responses_for("temporal_stickiness", "Activity Regularity (Temporal)"),
             summary="Temporal stickiness (synchronous)", dependencies=[Depends(require_api_key)])
def run_temporal_stickiness_sync(params: dict = Body(default=None, examples=[_BODY_EXAMPLE])):
    """Consecutive-day temporal activity correlation per species. Reads tunable
    `top_n_temporal`. Needs the BirdNET aggregate."""
    return _run_algorithm_sync("temporal_stickiness", params)


@router.post("/spatial_stickiness",
             responses=_responses_for("spatial_stickiness", "Habitat Affinity (Spatial)"),
             summary="Spatial stickiness (synchronous)", dependencies=[Depends(require_api_key)])
def run_spatial_stickiness_sync(params: dict = Body(default=None, examples=[_BODY_EXAMPLE])):
    """Consecutive-day spatial distribution correlation. Needs the BirdNET
    aggregate with >=2 spots (returns 404-skip otherwise)."""
    return _run_algorithm_sync("spatial_stickiness", params)


@router.post("/migratory_classification",
             responses=_responses_for("migratory_classification", "Migratory vs Resident"),
             summary="Migratory vs resident classification (synchronous)",
             dependencies=[Depends(require_api_key)])
def run_migratory_classification_sync(params: dict = Body(default=None, examples=[_BODY_EXAMPLE])):
    """Classify species migratory vs resident. Reads tunables `sci_threshold`,
    `kurtosis_threshold`, `pmr_threshold`, `window_size`. Needs the aggregate."""
    return _run_algorithm_sync("migratory_classification", params)


@router.post("/solar_correlation",
             responses=_responses_for("solar_correlation", "Solar Event Correlation"),
             summary="Solar event correlation (synchronous)", dependencies=[Depends(require_api_key)])
def run_solar_correlation_sync(params: dict = Body(default=None, examples=[_BODY_EXAMPLE])):
    """Correlate daily peak activity hour with sunrise/sunset. Reads tunable
    `min_solar_days`. Needs the BirdNET aggregate."""
    return _run_algorithm_sync("solar_correlation", params)


@router.post("/daily_timeseries",
             responses=_responses_for("daily_timeseries", "Daily Call Time Series"),
             summary="Daily call time series (synchronous)", dependencies=[Depends(require_api_key)])
def run_daily_timeseries_sync(params: dict = Body(default=None, examples=[_BODY_EXAMPLE])):
    """Per-species daily call-count time series + data-availability heatmap. Reads
    tunable `max_timeseries_species`. Needs the BirdNET aggregate."""
    return _run_algorithm_sync("daily_timeseries", params)


# --------------------------------------------------------------------------- #
# Generic fallback (registered LAST so the seven named routes match first).
# Mirrors the async API's generic /run/{step}; keeps any future algo working.
# --------------------------------------------------------------------------- #
@router.post("/{algo}", responses=_responses_for("solar_correlation", "Algorithm"),
             summary="Run any algorithm synchronously (generic fallback)",
             dependencies=[Depends(require_api_key)])
def run_algorithm(algo: str, params: dict = Body(default=None, examples=[_BODY_EXAMPLE])):
    """Generic synchronous runner. The seven named routes above are preferred and
    self-documented; this catch-all keeps the contract working for any valid
    `algo` id (`birdnet` or one of the six analyses) and for any future step."""
    return _run_algorithm_sync(algo, params)
