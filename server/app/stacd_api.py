"""
CEM Server API -- clean 3-group design.

  1. Upload -- project-level file storage.
  2. Scripts -- synchronous algorithm execution with typed Pydantic bodies.
  3. Polling & download -- job status, results, files.
"""
import json
import os
import uuid
import zipfile
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Body, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse

from . import jobs as jobstore
from . import pipeline_meta as meta
from . import projects as projectstore
from . import runner
from . import stac
from .schemas import (
    AcousticIndicesParams,
    BaseRunParams,
    BirdnetParams,
    DailyTimeseriesParams,
    HeatmapsParams,
    MigratoryClassificationParams,
    SolarCorrelationParams,
    SpatialStickinessParams,
    STEP_MODELS,
    TemporalStickinessParams,
)
from .settings import get_settings

router = APIRouter(prefix="/api/v1", tags=["cem"])


# =========================================================================== #
#  Helpers
# =========================================================================== #

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


# =========================================================================== #
#  GROUP 1 -- Upload
# =========================================================================== #

@router.get("/projects/status")
def project_status(project: str = Query(..., description="Project folder name")):
    proj = projectstore.get_project(project)
    if proj is None:
        raise HTTPException(404, f"Project '{project}' not found.")
    return proj.status()


@router.post("/projects/upload/audio")
def project_upload_audio(
    project: str = Form(..., description="Project folder name"),
    spot: str = Form(..., description="Spot name for these files"),
    files: list[UploadFile] = File(...),
):
    if not files:
        raise HTTPException(400, "No files provided.")
    if not spot.strip():
        raise HTTPException(400, "spot is required.")

    proj = projectstore.get_or_create_project(project)
    audio_dir = proj.spot_audio_dir(spot)
    audio_dir.mkdir(parents=True, exist_ok=True)
    existing = set(p.name for p in audio_dir.iterdir() if p.is_file())

    saved, skipped = [], []
    for up in files:
        fname = _safe_name(up.filename)
        if fname in existing:
            skipped.append(fname)
            continue
        size = _save_upload(audio_dir / fname, up)
        saved.append({"filename": fname, "size_bytes": size})
        existing.add(fname)

    proj._touch()
    return {
        "status": "ok",
        "project": project,
        "spot": spot,
        "uploaded": saved,
        "skipped": skipped,
        "spot_audio_count": proj.audio_count(spot),
        "total_audio": proj.audio_count(),
    }


@router.post("/projects/upload/aggregate")
def project_upload_aggregate(
    project: str = Form(..., description="Project folder name"),
    file: UploadFile = File(...),
):
    proj = projectstore.get_or_create_project(project)
    proj.dataset_dir.mkdir(parents=True, exist_ok=True)
    _save_upload(proj.aggregate_path, file)
    proj._touch()
    return {"status": "ok", "project": project, "has_aggregate": True}


@router.post("/projects/upload/processed")
def project_upload_processed(
    project: str = Form(..., description="Project folder name"),
    file: UploadFile = File(...),
):
    proj = projectstore.get_or_create_project(project)
    proj.dataset_dir.mkdir(parents=True, exist_ok=True)
    _save_upload(proj.processed_path, file)
    proj._touch()
    return {"status": "ok", "project": project, "has_processed": True}


# =========================================================================== #
#  GROUP 2 -- Scripts
# =========================================================================== #

@router.get("/steps")
def steps():
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


def _extract_script_params(body: dict) -> dict:
    common = {"project", "spots", "start_date", "end_date", "spots_geo"}
    return {k: v for k, v in body.items() if k not in common and v is not None}


def _classify_error(err: str) -> tuple[int, str]:
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


def _run_script(script_name: str, body: dict):
    if not meta.is_valid_step(script_name):
        raise HTTPException(404, f"Unknown script '{script_name}'. Options: {list(meta.SCRIPTS)}")

    project = body.get("project")
    spots = body.get("spots")
    if not project:
        raise HTTPException(400, "project is required.")
    if not spots:
        raise HTTPException(400, "spots is required (list of spot names).")

    start_date = body.get("start_date")
    end_date = body.get("end_date")
    spots_geo = body.get("spots_geo")

    proj = projectstore.get_project(project)
    if proj is None:
        raise HTTPException(404, f"Project '{project}' not found.")

    job = jobstore.create_job(project, script_name)
    stats = proj.populate_job(job, spots=spots, start_date=start_date, end_date=end_date)

    if spots_geo:
        job.set_geo(spots_geo)

    run_params = _extract_script_params(body)
    run_params["spots"] = spots
    if start_date:
        run_params["start_date"] = start_date
    if end_date:
        run_params["end_date"] = end_date

    task_id = uuid.uuid4().hex
    task = runner.run_sync(job, script_name, run_params)

    if task["status"] != "success":
        code, etype = _classify_error(task.get("error"))
        return JSONResponse(status_code=code, content={
            "status": "skipped" if code in (400, 404) else "failed",
            "error": etype,
            "message": task.get("error") or "Script failed.",
            "task_id": task_id,
            "job_id": job.id,
        })

    rels = [r for r in task.get("results", [])
            if not r.endswith(".stac.json")
            and not r.endswith("_run.log")
            and not r.endswith("processed_files.txt")]
    drange = f"{run_params.get('start_date', 'all')}_{run_params.get('end_date', 'all')}"
    prefix = get_settings().STACD_ASSET_ID_PREFIX
    items, asset_ids = [], []
    for rel in rels:
        fname = rel.split("/")[-1]
        asset_id = f"{prefix}/{job.id}/{script_name}/{drange}/{fname}"
        items.append(stac.build_stacd_item(
            asset_id, script_name, job.root / rel, run_params, spots_geo or [],
            browse_href=_browse_href(job, rel)))
        asset_ids.append(asset_id)

    proj.update_from_job(job)

    msg = f"{meta.load_manifest().get(script_name, {}).get('name', script_name)} completed"
    resp = {
        "status": "completed",
        "Success": msg,
        "message": msg,
        "task_id": task_id,
        "job_id": job.id,
        "project": project,
        "audio_linked": stats["audio_linked"],
        "audio_skipped": stats["audio_skipped"],
    }
    if len(items) == 1:
        resp["asset_id"] = asset_ids[0]
        resp["stac"] = items[0]
    else:
        resp["asset_id"] = asset_ids
        resp["asset_ids"] = asset_ids
        resp["stac"] = items
    return resp


# ---- named script wrappers (typed bodies -> Swagger shows all params) ----

@router.post("/scripts/birdnet", summary="BirdNET species detection")
def run_birdnet(body: BirdnetParams):
    return _run_script("birdnet", body.model_dump())


@router.post("/scripts/acoustic_indices", summary="Acoustic indices + box plots")
def run_acoustic_indices(body: AcousticIndicesParams):
    return _run_script("acoustic_indices", body.model_dump())


@router.post("/scripts/heatmaps", summary="Species activity heatmaps")
def run_heatmaps(body: HeatmapsParams):
    return _run_script("heatmaps", body.model_dump())


@router.post("/scripts/temporal_stickiness", summary="Temporal stickiness")
def run_temporal_stickiness(body: TemporalStickinessParams):
    return _run_script("temporal_stickiness", body.model_dump())


@router.post("/scripts/spatial_stickiness", summary="Spatial stickiness")
def run_spatial_stickiness(body: SpatialStickinessParams):
    return _run_script("spatial_stickiness", body.model_dump())


@router.post("/scripts/migratory_classification", summary="Migratory vs resident classification")
def run_migratory_classification(body: MigratoryClassificationParams):
    return _run_script("migratory_classification", body.model_dump())


@router.post("/scripts/solar_correlation", summary="Solar event correlation")
def run_solar_correlation(body: SolarCorrelationParams):
    return _run_script("solar_correlation", body.model_dump())


@router.post("/scripts/daily_timeseries", summary="Daily call time series")
def run_daily_timeseries(body: DailyTimeseriesParams):
    return _run_script("daily_timeseries", body.model_dump())


# ---- generic fallback (registered LAST) ----

@router.post("/scripts/{name}", summary="Run any script (generic fallback)")
def run_script_generic(name: str, body: dict = Body(...)):
    return _run_script(name, body)


# =========================================================================== #
#  GROUP 3 -- Polling & download
# =========================================================================== #

@router.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = _require_job(job_id)
    meta_d = job.read()
    s = get_settings()
    return {
        "job_id": job.id,
        "project": meta_d.get("project"),
        "script": meta_d.get("script"),
        "created_at": meta_d["created_at"],
        "inputs": {
            "audio_files": sorted(p.name for p in job.audio_dir.glob("*")) if job.audio_dir.is_dir() else [],
            "uploaded_aggregate": job.uploaded_aggregate.is_file(),
        },
        "has_aggregate": job.resolve_aggregate() is not None,
        "tasks": meta_d.get("tasks", []),
        "results": job.list_results(),
        "api_version": s.API_VERSION,
    }


@router.get("/jobs/{job_id}/results")
def list_results(job_id: str):
    job = _require_job(job_id)
    s = get_settings()
    return {
        "job_id": job.id,
        "results": job.list_results(),
        "api_version": s.API_VERSION,
    }


@router.get("/jobs/{job_id}/tasks/{task_id}/log", response_class=PlainTextResponse)
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
