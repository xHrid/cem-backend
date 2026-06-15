"""
CEM BirdNET pipeline server — FastAPI app.

Flow:
  1. Upload inputs (audio WAVs and/or an aggregate CSV) -> get a job_id.
  2. Run birdnet and/or analysis steps (async; returns a task_id to poll).
  3. Download generated results (per-file, per-step zip, or whole-job zip).
"""
import os
import shutil
import zipfile
from pathlib import Path
from typing import Optional

from fastapi import (
    Depends, FastAPI, File, Form, HTTPException, Query, UploadFile,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, PlainTextResponse

from . import jobs as jobstore
from . import pipeline_meta as meta
from . import retention
from . import runner
from . import stacd_api
from .auth import require_api_key
from .schemas import (
    CreateJobResponse, JobSummary, RunAllResponse, RunParams, RunResponse,
    TaskInfo, UploadResponse, UploadedFile,
)
from .settings import get_settings

app = FastAPI(
    title="CEM BirdNET Pipeline API",
    version=get_settings().API_VERSION,
    description="Upload audio/aggregate, run BirdNET + ecological analyses, download results.",
)


@app.on_event("startup")
def _on_startup() -> None:
    # Background output retention sweeper (item 15). No-op when disabled.
    retention.start_background()


# STACD / Airflow synchronous algorithm API (additive; /api/v1/*). The async
# /jobs API above is unchanged and still used by the web app directly.
app.include_router(stacd_api.router)

# Allow the browser-based static site (different origin) to call this API.
# Auth is a custom X-API-Key header, not cookies, so credentials stay off and a
# wildcard origin is safe. allow_headers must include the custom header (covered
# by "*"). Without this, every fetch from the webapp fails the CORS preflight.
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)

UPLOAD_KINDS = {"audio", "reference", "aggregate", "processed", "ebird", "static_noise", "rain_noise"}

# Fixed save names for the single-file override kinds.
# `processed` = the caller's processed-files list (one filename per line). The
# webapp ships its local processed_<script>_server.txt so BirdNET skips files it
# already ran — giving the stateless server the same dedup the watcher has.
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
    dest.parent.mkdir(parents=True, exist_ok=True)
    max_bytes = get_settings().MAX_UPLOAD_MB * 1024 * 1024
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
                raise HTTPException(413, f"Upload exceeds {get_settings().MAX_UPLOAD_MB} MB limit.")
            out.write(chunk)
    return written


def _task_info(t: dict) -> TaskInfo:
    return TaskInfo(**t)


def _do_upload(job: jobstore.Job, kind: str, files: list[UploadFile], spot: Optional[str]) -> list[UploadedFile]:
    if kind not in UPLOAD_KINDS:
        raise HTTPException(400, f"Unknown kind '{kind}'. One of: {sorted(UPLOAD_KINDS)}")
    if not files:
        raise HTTPException(400, "No files provided.")

    saved: list[UploadedFile] = []
    for up in files:
        original = _safe_name(up.filename)
        if kind == "audio":
            dest = job.audio_dir / original
        elif kind == "reference":
            dest = job.reference_dir / original
        else:  # single-file override kinds
            dest = job.input_dir / _FIXED_NAMES[kind]
        size = _save_upload(dest, up)
        if kind == "reference":
            job.set_reference_spot(original, spot)
        saved.append(UploadedFile(
            filename=original, kind=kind, size_bytes=size,
            rel_path=str(dest.relative_to(job.root)).replace("\\", "/"),
        ))
    return saved


def _zip_paths(job: jobstore.Job, rel_paths: list[str], zip_name: str) -> Path:
    if not rel_paths:
        raise HTTPException(404, "No result files to download yet.")
    dl_dir = job.root / "_downloads"
    dl_dir.mkdir(parents=True, exist_ok=True)
    zip_path = dl_dir / zip_name
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for rel in rel_paths:
            abs_p = (job.root / rel)
            if abs_p.is_file():
                zf.write(abs_p, arcname=rel)
    return zip_path


# --------------------------------------------------------------------------- #
# health (no auth)
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    return {
        "status": "ok",
        "api_version": get_settings().API_VERSION,
        "steps": list(meta.SCRIPTS.keys()),
    }


@app.get("/steps", dependencies=[Depends(require_api_key)])
def steps():
    """Catalogue of runnable steps with names/descriptions from the manifest."""
    man = meta.load_manifest()
    return {
        sid: {
            "name": man.get(sid, {}).get("name", sid),
            "description": man.get(sid, {}).get("description", ""),
            "is_analysis": meta.is_analysis(sid),
            "depends_on": man.get(sid, {}).get("depends_on", []),
            "version": meta.step_version(sid),          # algorithm version (item 12)
            "api_version": get_settings().API_VERSION,  # compute API version
        }
        for sid in meta.SCRIPTS
    }


# --------------------------------------------------------------------------- #
# jobs
# --------------------------------------------------------------------------- #
@app.post("/jobs", response_model=CreateJobResponse, dependencies=[Depends(require_api_key)])
def create_job():
    job = jobstore.create_job()
    return CreateJobResponse(job_id=job.id, created_at=job.read()["created_at"])


@app.get("/jobs", dependencies=[Depends(require_api_key)])
def list_jobs():
    return {"jobs": jobstore.list_jobs()}


@app.get("/jobs/{job_id}", response_model=JobSummary, dependencies=[Depends(require_api_key)])
def get_job(job_id: str):
    job = _require_job(job_id)
    meta_d = job.read()
    agg = job.resolve_aggregate()
    return JobSummary(
        job_id=job.id,
        created_at=meta_d["created_at"],
        inputs={
            "audio_files": sorted(p.name for p in job.audio_dir.glob("*")) if job.audio_dir.is_dir() else [],
            "reference_files": job.get_reference_spots(),
            "uploaded_aggregate": job.uploaded_aggregate.is_file(),
        },
        has_aggregate=agg is not None,
        tasks=[_task_info(t) for t in meta_d.get("tasks", [])],
        results=job.list_results(),
        browse_url=get_settings().file_browser_url(job.id),
        api_version=get_settings().API_VERSION,
    )


# --------------------------------------------------------------------------- #
# UPLOAD API
# --------------------------------------------------------------------------- #
@app.post("/upload", response_model=UploadResponse, dependencies=[Depends(require_api_key)])
def upload_new(
    kind: str = Form("audio"),
    spot: Optional[str] = Form(None),
    files: list[UploadFile] = File(...),
):
    """Create a fresh job and upload files into it in one call."""
    job = jobstore.create_job()
    saved = _do_upload(job, kind, files, spot)
    return UploadResponse(job_id=job.id, uploaded=saved)


@app.post("/jobs/{job_id}/upload", response_model=UploadResponse, dependencies=[Depends(require_api_key)])
def upload_to_job(
    job_id: str,
    kind: str = Form("audio"),
    spot: Optional[str] = Form(None),
    files: list[UploadFile] = File(...),
):
    """Add files to an existing job.

    kind: audio | reference | aggregate | ebird | static_noise | rain_noise
    spot: (reference only) recorder/site label attached to the reference files.
    """
    job = _require_job(job_id)
    saved = _do_upload(job, kind, files, spot)
    return UploadResponse(job_id=job.id, uploaded=saved)


# --------------------------------------------------------------------------- #
# RUN APIs (one per script + run-all)
# --------------------------------------------------------------------------- #
def _params_for_run(job: jobstore.Job, params: RunParams) -> dict:
    """Model -> CLI param dict; sidelines spot geolocation onto the job (item 9)."""
    pdict = params.model_dump(exclude_none=True)
    geo = pdict.pop("spots_geo", None)  # geo is provenance, not a pipeline flag
    if geo:
        job.set_geo(geo)
    return pdict


def _run_step(job_id: str, step: str, params: RunParams) -> RunResponse:
    if not meta.is_valid_step(step):
        raise HTTPException(404, f"Unknown step '{step}'. One of: {list(meta.SCRIPTS)}")
    job = _require_job(job_id)
    try:
        task = runner.submit(job, step, _params_for_run(job, params))
    except ValueError as e:
        raise HTTPException(400, str(e))
    return RunResponse(job_id=job.id, task_id=task["task_id"], step=step, status=task["status"])


@app.post("/jobs/{job_id}/run/birdnet", response_model=RunResponse, dependencies=[Depends(require_api_key)])
def run_birdnet(job_id: str, params: RunParams = RunParams()):
    return _run_step(job_id, "birdnet", params)


@app.post("/jobs/{job_id}/run/heatmaps", response_model=RunResponse, dependencies=[Depends(require_api_key)])
def run_heatmaps(job_id: str, params: RunParams = RunParams()):
    return _run_step(job_id, "heatmaps", params)


@app.post("/jobs/{job_id}/run/temporal_stickiness", response_model=RunResponse, dependencies=[Depends(require_api_key)])
def run_temporal(job_id: str, params: RunParams = RunParams()):
    return _run_step(job_id, "temporal_stickiness", params)


@app.post("/jobs/{job_id}/run/spatial_stickiness", response_model=RunResponse, dependencies=[Depends(require_api_key)])
def run_spatial(job_id: str, params: RunParams = RunParams()):
    return _run_step(job_id, "spatial_stickiness", params)


@app.post("/jobs/{job_id}/run/migratory_classification", response_model=RunResponse, dependencies=[Depends(require_api_key)])
def run_migratory(job_id: str, params: RunParams = RunParams()):
    return _run_step(job_id, "migratory_classification", params)


@app.post("/jobs/{job_id}/run/solar_correlation", response_model=RunResponse, dependencies=[Depends(require_api_key)])
def run_solar(job_id: str, params: RunParams = RunParams()):
    return _run_step(job_id, "solar_correlation", params)


@app.post("/jobs/{job_id}/run/daily_timeseries", response_model=RunResponse, dependencies=[Depends(require_api_key)])
def run_timeseries(job_id: str, params: RunParams = RunParams()):
    return _run_step(job_id, "daily_timeseries", params)


# generic fallback (any valid step id)
@app.post("/jobs/{job_id}/run/{step}", response_model=RunResponse, dependencies=[Depends(require_api_key)])
def run_generic(job_id: str, step: str, params: RunParams = RunParams()):
    return _run_step(job_id, step, params)


@app.post("/jobs/{job_id}/run-all", response_model=RunAllResponse, dependencies=[Depends(require_api_key)])
def run_all(job_id: str, params: RunParams = RunParams()):
    """Run birdnet (if audio present) then all 6 analyses in order."""
    job = _require_job(job_id)
    tasks = runner.submit_all(job, _params_for_run(job, params))
    return RunAllResponse(
        job_id=job.id,
        tasks=[RunResponse(job_id=job.id, task_id=t["task_id"], step=t["step"], status=t["status"]) for t in tasks],
    )


# --------------------------------------------------------------------------- #
# STATUS
# --------------------------------------------------------------------------- #
@app.get("/jobs/{job_id}/tasks/{task_id}", response_model=TaskInfo, dependencies=[Depends(require_api_key)])
def get_task(job_id: str, task_id: str):
    job = _require_job(job_id)
    t = job.get_task(task_id)
    if t is None:
        raise HTTPException(404, f"Task '{task_id}' not found.")
    return _task_info(t)


@app.get("/jobs/{job_id}/tasks/{task_id}/log", response_class=PlainTextResponse, dependencies=[Depends(require_api_key)])
def get_task_log(job_id: str, task_id: str):
    job = _require_job(job_id)
    t = job.get_task(task_id)
    if t is None:
        raise HTTPException(404, f"Task '{task_id}' not found.")
    log = job.step_results_dir(t["step"]) / "_run.log"
    if not log.is_file():
        return PlainTextResponse("(no log yet)")
    return PlainTextResponse(log.read_text(errors="replace"))


# --------------------------------------------------------------------------- #
# DOWNLOAD API
# --------------------------------------------------------------------------- #
@app.get("/jobs/{job_id}/results", dependencies=[Depends(require_api_key)])
def list_results(job_id: str):
    job = _require_job(job_id)
    s = get_settings()
    return {
        "job_id": job.id,
        "results": job.list_results(),
        # Deep link to the job's output dir in the file-browser service (item 11).
        # None when FILE_BROWSER_BASE_URL is unset.
        "browse_url": s.file_browser_url(job.id),
        "api_version": s.API_VERSION,
    }


@app.get("/jobs/{job_id}/stac", dependencies=[Depends(require_api_key)])
def list_stac(job_id: str):
    """STAC Item sidecars produced for this job's outputs (item 9)."""
    job = _require_job(job_id)
    items = [r for r in job.list_results() if r.endswith(".stac.json")]
    return {"job_id": job.id, "stac_items": items, "count": len(items)}


@app.get("/jobs/{job_id}/download", dependencies=[Depends(require_api_key)])
def download_all(job_id: str):
    """Zip of every result file in the job."""
    job = _require_job(job_id)
    zip_path = _zip_paths(job, job.list_results(), f"{job.id}_all_results.zip")
    return FileResponse(zip_path, media_type="application/zip", filename=zip_path.name)


@app.get("/jobs/{job_id}/download/{step}", dependencies=[Depends(require_api_key)])
def download_step(job_id: str, step: str):
    """Zip of one step's result files."""
    if not meta.is_valid_step(step):
        raise HTTPException(404, f"Unknown step '{step}'.")
    job = _require_job(job_id)
    rels = [r for r in job.list_results() if r.startswith(f"results/{step}/") or
            (step == meta.BIRDNET and r.startswith("work/"))]
    zip_path = _zip_paths(job, rels, f"{job.id}_{step}.zip")
    return FileResponse(zip_path, media_type="application/zip", filename=zip_path.name)


@app.get("/jobs/{job_id}/file", dependencies=[Depends(require_api_key)])
def download_file(job_id: str, path: str = Query(..., description="Result path relative to job root")):
    """Download a single result file by its relative path."""
    job = _require_job(job_id)
    rel = path.replace("\\", "/").lstrip("/")
    target = (job.root / rel).resolve()
    # containment check
    if not str(target).startswith(str(job.root.resolve()) + os.sep):
        raise HTTPException(400, "Path escapes job directory.")
    if not target.is_file():
        raise HTTPException(404, f"File '{path}' not found.")
    return FileResponse(target, filename=target.name)
