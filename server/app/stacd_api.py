"""
CEM Server API -- clean 3-group design.

  1. Upload -- project-level file storage.
  2. Scripts -- single synchronous algorithm endpoint (script name in body).
  3. Polling & download -- job status, results, files.
"""
import csv
from datetime import datetime
import json
import os

import zipfile
from pathlib import Path
from typing import Optional
from urllib.parse import quote

from fastapi import (
    APIRouter, Body, Depends, File, Form, Header, HTTPException, Query, UploadFile,
)
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse

from . import activity_log
from . import airflow_client
from . import jobs as jobstore
from . import pipeline_meta as meta
from . import projects as projectstore
from . import runner
from . import stac
from .safepath import UnsafeComponent, safe_component
from .settings import get_settings
from .debug import debug

router = APIRouter(prefix="/api/v1", tags=["cem"])


# =========================================================================== #
#  Helpers
# =========================================================================== #

def _user(
    x_user_email: Optional[str] = Header(None),
    x_user_id: Optional[str] = Header(None),
) -> dict:
    return {"email": x_user_email, "id": x_user_id}


def _component(value, field: str) -> str:
    """Validate a client-supplied path component; 400 on anything unsafe."""
    try:
        return safe_component(value, field)
    except UnsafeComponent as e:
        raise HTTPException(400, str(e))


def _require_job(job_id: str) -> jobstore.Job:
    _component(job_id, "job_id")
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
    debug("upload.saved", filename=dest.name, bytes=written)
    return written


def _successful_server_jobs(proj: projectstore.Project) -> list[dict]:
    """Completed jobs physically present in this server's DATA_DIR project tree.

    Browser/local-watcher jobs never create server job.json records here, so
    requiring a successful server-side task is the backend guard that keeps
    local-only analysis from being published.
    """
    out: list[dict] = []
    if not proj.root.is_dir():
        return out
    for script_dir in sorted(proj.root.iterdir()):
        if (
            not script_dir.is_dir()
            or script_dir.name in {"dataset", ".git", "__pycache__"}
            or script_dir.name.startswith(".")
            or (script_dir / "audio").is_dir()
        ):
            continue
        for job_dir in sorted(script_dir.iterdir()):
            meta_path = job_dir / "job.json"
            if not (job_dir.is_dir() and meta_path.is_file()):
                continue
            try:
                meta_d = json.loads(meta_path.read_text())
            except Exception:
                continue
            tasks = meta_d.get("tasks") or []
            if any(str(t.get("status") or "").lower() == "success" for t in tasks):
                out.append({
                    "job_id": job_dir.name,
                    "script": script_dir.name,
                    "path": str(job_dir),
                    "task_count": len(tasks),
                })
    return out


def _format_job_date(value) -> str | None:
    if not value:
        return None
    raw = str(value)
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            pass
    return None


def _repair_missing_aggregate_dates(proj: projectstore.Project) -> bool:
    """Fill missing aggregate date/hour for server-compute files with odd names.

    Song Meter filenames carry date/time. Browser-uploaded files like
    recording.wav do not, so BirdNET can produce valid detections with blank
    date/hour. The master indexer requires dates; use the BirdNET job's selected
    start_date as the conservative fallback.
    """
    if not proj.aggregate_path.is_file():
        return False

    fallback_date = None
    for job in _successful_server_jobs(proj):
        if job.get("script") != "birdnet":
            continue
        meta_path = Path(job["path"]) / "job.json"
        try:
            meta_d = json.loads(meta_path.read_text())
        except Exception:
            continue
        for task in reversed(meta_d.get("tasks") or []):
            fallback_date = _format_job_date((task.get("params") or {}).get("start_date"))
            if fallback_date:
                break
        if fallback_date:
            break

    if not fallback_date:
        return False

    with open(proj.aggregate_path, newline="") as src:
        reader = csv.DictReader(src)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    if not rows or "date" not in fieldnames or "hour" not in fieldnames:
        return False

    changed = False
    for row in rows:
        if not (row.get("date") or "").strip():
            row["date"] = fallback_date
            changed = True
        if not (row.get("hour") or "").strip():
            row["hour"] = "0"
            changed = True

    if not changed:
        return False

    tmp = proj.aggregate_path.with_suffix(".csv.tmp")
    with open(tmp, "w", newline="") as dest:
        writer = csv.DictWriter(dest, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(proj.aggregate_path)
    return True


# =========================================================================== #
#  GROUP 1 -- Upload
# =========================================================================== #

@router.get("/projects/status")
def project_status(project: str = Query(..., description="Project folder name")):
    project = _component(project, "project")
    proj = projectstore.get_project(project)
    if proj is None:
        return {
            "project": project,
            "exists": False,
            "visibility": "private",
            "is_public": False,
            "retention_hours": projectstore.PRIVATE_RETENTION_HOURS,
            "missing_on_server": True,
        }
    return proj.status()


@router.get("/projects/audio")
def project_audio(
    project: str = Query(..., description="Project folder name"),
    spot: str | None = Query(default=None, description="Optional spot folder name"),
):
    project = _component(project, "project")
    spot = _component(spot, "spot") if spot else None
    proj = projectstore.get_project(project)
    if proj is None:
        raise HTTPException(404, f"Project '{project}' not found.")
    items = proj.list_audio_records(spot)
    return {
        "project": project,
        "spot": spot,
        "items": items,
        "total": len(items),
    }


@router.get("/projects/snippets")
def project_snippets(
    project: str = Query(..., description="Project folder name"),
):
    project = _component(project, "project")
    proj = projectstore.get_project(project)
    if proj is None:
        raise HTTPException(404, f"Project '{project}' not found.")
    index_path = proj.snippets_index_path
    if not index_path.is_file():
        debug("snippets.missing_index", project=project, reason="Run BirdNET or backfill existing detections")
        return {
            "project": project,
            "total_species_snippets": 0,
            "species": {},
        }
    try:
        payload = json.loads(index_path.read_text())
        debug("snippets.metadata", project=project, count=len(payload.get("species", {})))
        return payload
    except Exception as e:
        raise HTTPException(500, f"Failed to read snippets metadata: {e}")


@router.get("/projects/snippets/audio")
def get_snippet_audio(
    project: str = Query(..., description="Project folder name"),
    filename: str = Query(..., description="Snippet audio filename"),
):
    project = _component(project, "project")
    fname = _safe_name(filename)
    proj = projectstore.get_project(project)
    if proj is None:
        raise HTTPException(404, f"Project '{project}' not found.")
    target = (proj.snippets_dir / fname).resolve()
    if not str(target).startswith(str(proj.snippets_dir.resolve()) + os.sep):
        raise HTTPException(400, "Path escapes snippets directory.")
    if not target.is_file():
        raise HTTPException(404, f"Snippet file '{filename}' not found.")
    debug("snippets.stream", project=project, filename=target.name, bytes=target.stat().st_size)
    return FileResponse(target, media_type="audio/wav", filename=target.name)


@router.post("/projects/check-files")
def check_files(body: dict = Body(...)):
    project = body.get("project")
    if not project:
        raise HTTPException(400, "project is required.")
    project = _component(project, "project")
    files_by_spot = body.get("files", {})
    if not files_by_spot:
        return {"to_upload": {}}

    proj = projectstore.get_or_create_project(project)
    to_upload = {}
    for spot, filenames in files_by_spot.items():
        spot = _component(spot, "spot")
        existing = set(proj.list_audio_files(spot))
        needed = [f for f in filenames if f not in existing]
        if needed:
            to_upload[spot] = needed
    return {"to_upload": to_upload}


@router.post("/projects/upload/audio")
def project_upload_audio(
    project: str = Form(..., description="Project folder name"),
    spot: str = Form(..., description="Spot name for these files"),
    files: list[UploadFile] = File(...),
    user: dict = Depends(_user),
):
    if not files:
        raise HTTPException(400, "No files provided.")
    if not spot.strip():
        raise HTTPException(400, "spot is required.")
    project = _component(project, "project")
    spot = _component(spot, "spot")

    proj = projectstore.get_or_create_project(project)
    audio_dir = proj.spot_audio_dir(spot)
    audio_dir.mkdir(parents=True, exist_ok=True)
    existing = set(p.name for p in audio_dir.iterdir() if p.is_file())

    saved, skipped, skipped_records = [], [], []
    for up in files:
        fname = _safe_name(up.filename)
        if fname in existing:
            skipped.append(fname)
            skipped_records.append(proj.audio_record(spot, fname))
            continue
        size = _save_upload(audio_dir / fname, up)
        record = proj.audio_record(spot, fname)
        record["size_bytes"] = size
        saved.append(record)
        existing.add(fname)

    audio_manifest = proj.write_audio_manifest()
    proj._touch()
    activity_log.append(
        user, "upload_audio",
        project=project, spot=spot,
        uploaded=len(saved), skipped=len(skipped),
    )
    return {
        "status": "ok",
        "project": project,
        "spot": spot,
        "uploaded": saved,
        "skipped": skipped,
        "skipped_records": skipped_records,
        "audio_manifest_count": len(audio_manifest),
        "spot_audio_count": proj.audio_count(spot),
        "total_audio": proj.audio_count(),
    }


@router.post("/projects/upload/aggregate")
def project_upload_aggregate(
    project: str = Form(..., description="Project folder name"),
    file: UploadFile = File(...),
):
    project = _component(project, "project")
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
    project = _component(project, "project")
    proj = projectstore.get_or_create_project(project)
    proj.dataset_dir.mkdir(parents=True, exist_ok=True)
    _save_upload(proj.processed_path, file)
    proj._touch()
    return {"status": "ok", "project": project, "has_processed": True}


@router.post("/projects/publish")
def publish_project(body: dict = Body(...), user: dict = Depends(_user)):
    project = body.get("project")
    if not project:
        raise HTTPException(400, "project is required.")
    project = _component(project, "project")

    proj = projectstore.get_project(project)
    if proj is None:
        raise HTTPException(
            404,
            f"Project '{project}' has no server-side compute data. "
            "Run analysis in server mode before publishing.",
        )

    successful_jobs = _successful_server_jobs(proj)
    if not successful_jobs:
        raise HTTPException(
            409,
            "Project cannot be made public because no completed server-compute "
            "job exists for it. Local watcher jobs are not publishable.",
        )

    birdnet_jobs = [job for job in successful_jobs if job.get("script") == meta.BIRDNET]
    if not birdnet_jobs:
        raise HTTPException(
            409,
            "Project cannot be made public because no completed BirdNET "
            "server-compute job exists for it.",
        )

    if not proj.has_aggregate():
        raise HTTPException(
            409,
            "Project cannot be made public because server compute has not "
            "produced dataset/aggregate.csv yet. Run BirdNET/server analysis first.",
        )

    dry_run = bool(body.get("dry_run", False))
    repaired_aggregate_dates = False if dry_run else _repair_missing_aggregate_dates(proj)
    if dry_run:
        return {
            "status": "dry_run",
            "project": project,
            "server_jobs": successful_jobs,
            "repaired_aggregate_dates": repaired_aggregate_dates,
            "data_dir_visibility_updated": False,
        }

    proj.write_audio_manifest()
    meta_d = proj.mark_public(
        server_jobs=successful_jobs,
        repaired_aggregate_dates=repaired_aggregate_dates,
    )

    activity_log.append(
        user,
        "publish_project",
        project=project,
        server_jobs=len(successful_jobs),
        public_project_path=str(proj.root),
    )

    return {
        "status": "published",
        "project": project,
        "server_jobs": successful_jobs,
        "repaired_aggregate_dates": repaired_aggregate_dates,
        "data_dir_visibility_updated": True,
        "public_project_path": str(proj.root),
        "visibility": meta_d.get("visibility"),
        "retention_hours": meta_d.get("retention_hours"),
        "indexer": {
            "mode": "polling",
            "message": "CEM Master indexer will pick up this public project from DATA_DIR.",
        },
    }


@router.post("/projects/unpublish")
def unpublish_project(body: dict = Body(...), user: dict = Depends(_user)):
    project = body.get("project")
    if not project:
        raise HTTPException(400, "project is required.")
    project = _component(project, "project")

    proj = projectstore.get_project(project)
    if proj is None:
        activity_log.append(user, "unpublish_missing_project", project=project)
        return {
            "status": "private",
            "project": project,
            "data_dir_visibility_updated": False,
            "visibility": "private",
            "retention_hours": projectstore.PRIVATE_RETENTION_HOURS,
            "missing_on_server": True,
            "indexer": {
                "mode": "polling",
                "message": "Project is absent from DATA_DIR, so it is not public.",
            },
        }

    meta_d = proj.mark_private()
    activity_log.append(user, "unpublish_project", project=project)

    return {
        "status": "private",
        "project": project,
        "data_dir_visibility_updated": True,
        "visibility": meta_d.get("visibility"),
        "retention_hours": meta_d.get("retention_hours"),
        "indexer": {
            "mode": "polling",
            "message": "CEM Master indexer will prune this private project from the public catalog.",
        },
    }


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
    common = {"script", "project", "spots", "start_date", "end_date", "spots_geo", "job_id"}
    return {k: v for k, v in body.items() if k not in common and v is not None}


# Structured error code -> HTTP status. The runner attaches the code to the
# task record (no free-text guessing). Anything unknown is treated as 500.
_ERROR_CODE_STATUS: dict[str, int] = {
    "BAD_REQUEST": 400,
    "NO_DATA": 404,
    "PIPELINE_ERROR": 500,
}


def _browse_href(job: jobstore.Job, rel: str) -> Optional[str]:
    s = get_settings()
    if s.STAC_ASSET_BASE_URL:
        return f"{s.STAC_ASSET_BASE_URL}/jobs/{job.id}/{rel}"
    return None


def _resolve_script_name(name: str) -> str:
    """Accept step ID ('birdnet'), filename ('birdnet_predictions.py'), or
    basename without extension ('birdnet_predictions') and return the step ID."""
    if name in meta.SCRIPTS:
        return name
    # Try matching by script filename (with or without .py)
    bare = name.removesuffix(".py")
    for sid, filename in meta.SCRIPTS.items():
        if filename.removesuffix(".py") == bare:
            return sid
    return name  # fall through — will fail validation below


def _birdnet_covers(proj, spots: list, start_date, end_date) -> bool:
    """True if BirdNET has already processed every in-range audio file."""
    in_range = proj.in_range_audio(spots, start_date, end_date)
    if not in_range:
        return True  # nothing to process; the analysis will report NO_DATA itself
    if not proj.has_aggregate():
        return False
    return in_range <= proj.processed_set()


def _ensure_dependencies(step: str, body: dict):
    """Run upstream producers this analysis needs, before the analysis itself.
    Only raw audio is ever uploaded by the client; upstream results are computed
    here. Returns a JSONResponse if a dependency run failed, else None."""
    if not meta.is_analysis(step):
        return None

    project = body.get("project")
    spots = body.get("spots")
    proj = projectstore.get_project(project) if project else None
    if proj is None:
        return None  # _run_script handles the missing-project error

    start_date = body.get("start_date")
    end_date = body.get("end_date")

    for dep in meta.load_manifest().get(step, {}).get("depends_on", []):
        if dep != meta.BIRDNET:
            continue  # birdnet is the only project-level producer today
        if _birdnet_covers(proj, spots, start_date, end_date):
            continue
        dep_body = {
            "project": project,
            "spots": spots,
            "start_date": start_date,
            "end_date": end_date,
            "job_id": jobstore.new_id("job_"),
        }
        for k in ("snr_db", "min_confidence"):
            if body.get(k) is not None:
                dep_body[k] = body[k]
        result = _run_script(meta.BIRDNET, dep_body)
        if isinstance(result, JSONResponse):
            return result
    return None


def _matching_task(job: jobstore.Job, step: str, params: dict) -> Optional[dict]:
    """Most recent task for this job with the same step and identical params."""
    for t in reversed(job.read().get("tasks", [])):
        if t.get("step") == step and t.get("params") == params:
            return t
    return None


def _run_script(script_name: str, body: dict):
    script_name = _resolve_script_name(script_name)
    if not meta.is_valid_step(script_name):
        raise HTTPException(404, f"Unknown script '{script_name}'. Options: {list(meta.SCRIPTS)}")

    project = body.get("project")
    spots = body.get("spots")
    if not project:
        raise HTTPException(400, "project is required.")
    if not spots:
        raise HTTPException(400, "spots is required (list of spot names).")
    project = _component(project, "project")
    spots = [_component(s, "spot") for s in spots]

    start_date = body.get("start_date")
    end_date = body.get("end_date")
    spots_geo = body.get("spots_geo")

    proj = projectstore.get_project(project)
    if proj is None:
        raise HTTPException(404, f"Project '{project}' not found.")

    dep_resp = _ensure_dependencies(script_name, body)
    if dep_resp is not None:
        return dep_resp

    client_job_id = body.get("job_id")
    if not client_job_id:
        raise HTTPException(400, "job_id is required (minted by the client).")
    client_job_id = _component(client_job_id, "job_id")

    if script_name in meta.WAV_SCRIPTS and not proj.in_range_audio(
        spots, start_date=start_date, end_date=end_date
    ):
        raise HTTPException(
            409,
            "No audio files are uploaded on the server for the selected project, "
            "spots, and date range. Import/link WAV files and make sure the "
            "analysis date range includes their filename date.",
        )

    job = jobstore.create_job(project, script_name, job_id=client_job_id)
    stats = proj.populate_job(job, spots=spots, start_date=start_date, end_date=end_date)

    if script_name in meta.WAV_SCRIPTS and stats["audio_linked"] == 0:
        raise HTTPException(
            409,
            "No audio files are uploaded on the server for the selected project, "
            "spots, and date range. Import/link WAV files and make sure the "
            "analysis date range includes their filename date.",
        )

    if spots_geo:
        job.set_geo(spots_geo)

    run_params = _extract_script_params(body)
    run_params["spots"] = spots
    if start_date:
        run_params["start_date"] = start_date
    if end_date:
        run_params["end_date"] = end_date

    # Run-level idempotency: a prior successful task with identical params is
    # returned as-is (no blind re-run). A concurrent run of the same job (lock
    # held) is 409; global saturation is 429.
    existing = _matching_task(job, script_name, run_params)
    if existing is not None and existing.get("status") == "success":
        task = existing
    else:
        try:
            task = runner.run_sync(job, script_name, run_params)
        except runner.JobBusy:
            raise HTTPException(409, f"A run for job '{job.id}' is already in progress.")
        except runner.CapacityError:
            raise HTTPException(429, "Server is at capacity; too many analyses running. Retry shortly.")
    task_id = task["task_id"]

    activity_log.copy_run_log(
        job.step_results_dir(script_name) / "_run.log", job.id, script_name)

    if task["status"] != "success":
        etype = task.get("error_code") or "PIPELINE_ERROR"
        code = _ERROR_CODE_STATUS.get(etype, 500)
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
    # Surface STAC provenance problems even on an otherwise-successful run.
    if task.get("stac_warning"):
        resp["warnings"] = [task["stac_warning"]]
    if len(items) == 1:
        resp["asset_id"] = asset_ids[0]
        resp["stac"] = items[0]
    else:
        resp["asset_id"] = asset_ids
        resp["asset_ids"] = asset_ids
        resp["stac"] = items
    return resp


@router.post("/scripts", summary="Run a pipeline script (pass 'script' in body)")
def run_script(body: dict = Body(...)):
    """Synchronous EXECUTOR. Runs the pipeline to completion and returns the
    result inline. This is what Airflow's DAG node calls back into; it never
    triggers Airflow itself (no dispatch loop). Front-ends should call /analyze."""
    script_name = body.get("script")
    if not script_name:
        raise HTTPException(400, "\"script\" is required in request body. "
                            f"Options: {list(meta.SCRIPTS)}")
    return _run_script(script_name, body)


@router.post("/analyze", summary="Dispatch a script: via Airflow if configured, else run locally")
def analyze(body: dict = Body(...), user: dict = Depends(_user)):
    """Front-end entry point (DISPATCHER).

    - Airflow not configured -> run the pipeline locally and return the result
      inline (blocking, no polling needed).
    - Airflow configured -> trigger the DAG and return the client-minted job_id
      immediately. The client then polls GET /jobs/{job_id}. Airflow calls
      /scripts back with the same job_id to do the actual work.
    """
    script_name = body.get("script")
    if not script_name:
        raise HTTPException(400, "\"script\" is required in request body. "
                            f"Options: {list(meta.SCRIPTS)}")
    job_id = body.get("job_id")
    if not job_id:
        raise HTTPException(400, "job_id is required (minted by the client).")
    job_id = _component(job_id, "job_id")

    activity_log.append(
        user, "run_analysis",
        project=body.get("project"), step=script_name, job_id=job_id,
        spots=body.get("spots"),
        start_date=body.get("start_date"), end_date=body.get("end_date"),
    )

    # Direct mode: identical behaviour to /scripts (synchronous result).
    if not airflow_client.is_configured():
        return _run_script(script_name, body)

    # Airflow mode: validate, pre-create the job (so polls don't 404 in the gap
    # before Airflow calls /scripts back), trigger the DAG, return job_id now.
    resolved = _resolve_script_name(script_name)
    if not meta.is_valid_step(resolved):
        raise HTTPException(404, f"Unknown script '{script_name}'. Options: {list(meta.SCRIPTS)}")
    project = body.get("project")
    spots = body.get("spots")
    if not project:
        raise HTTPException(400, "project is required.")
    if not spots:
        raise HTTPException(400, "spots is required (list of spot names).")
    project = _component(project, "project")
    for s in spots:
        _component(s, "spot")
    if projectstore.get_project(project) is None:
        raise HTTPException(404, f"Project '{project}' not found.")

    job = jobstore.create_job(project, resolved, job_id=job_id)

    conf = {k: v for k, v in body.items() if v is not None}
    conf["script"] = resolved
    conf.setdefault("execution_type", "fullexec")

    s = get_settings()
    try:
        run = airflow_client.trigger_dag(conf)
    except Exception as e:
        job.set_dispatch({"mode": "airflow", "state": "trigger_failed", "error": str(e)})
        return JSONResponse(status_code=502, content={
            "status": "failed",
            "error": "AIRFLOW_TRIGGER_FAILED",
            "message": f"Could not trigger Airflow DAG: {e}",
            "job_id": job_id,
        })

    dag_run_id = run.get("dag_run_id")
    job.set_dispatch({
        "mode": "airflow",
        "state": "queued",
        "dag_id": s.AIRFLOW_DAG_ID,
        "dag_run_id": dag_run_id,
    })
    # The client polls Airflow's dagRun STATE, but a browser can't call Airflow
    # cross-origin (no CORS headers there). So it polls this server's relay
    # (/airflow/dag-run), which fetches Airflow server-side. State still comes
    # from Airflow; creds stay on the server.
    poll_path = f"/api/v1/airflow/dag-run?dag_run_id={quote(dag_run_id, safe='')}"
    return {
        "status": "queued",
        "mode": "airflow",
        "job_id": job_id,
        "dag_run_id": dag_run_id,
        "project": project,
        "script": resolved,
        "poll": {
            "target": "airflow",
            "path": poll_path,           # relative to this server's base URL
            "interval_ms": 5000,
            # Final results/logs are still fetched from this server's disk:
            "results_url": f"/api/v1/jobs/{job_id}",
        },
    }


# =========================================================================== #
#  GROUP 3 -- Polling & download
# =========================================================================== #

@router.get("/airflow/dag-run", summary="Relay an Airflow dagRun's state (CORS-safe proxy)")
def airflow_dag_run(dag_run_id: str = Query(..., description="Airflow dag_run_id to poll")):
    """Browser-facing relay: the front-end can't call Airflow cross-origin, so it
    polls this endpoint, which fetches the dagRun server-side and returns its
    state. Auth lives on the server; only the state is exposed."""
    if not airflow_client.is_configured():
        raise HTTPException(400, "Airflow is not configured on this server.")
    try:
        run = airflow_client.get_dag_run(dag_run_id)
    except Exception as e:
        raise HTTPException(502, f"Could not reach Airflow: {e}")
    return {"dag_run_id": dag_run_id, "state": run.get("state")}


def _job_status(meta_d: dict) -> dict:
    """Derive a single normalized status for polling.

    Fast path = our own job store (rich: structured error_code, results,
    warnings). Fallback = Airflow dagRun state, used only while we have no
    terminal task yet, so orchestration-level failures (DAG never reached
    /scripts) don't leave the client polling forever.
    """
    tasks = meta_d.get("tasks", [])
    if tasks:
        last = tasks[-1]
        st = last.get("status")
        if st == "success":
            out = {"status": "completed", "source": "server"}
            if last.get("stac_warning"):
                out["warnings"] = [last["stac_warning"]]
            return out
        if st == "failed":
            return {
                "status": "failed",
                "error": last.get("error_code") or "PIPELINE_ERROR",
                "message": last.get("error"),
                "source": "server",
            }

    dispatch = meta_d.get("dispatch") or {}
    if dispatch.get("state") == "trigger_failed":
        return {
            "status": "failed",
            "error": "AIRFLOW_TRIGGER_FAILED",
            "message": dispatch.get("error"),
            "source": "server",
        }

    dag_run_id = dispatch.get("dag_run_id")
    if dag_run_id and airflow_client.is_configured():
        try:
            astate = (airflow_client.get_dag_run(dag_run_id).get("state") or "").lower()
        except Exception:
            astate = ""
        if astate == "failed":
            return {
                "status": "failed",
                "error": "AIRFLOW_FAILED",
                "message": "Airflow DAG run failed before producing results.",
                "source": "airflow",
            }
        # success-at-airflow but no terminal task yet = our store still finalizing
        # -> report running so the next poll resolves it from the fast path.
        if astate in ("queued", "running", "success"):
            return {"status": "queued" if astate == "queued" else "running",
                    "source": "airflow"}

    return {"status": "running" if tasks else "queued", "source": "server"}

@router.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = _require_job(job_id)
    meta_d = job.read()
    s = get_settings()
    status = _job_status(meta_d)
    return {
        "job_id": job.id,
        "status": status["status"],
        "status_detail": status,
        "project": meta_d.get("project"),
        "script": meta_d.get("script"),
        "created_at": meta_d.get("created_at"),
        "dispatch": meta_d.get("dispatch") or {},
        "inputs": {
            "audio_files": sorted(p.name for p in job.audio_dir.glob("*")) if job.audio_dir.is_dir() else [],
            "uploaded_aggregate": job.uploaded_aggregate.is_file(),
        },
        "has_aggregate": job.resolve_aggregate() is not None,
        "tasks": meta_d.get("tasks", []),
        "results": job.list_results(),
        "shares": meta_d.get("shares") or {},
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


# =========================================================================== #
#  GROUP 4 -- Stratification (GEE)
# =========================================================================== #

@router.post("/stratify")
def stratify(body: dict = Body(...), user: dict = Depends(_user)):
    kml_content = body.get("kml_content")
    if not kml_content:
        raise HTTPException(400, "kml_content is required (base64 or raw string).")

    import base64
    try:
        kml_bytes = base64.b64decode(kml_content)
    except Exception:
        kml_bytes = kml_content.encode("utf-8") if isinstance(kml_content, str) else kml_content

    max_clusters = int(body.get("max_clusters", 5))
    year = int(body.get("year", 2024))
    scale = int(body.get("scale", 10))
    num_pixels = int(body.get("num_pixels", 1000))

    try:
        from .gee_processor import generate_stratification
        results = generate_stratification(
            kml_bytes,
            max_clusters=max_clusters,
            year=year,
            scale=scale,
            num_pixels=num_pixels,
        )
    except Exception as e:
        raise HTTPException(500, f"Stratification failed: {e}")

    activity_log.append(
        user, "stratify",
        project=body.get("project"), max_clusters=max_clusters, year=year,
    )
    return {"status": "ok", "results": results}


@router.get("/stratify/overlay/{overlay_id}")
def get_overlay(overlay_id: str):
    safe_id = os.path.basename(overlay_id).replace("..", "")
    fpath = get_settings().DATA_DIR / "stratification_overlays" / f"{safe_id}.png"
    if not fpath.is_file():
        raise HTTPException(404, "Overlay not found.")
    return FileResponse(
        fpath,
        media_type="image/png",
        filename=f"{safe_id}.png",
        headers={"Access-Control-Allow-Origin": "*"},
    )
