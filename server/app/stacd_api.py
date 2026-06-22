"""
CEM Server API -- clean 3-group design.

  1. Upload -- project-level file storage.
  2. Scripts -- single synchronous algorithm endpoint (script name in body).
  3. Polling & download -- job status, results, files.
"""
import json
import os

import zipfile
from pathlib import Path
from typing import Optional
from urllib.parse import quote

from fastapi import APIRouter, Body, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse

from . import airflow_client
from . import jobs as jobstore
from . import pipeline_meta as meta
from . import projects as projectstore
from . import runner
from . import stac
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
    if s.FILE_BROWSER_BASE_URL:
        return f"{s.FILE_BROWSER_BASE_URL}/jobs/{job.id}/{rel}"
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

    start_date = body.get("start_date")
    end_date = body.get("end_date")
    spots_geo = body.get("spots_geo")

    proj = projectstore.get_project(project)
    if proj is None:
        raise HTTPException(404, f"Project '{project}' not found.")

    client_job_id = body.get("job_id")
    if not client_job_id:
        raise HTTPException(400, "job_id is required (minted by the client).")
    job = jobstore.create_job(project, script_name, job_id=client_job_id)
    stats = proj.populate_job(job, spots=spots, start_date=start_date, end_date=end_date)

    if spots_geo:
        job.set_geo(spots_geo)

    run_params = _extract_script_params(body)
    run_params["spots"] = spots
    if start_date:
        run_params["start_date"] = start_date
    if end_date:
        run_params["end_date"] = end_date

    task = runner.run_sync(job, script_name, run_params)
    task_id = task["task_id"]

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
def analyze(body: dict = Body(...)):
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
