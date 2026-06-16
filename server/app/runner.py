"""
Script runner. Builds the config.apply_overrides() CLI for each pipeline step,
launches it as a subprocess (cwd = PIPELINE_DIR so `import config` works),
streams combined stdout/stderr to results/<step>/_run.log, and updates the
job's task status. Tasks run on a bounded thread pool (async model: the API
returns a task_id immediately and the caller polls status).
"""
import shutil
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from . import pipeline_meta as meta
from . import stac
from .jobs import Job
from .settings import get_settings

_DEFAULT_START = "19700101"
_DEFAULT_END = "20991231"

_settings = get_settings()
_POOL = ThreadPoolExecutor(max_workers=_settings.MAX_CONCURRENT_TASKS)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm_date(value: Optional[str], default: str) -> str:
    if not value:
        return default
    v = value.strip().replace("-", "")
    if len(v) != 8 or not v.isdigit():
        raise ValueError(f"Bad date '{value}', expected YYYY-MM-DD or YYYYMMDD")
    return v


def _common_filter_flags(params: dict) -> list[str]:
    flags: list[str] = []
    spots = params.get("spots")
    if spots:
        flags += ["--spots", ",".join(spots)]
    flags += ["--start-date", _norm_date(params.get("start_date"), _DEFAULT_START)]
    flags += ["--end-date", _norm_date(params.get("end_date"), _DEFAULT_END)]
    return flags


# Per-step algorithm tunables -> config.apply_overrides() CLI flag. Each is read
# by exactly one script; forwarded only when present in the run params (so the
# script keeps its config.py default otherwise). snr_db is handled inline above
# for parity with the original birdnet wiring.
# Shared filter tunables forwarded to every analysis step (filter_utils 3-step).
_SHARED_FILTER_TUNABLES: dict[str, str] = {
    "filter_confidence": "--filter-confidence",
    "filter_min_detections": "--filter-min-detections",
}

_STEP_TUNABLES: dict[str, dict[str, str]] = {
    "birdnet": {"min_confidence": "--min-confidence"},
    "heatmaps": {"top_n_species": "--top-n-species", **_SHARED_FILTER_TUNABLES},
    "temporal_stickiness": {"top_n_temporal": "--top-n-temporal", **_SHARED_FILTER_TUNABLES},
    "spatial_stickiness": {**_SHARED_FILTER_TUNABLES},
    "migratory_classification": {
        "sci_threshold": "--sci-threshold",
        "kurtosis_threshold": "--kurtosis-threshold",
        "pmr_threshold": "--pmr-threshold",
        "window_size": "--window-size",
        **_SHARED_FILTER_TUNABLES,
    },
    "solar_correlation": {"min_solar_days": "--min-solar-days", **_SHARED_FILTER_TUNABLES},
    "daily_timeseries": {"max_timeseries_species": "--max-timeseries-species", **_SHARED_FILTER_TUNABLES},
}


def _tunable_flags(step: str, params: dict) -> list[str]:
    """CLI flags for this step's algorithm tunables present in `params`."""
    flags: list[str] = []
    for key, flag in _STEP_TUNABLES.get(step, {}).items():
        val = params.get(key)
        if val not in (None, ""):
            flags += [flag, str(val)]
    return flags


def build_command(job: Job, step: str, params: dict) -> list[str]:
    """Return argv for the given step. Raises ValueError on missing prerequisites."""
    s = get_settings()
    script = s.PIPELINE_DIR / meta.SCRIPTS[step]
    cmd = [s.PYTHON_BIN, str(script)]

    if step == meta.BIRDNET:
        if not job.has_audio() and not job.get_reference_spots():
            raise ValueError("No audio uploaded. Upload WAV files (kind=audio) before running birdnet.")
        # Aggregate sync (item 13): if the webapp seeded the job with the local
        # birdnet_results.csv (kind=aggregate) and birdnet hasn't produced its own
        # work aggregate yet, copy it in so BirdNET APPENDS to it — the returned
        # aggregate is then the local one merged with the new detections.
        if (job.uploaded_aggregate.is_file()
                and job.uploaded_aggregate.stat().st_size > 0
                and not job.work_aggregate.is_file()):
            job.work_dir.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(job.uploaded_aggregate, job.work_aggregate)
        # Dedup parity: seed the work processed-list from the uploaded one so
        # BirdNET skips files already processed in a previous (server) run, just
        # like the watcher does off its persistent processed_<script>.txt. The
        # merged list is returned so the webapp can persist it locally.
        if (job.uploaded_processed.is_file()
                and not job.processed_file.is_file()):
            job.work_dir.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(job.uploaded_processed, job.processed_file)
        cmd += ["--datasets", str(job.audio_dir)]

        # Collect ALL file→spot overrides (references + uploaded audio) into
        # a single --input-file-list / --input-file-spots pair so BirdNET
        # writes the UI-selected spot name into the aggregate CSV.
        all_files, all_spots = [], []

        ref_spots = job.get_reference_spots()
        if ref_spots:
            for base, spot in ref_spots.items():
                fp = job.reference_dir / base
                if fp.is_file():
                    all_files.append(str(fp))
                    all_spots.append(spot if spot else "_")

        audio_spots = job.get_audio_spots()
        if audio_spots and job.audio_dir.is_dir():
            for f in sorted(job.audio_dir.iterdir()):
                if f.is_file() and f.name in audio_spots:
                    all_files.append(str(f))
                    all_spots.append(audio_spots[f.name] or "_")

        if all_files:
            cmd += ["--input-file-list", *all_files]
            cmd += ["--input-file-spots", *all_spots]

        cmd += ["--aggregate-file", str(job.work_aggregate)]
        cmd += ["--processed-file", str(job.processed_file)]
        cmd += ["--output-csv", str(job.output_csv)]
        cmd += ["--ebird-file", str(job.ebird_file())]
        cmd += ["--noise-path", str(job.static_noise())]
        cmd += ["--rain-path", str(job.rain_noise())]
        if params.get("snr_db") is not None:
            cmd += ["--snr-db", str(params["snr_db"])]
        cmd += _common_filter_flags(params)
        cmd += _tunable_flags(step, params)
        return cmd

    # analysis steps
    agg = job.resolve_aggregate()
    if agg is None:
        raise ValueError(
            "No aggregate available. Run birdnet first, or upload an aggregate CSV (kind=aggregate)."
        )
    out_dir = job.step_results_dir(step)
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd += ["--aggregate-file", str(agg)]
    cmd += ["--ebird-file", str(job.ebird_file())]
    cmd += ["--output-dir", str(out_dir)]
    cmd += _common_filter_flags(params)
    cmd += _tunable_flags(step, params)
    return cmd


def _result_files(job: Job, step: str) -> list[str]:
    if step == meta.BIRDNET:
        out = []
        for p in (job.output_csv, job.work_aggregate, job.processed_file):
            if p.is_file():
                out.append(str(p.relative_to(job.root)).replace("\\", "/"))
        return out
    d = job.step_results_dir(step)
    if not d.is_dir():
        return []
    return [
        str(p.relative_to(job.root)).replace("\\", "/")
        for p in sorted(d.rglob("*")) if p.is_file() and p.name != "_run.log"
    ]


def _execute(job: Job, task_id: str, step: str, params: dict) -> None:
    job.update_task(task_id, status="running", started_at=_now())
    step_dir = job.step_results_dir(step)
    step_dir.mkdir(parents=True, exist_ok=True)
    log_path = step_dir / "_run.log"
    try:
        cmd = build_command(job, step, params)
    except Exception as e:
        log_path.write_text(f"PREP ERROR: {e}\n")
        job.update_task(task_id, status="failed", finished_at=_now(), error=str(e))
        return

    try:
        with open(log_path, "w") as log:
            log.write("CMD: " + " ".join(cmd) + "\n\n")
            log.flush()
            proc = subprocess.run(
                cmd,
                cwd=str(get_settings().PIPELINE_DIR),
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
            )
        rc = proc.returncode
        status = "success" if rc == 0 else "failed"
        err = None if rc == 0 else f"Script exited with code {rc}. See _run.log."
        results = _result_files(job, step)
        # STAC provenance per output (item 9 + 12). Best-effort; appends the
        # generated <file>.stac.json sidecars to the task's result list.
        if rc == 0:
            try:
                with open(log_path, "a") as log:
                    sidecars = stac.write_items(
                        job.root, job.id, step, results, params, job.get_geo())
                    if sidecars:
                        log.write(f"\nSTAC: wrote {len(sidecars)} item(s).\n")
                results = results + sidecars
            except Exception as e:
                pass
        job.update_task(
            task_id, status=status, finished_at=_now(), returncode=rc,
            error=err, results=results,
        )
    except Exception as e:
        job.update_task(task_id, status="failed", finished_at=_now(), error=str(e))


def submit(job: Job, step: str, params: dict) -> dict:
    """Create a task record and schedule it. Returns the task record."""
    if not meta.is_valid_step(step):
        raise ValueError(f"Unknown step '{step}'")
    task = job.add_task(step, params)
    _POOL.submit(_execute, job, task["task_id"], step, params)
    return task


def run_sync(job: Job, step: str, params: dict) -> dict:
    """Run a step to completion on the calling thread (BLOCKING) and return the
    final task record. Used by the STACD/Airflow synchronous algorithm API,
    where the HTTP response itself is the completion signal (no polling)."""
    if not meta.is_valid_step(step):
        raise ValueError(f"Unknown step '{step}'")
    task = job.add_task(step, params)
    _execute(job, task["task_id"], step, params)
    return job.get_task(task["task_id"])


def _run_all_worker(job: Job, task_ids: dict, params: dict) -> None:
    """Sequential birdnet -> analyses in one thread (correct ordering)."""
    for step in meta.RUN_ORDER:
        tid = task_ids[step]
        if step == meta.BIRDNET:
            if not job.has_audio() and not job.get_reference_spots():
                job.update_task(tid, status="failed", finished_at=_now(),
                                error="skipped: no audio uploaded")
                continue
        else:
            if job.resolve_aggregate() is None:
                job.update_task(tid, status="failed", finished_at=_now(),
                                error="skipped: no aggregate available")
                continue
        _execute(job, tid, step, params)


def submit_all(job: Job, params: dict) -> list[dict]:
    """Create task records for every step and run them in order on one worker."""
    tasks = {step: job.add_task(step, params) for step in meta.RUN_ORDER}
    task_ids = {step: t["task_id"] for step, t in tasks.items()}
    _POOL.submit(_run_all_worker, job, task_ids, params)
    return list(tasks.values())
