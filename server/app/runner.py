"""
Script runner. Builds the config.apply_overrides() CLI for each pipeline step,
launches it as a subprocess (cwd = PIPELINE_DIR so `import config` works),
streams combined stdout/stderr to results/<step>/_run.log, and updates the
job's task status.

Execution model: SYNCHRONOUS. ``run_sync`` runs the step to completion on the
calling thread and the HTTP response itself is the completion signal — this is
what the STACD/Airflow algorithm API requires (no task polling on this server).
"""
import shutil
import subprocess
from datetime import datetime, timezone

from . import pipeline_meta as meta
from . import stac
from .jobs import Job
from .settings import get_settings

_DEFAULT_START = "19700101"
_DEFAULT_END = "20991231"

# Exit code a pipeline script may use to signal "ran fine, but there was no data
# to act on" (no detections, empty aggregate). Mapped to NO_DATA instead of a
# generic failure. Any other non-zero code is treated as PIPELINE_ERROR.
_NO_DATA_EXIT = 3


# --------------------------------------------------------------------------- #
# Structured errors — each carries a machine code + HTTP status so the API
# never has to guess severity from free-text messages.
# --------------------------------------------------------------------------- #
class RunError(Exception):
    code = "PIPELINE_ERROR"
    http_status = 500


class BadRequestError(RunError):
    code = "BAD_REQUEST"
    http_status = 400


class NoDataError(RunError):
    code = "NO_DATA"
    http_status = 404


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm_date(value: str | None, default: str) -> str:
    if not value:
        return default
    v = value.strip().replace("-", "")
    if len(v) != 8 or not v.isdigit():
        raise BadRequestError(f"Bad date '{value}', expected YYYY-MM-DD or YYYYMMDD")
    return v


def _common_filter_flags(params: dict) -> list[str]:
    flags: list[str] = []
    spots = params.get("spots")
    if spots:
        flags += ["--spots", ",".join(spots)]
    flags += ["--start-date", _norm_date(params.get("start_date"), _DEFAULT_START)]
    flags += ["--end-date", _norm_date(params.get("end_date"), _DEFAULT_END)]
    return flags


# Per-step algorithm tunables -> config.apply_overrides() CLI flag.
_SHARED_FILTER_TUNABLES: dict[str, str] = {
    "filter_confidence": "--filter-confidence",
    "filter_min_detections": "--filter-min-detections",
}

_STEP_TUNABLES: dict[str, dict[str, str]] = {
    "birdnet": {"min_confidence": "--min-confidence"},
    "acoustic_indices": {},
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
    """CLI flags for this step's algorithm tunables present in params."""
    flags: list[str] = []
    for key, flag in _STEP_TUNABLES.get(step, {}).items():
        val = params.get(key)
        if val not in (None, ""):
            flags += [flag, str(val)]
    return flags


def build_command(job: Job, step: str, params: dict) -> list[str]:
    """Return argv for the given step. Raises RunError on missing prerequisites."""
    s = get_settings()
    script = s.PIPELINE_DIR / meta.SCRIPTS[step]
    cmd = [s.PYTHON_BIN, str(script)]

    if step == meta.BIRDNET:
        if not job.has_audio():
            raise NoDataError("No audio uploaded. Upload WAV files before running birdnet.")
        if (job.uploaded_aggregate.is_file()
                and job.uploaded_aggregate.stat().st_size > 0
                and not job.work_aggregate.is_file()):
            job.work_dir.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(job.uploaded_aggregate, job.work_aggregate)
        if (job.uploaded_processed.is_file()
                and not job.processed_file.is_file()):
            job.work_dir.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(job.uploaded_processed, job.processed_file)
        cmd += ["--datasets", str(job.audio_dir)]

        all_files, all_spots = [], []
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

    if step == meta.ACOUSTIC_INDICES:
        if not job.has_audio():
            raise NoDataError("No audio uploaded. Upload WAV files before running acoustic_indices.")
        out_dir = job.step_results_dir(step)
        out_dir.mkdir(parents=True, exist_ok=True)
        indices_agg = out_dir / "indices_aggregate.csv"
        indices_proc = out_dir / "indices_processed_files.txt"
        cmd += ["--datasets", str(job.audio_dir)]
        all_files, all_spots = [], []
        audio_spots = job.get_audio_spots()
        if audio_spots and job.audio_dir.is_dir():
            for f in sorted(job.audio_dir.iterdir()):
                if f.is_file() and f.name in audio_spots:
                    all_files.append(str(f))
                    all_spots.append(audio_spots[f.name] or "_")
        if all_files:
            cmd += ["--input-file-list", *all_files]
            cmd += ["--input-file-spots", *all_spots]
        cmd += ["--aggregate-file-indices", str(indices_agg)]
        cmd += ["--processed-file-indices", str(indices_proc)]
        cmd += ["--output-dir", str(out_dir)]
        cmd += ["--noise-path", str(job.static_noise())]
        if params.get("snr_db") is not None:
            cmd += ["--snr-db", str(params["snr_db"])]
        cmd += _common_filter_flags(params)
        cmd += _tunable_flags(step, params)
        return cmd

    # analysis steps
    agg = job.resolve_aggregate()
    if agg is None:
        raise NoDataError(
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

    # ---- prepare command (validation / prerequisite errors) ----
    try:
        cmd = build_command(job, step, params)
    except RunError as e:
        log_path.write_text(f"PREP ERROR [{e.code}]: {e}\n")
        job.update_task(task_id, status="failed", finished_at=_now(),
                        error=str(e), error_code=e.code)
        return
    except Exception as e:
        log_path.write_text(f"PREP ERROR: {e}\n")
        job.update_task(task_id, status="failed", finished_at=_now(),
                        error=str(e), error_code="PIPELINE_ERROR")
        return

    # ---- run the pipeline subprocess ----
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
    except Exception as e:
        job.update_task(task_id, status="failed", finished_at=_now(),
                        error=str(e), error_code="PIPELINE_ERROR")
        return

    if rc != 0:
        if rc == _NO_DATA_EXIT:
            code, msg = "NO_DATA", "No data to process (no detections / empty aggregate)."
        else:
            code, msg = "PIPELINE_ERROR", f"Script exited with code {rc}. See _run.log."
        job.update_task(task_id, status="failed", finished_at=_now(),
                        returncode=rc, error=msg, error_code=code)
        return

    # ---- success: collect results + write STAC sidecars ----
    results = _result_files(job, step)
    stac_warning = None
    try:
        sidecars = stac.write_items(
            job.root, job.id, step, results, params, job.get_geo())
        if sidecars:
            with open(log_path, "a") as log:
                log.write(f"\nSTAC: wrote {len(sidecars)} item(s).\n")
            results = results + sidecars
    except Exception as e:
        # Surface STAC failures instead of hiding them — the run still produced
        # results, but provenance is incomplete, and the user should know.
        stac_warning = f"STAC sidecar generation failed: {e}"
        try:
            with open(log_path, "a") as log:
                log.write(f"\nSTAC WARNING: {stac_warning}\n")
        except Exception:
            pass

    job.update_task(
        task_id, status="success", finished_at=_now(), returncode=rc,
        error=None, error_code=None, results=results, stac_warning=stac_warning,
    )


def run_sync(job: Job, step: str, params: dict) -> dict:
    """Run a step to completion on the calling thread (BLOCKING) and return the
    final task record. Used by the STACD/Airflow synchronous algorithm API,
    where the HTTP response itself is the completion signal (no polling)."""
    if not meta.is_valid_step(step):
        raise ValueError(f"Unknown step '{step}'")
    task = job.add_task(step, params)
    _execute(job, task["task_id"], step, params)
    return job.get_task(task["task_id"])
