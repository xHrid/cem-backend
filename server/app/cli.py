"""
Stable CLI entrypoint for Airflow integration.
"""
import argparse
import glob
import json
import shutil
import sys
import time
from pathlib import Path

from . import jobs as jobstore
from . import pipeline_meta as meta
from . import retention
from . import runner
from .settings import get_settings

_FIXED_NAMES = {
    "aggregate": "aggregate.csv",
    "processed": "processed_files.txt",
    "ebird": "ebird_checklist.txt",
    "static_noise": "static_noise.wav",
    "rain_noise": "rain_noise.wav",
}
_TERMINAL = {"success", "failed"}


def _expand(paths: list[str]) -> list[str]:
    out: list[str] = []
    for p in paths:
        matches = glob.glob(p)
        out.extend(matches if matches else [p])
    return out


def _copy_into_job(job: jobstore.Job, kind: str, src_paths: list[str], spot: str | None) -> int:
    n = 0
    for src in _expand(src_paths):
        sp = Path(src)
        if not sp.is_file():
            print(f"  skip (not a file): {src}", file=sys.stderr)
            continue
        if kind == "audio":
            dest = job.audio_dir / sp.name
        elif kind == "reference":
            dest = job.reference_dir / sp.name
        elif kind in _FIXED_NAMES:
            dest = job.input_dir / _FIXED_NAMES[kind]
        else:
            raise SystemExit(f"Unknown kind '{kind}'.")
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(sp, dest)
        if kind == "reference":
            job.set_reference_spot(sp.name, spot)
        n += 1
    return n


def _apply_geo(job: jobstore.Job, geo_path: str | None) -> None:
    if not geo_path:
        return
    data = json.loads(Path(geo_path).read_text())
    job.set_geo(data)


def _run_params(args) -> dict:
    p: dict = {}
    if getattr(args, "spots", None):
        p["spots"] = [s for s in args.spots.split(",") if s]
    if getattr(args, "start", None):
        p["start_date"] = args.start
    if getattr(args, "end", None):
        p["end_date"] = args.end
    if getattr(args, "snr", None) is not None:
        p["snr_db"] = args.snr
    for attr in ("min_confidence", "top_n_species", "top_n_temporal",
                 "sci_threshold", "kurtosis_threshold", "pmr_threshold",
                 "window_size", "min_solar_days", "max_timeseries_species"):
        v = getattr(args, attr, None)
        if v is not None:
            p[attr] = v
    return p


def _wait(job: jobstore.Job, task_ids: list[str], poll: float = 2.0) -> bool:
    ok = True
    pending = set(task_ids)
    while pending:
        time.sleep(poll)
        for tid in list(pending):
            t = job.get_task(tid)
            if t and t["status"] in _TERMINAL:
                pending.discard(tid)
                status = t["status"]
                print(f"  [{t['step']}] {status}" + (f" - {t['error']}" if t.get("error") else ""))
                if status == "failed":
                    ok = False
    return ok


def cmd_create_job(args) -> int:
    job = jobstore.create_job(args.project, args.step or "birdnet")
    _apply_geo(job, args.geo)
    print(job.id)
    return 0


def cmd_upload(args) -> int:
    job = jobstore.get_job(args.job) or _die(f"Job '{args.job}' not found.")
    n = _copy_into_job(job, args.kind, args.paths, args.spot)
    print(f"uploaded {n} file(s) as kind={args.kind}")
    return 0


def cmd_run(args) -> int:
    job = jobstore.get_job(args.job) or _die(f"Job '{args.job}' not found.")
    _apply_geo(job, args.geo)
    if not meta.is_valid_step(args.step):
        _die(f"Unknown step '{args.step}'. One of: {list(meta.SCRIPTS)}")
    try:
        task = runner.submit(job, args.step, _run_params(args))
    except ValueError as e:
        _die(str(e))
    print(f"task {task['task_id']} ({args.step}) submitted")
    if args.wait:
        return 0 if _wait(job, [task["task_id"]]) else 1
    return 0


def cmd_run_all(args) -> int:
    job = jobstore.get_job(args.job) or _die(f"Job '{args.job}' not found.")
    _apply_geo(job, args.geo)
    tasks = runner.submit_all(job, _run_params(args))
    ids = [t["task_id"] for t in tasks]
    print(f"submitted {len(ids)} task(s): {[t['step'] for t in tasks]}")
    if args.wait:
        return 0 if _wait(job, ids) else 1
    return 0


def cmd_ingest(args) -> int:
    job = jobstore.create_job(args.project, args.step if args.step != "all" else "birdnet")
    _apply_geo(job, args.geo)
    src = args.audio
    paths = [str(p) for p in Path(src).rglob("*.wav")] if Path(src).is_dir() else [src]
    n = _copy_into_job(job, "audio", paths, None)
    print(f"job {job.id}: ingested {n} audio file(s)")
    if args.step == "all":
        tasks = runner.submit_all(job, _run_params(args))
    else:
        if not meta.is_valid_step(args.step):
            _die(f"Unknown step '{args.step}'.")
        tasks = [runner.submit(job, args.step, _run_params(args))]
    ids = [t["task_id"] for t in tasks]
    print(f"job {job.id}: submitted {[t['step'] for t in tasks]}")
    if args.wait:
        return 0 if _wait(job, ids) else 1
    return 0


def cmd_status(args) -> int:
    job = jobstore.get_job(args.job) or _die(f"Job '{args.job}' not found.")
    meta_d = job.read()
    if args.task:
        print(json.dumps(job.get_task(args.task), indent=2))
    else:
        print(json.dumps({
            "job_id": job.id,
            "tasks": [{"step": t["step"], "status": t["status"], "task_id": t["task_id"]}
                      for t in meta_d.get("tasks", [])],
            "results": job.list_results(),
            "browse_url": get_settings().file_browser_url(job.id),
        }, indent=2))
    return 0


def cmd_cleanup(args) -> int:
    removed = retention.sweep_once(args.hours)
    print(f"removed {len(removed)} job(s): {removed}")
    return 0


def _die(msg: str):
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(2)


def _add_run_flags(p):
    p.add_argument("--spots", help="comma-separated spot names")
    p.add_argument("--start", help="start date YYYYMMDD or YYYY-MM-DD")
    p.add_argument("--end", help="end date YYYYMMDD or YYYY-MM-DD")
    p.add_argument("--snr", type=float, help="BirdNET denoise SNR dB")
    p.add_argument("--geo", help="path to JSON file with spot coords")
    p.add_argument("--wait", action="store_true", help="block until tasks finish")
    p.add_argument("--min-confidence", type=float)
    p.add_argument("--top-n-species", type=int)
    p.add_argument("--top-n-temporal", type=int)
    p.add_argument("--sci-threshold", type=float)
    p.add_argument("--kurtosis-threshold", type=float)
    p.add_argument("--pmr-threshold", type=float)
    p.add_argument("--window-size", type=int)
    p.add_argument("--min-solar-days", type=int)
    p.add_argument("--max-timeseries-species", type=int)


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="app.cli", description="CEM bioacoustics CLI")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("create-job")
    p.add_argument("--project", required=True)
    p.add_argument("--step", default="birdnet")
    p.add_argument("--geo")
    p.set_defaults(func=cmd_create_job)

    p = sub.add_parser("upload")
    p.add_argument("--job", required=True)
    p.add_argument("--kind", default="audio",
                   choices=["audio", "reference", "aggregate", "processed", "ebird", "static_noise", "rain_noise"])
    p.add_argument("--spot", default=None)
    p.add_argument("paths", nargs="+")
    p.set_defaults(func=cmd_upload)

    p = sub.add_parser("run")
    p.add_argument("--job", required=True)
    p.add_argument("--step", required=True)
    _add_run_flags(p)
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("run-all")
    p.add_argument("--job", required=True)
    _add_run_flags(p)
    p.set_defaults(func=cmd_run_all)

    p = sub.add_parser("ingest")
    p.add_argument("--project", required=True)
    p.add_argument("--audio", required=True)
    p.add_argument("--step", default="all")
    _add_run_flags(p)
    p.set_defaults(func=cmd_ingest)

    p = sub.add_parser("status")
    p.add_argument("--job", required=True)
    p.add_argument("--task", default=None)
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("cleanup")
    p.add_argument("--hours", type=float, default=None)
    p.set_defaults(func=cmd_cleanup)

    return ap


def main() -> None:
    ap = build_parser()
    args = ap.parse_args()
    raise SystemExit(args.func(args))


if __name__ == "__main__":
    main()
