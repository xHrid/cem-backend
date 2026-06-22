"""
Output retention cleanup.

Jobs now live inside projects: <DATA_DIR>/projects/<project>/<script>/<job_id>/.
The sweeper walks all project/script dirs and removes expired job dirs.
"""
import shutil
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from .settings import get_settings
from . import pipeline_meta as meta


def _last_activity(job_dir: Path) -> float:
    """Job last-activity time. job.json is rewritten on every task update, so its
    mtime tracks activity in O(1) — no need to walk the whole tree each sweep."""
    return (job_dir / "job.json").stat().st_mtime


def sweep_once(retention_hours: float | None = None) -> list[str]:
    s = get_settings()
    hours = s.RETENTION_HOURS if retention_hours is None else retention_hours
    if hours <= 0:
        return []
    cutoff = time.time() - hours * 3600
    projects_dir = s.projects_dir
    removed: list[str] = []
    if not projects_dir.is_dir():
        return removed

    script_names = set(meta.SCRIPTS.keys())

    for proj_dir in projects_dir.iterdir():
        if not proj_dir.is_dir():
            continue
        for script_dir in proj_dir.iterdir():
            if not script_dir.is_dir() or script_dir.name not in script_names:
                continue
            for job_dir in script_dir.iterdir():
                if not job_dir.is_dir() or not (job_dir / "job.json").is_file():
                    continue
                try:
                    if _last_activity(job_dir) < cutoff:
                        shutil.rmtree(job_dir, ignore_errors=True)
                        idx = s.jobs_index_dir / f"{job_dir.name}.json"
                        idx.unlink(missing_ok=True)
                        removed.append(job_dir.name)
                except OSError:
                    continue

    if removed:
        ts = datetime.now(timezone.utc).isoformat()
        print(f"[retention] {ts} removed {len(removed)} job(s) older than {hours}h: {removed}")
    return removed


def _loop() -> None:
    s = get_settings()
    interval = max(1.0, s.RETENTION_SWEEP_MINUTES) * 60
    while True:
        try:
            sweep_once()
        except Exception as e:
            print(f"[retention] sweep error: {e}")
        time.sleep(interval)


_thread: threading.Thread | None = None


def start_background() -> bool:
    global _thread
    s = get_settings()
    if s.RETENTION_HOURS <= 0 or s.RETENTION_SWEEP_MINUTES <= 0:
        return False
    if _thread and _thread.is_alive():
        return True
    _thread = threading.Thread(target=_loop, name="retention-sweeper", daemon=True)
    _thread.start()
    print(f"[retention] sweeper started: every {s.RETENTION_SWEEP_MINUTES}min, "
          f"expire after {s.RETENTION_HOURS}h")
    return True
