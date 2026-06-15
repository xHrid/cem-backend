"""
Output retention cleanup.

The cluster is compute, not long-term storage. Job directories under
<DATA_DIR>/jobs/ older than RETENTION_HOURS are deleted. Long-term copies live
in the shared data dir / STAC catalogue via the other teams' services.

Two entry points:
  * sweep_once()       — delete expired jobs now; returns the ids removed.
  * start_background() — launch a daemon thread that sweeps every
                         RETENTION_SWEEP_MINUTES. No-op when disabled.

"Age" is the job directory's most-recent mtime (any file touched resets it), so
a job that is still being written to or downloaded is not reaped mid-flight.
"""
import shutil
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from .settings import get_settings


def _newest_mtime(path: Path) -> float:
    """Most-recent mtime across a job dir (its own + any descendant)."""
    newest = path.stat().st_mtime
    for p in path.rglob("*"):
        try:
            newest = max(newest, p.stat().st_mtime)
        except OSError:
            continue
    return newest


def sweep_once(retention_hours: float | None = None) -> list[str]:
    """Delete expired job dirs. Returns the job ids removed."""
    s = get_settings()
    hours = s.RETENTION_HOURS if retention_hours is None else retention_hours
    if hours <= 0:
        return []
    cutoff = time.time() - hours * 3600
    jobs_dir = s.jobs_dir
    removed: list[str] = []
    if not jobs_dir.is_dir():
        return removed
    for job_dir in jobs_dir.iterdir():
        if not job_dir.is_dir():
            continue
        # Optionally exempt one pinned "registered" workspace (STACD_WORKSPACE_ID).
        # Empty by default -> nothing is exempt, every per-job dir is swept.
        if s.STACD_WORKSPACE_ID and job_dir.name == s.STACD_WORKSPACE_ID:
            continue
        try:
            if _newest_mtime(job_dir) < cutoff:
                shutil.rmtree(job_dir, ignore_errors=True)
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
        except Exception as e:  # never let the sweeper die
            print(f"[retention] sweep error: {e}")
        time.sleep(interval)


_thread: threading.Thread | None = None


def start_background() -> bool:
    """Start the daemon sweeper if retention + interval are enabled. Idempotent."""
    global _thread
    s = get_settings()
    if s.RETENTION_HOURS <= 0 or s.RETENTION_SWEEP_MINUTES <= 0:
        return False
    if _thread and _thread.is_alive():
        return True
    _thread = threading.Thread(target=_loop, name="retention-sweeper", daemon=True)
    _thread.start()
    print(f"[retention] sweeper started: every {s.RETENTION_SWEEP_MINUTES}min, "
          f"TTL {s.RETENTION_HOURS}h")
    return True
