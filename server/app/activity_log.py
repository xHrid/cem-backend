"""Activity log: one JSONL line per user action, split day-wise.

    <LOG_DIR>/activity-YYYY-MM-DD.jsonl

Each line records who did what (user identity from request headers) plus enough
context to find the run's own logs under data/jobs/<job_id>/. The `service` field
keeps the format shared, so other backends (e.g. drone) can write the same shape.
"""
import json
import shutil
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .settings import get_settings

_LOCK = threading.Lock()
_SERVICE = "cem-backend"


def append(user: dict, action: str, **fields) -> None:
    user = user or {}
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "service": _SERVICE,
        "user_email": user.get("email"),
        "user_id": user.get("id"),
        "action": action,
    }
    entry.update({k: v for k, v in fields.items() if v is not None})

    log_dir = get_settings().LOG_DIR
    log_dir.mkdir(parents=True, exist_ok=True)
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = log_dir / f"activity-{day}.jsonl"
    with _LOCK:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def copy_run_log(src: Path, job_id: str, step: str) -> Optional[str]:
    """Duplicate a run's own log into the logging directory so the logs dir is
    self-contained (activity ledger + the actual run output). Returns the path
    relative to LOG_DIR, or None if there was nothing to copy."""
    if not src.is_file():
        return None
    log_dir = get_settings().LOG_DIR
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dest_dir = log_dir / "runs" / day
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{job_id}_{step}.log"
    try:
        shutil.copyfile(src, dest)
    except OSError:
        return None
    return str(dest.relative_to(log_dir))
