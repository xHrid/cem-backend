"""
Project store: persistent project-level file storage on disk.

Layout (<DATA_DIR>/projects/<project_name>/):
    {SPOT_NAME}/audio/          uploaded WAV/MP3 files, organized by spot
    dataset/aggregate.csv       BirdNET aggregate (uploaded or produced)
    dataset/processed_files.txt processed-files list
    project.json                metadata (created_at, last_modified)
    {script}/{job_id}/          job workspaces (created at analysis time)

Audio is stored per-spot. Spot membership is implicit from directory structure.
"""
import json
import os
import re
import shutil
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .settings import get_settings

_LOCK = threading.RLock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Project:
    def __init__(self, name: str):
        self.name = name
        self.root = get_settings().projects_dir / name

    # ---- paths ----
    @property
    def meta_path(self) -> Path:
        return self.root / "project.json"

    @property
    def dataset_dir(self) -> Path:
        return self.root / "dataset"

    @property
    def aggregate_path(self) -> Path:
        return self.dataset_dir / "aggregate.csv"

    @property
    def processed_path(self) -> Path:
        return self.dataset_dir / "processed_files.txt"

    def spot_audio_dir(self, spot: str) -> Path:
        return self.root / spot / "audio"

    def exists(self) -> bool:
        return self.meta_path.is_file()

    # ---- metadata ----
    def _read_meta(self) -> dict:
        if self.meta_path.is_file():
            return json.loads(self.meta_path.read_text())
        return {}

    def _write_meta(self, meta: dict) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        tmp = self.meta_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(meta, indent=2))
        tmp.replace(self.meta_path)

    def _touch(self) -> None:
        with _LOCK:
            meta = self._read_meta()
            meta["last_modified"] = _now()
            self._write_meta(meta)

    _RESERVED = {"dataset", ".git", "__pycache__"}

    # ---- spots ----
    def list_spots(self) -> list[str]:
        if not self.root.is_dir():
            return []
        spots = []
        for d in sorted(self.root.iterdir()):
            if (d.is_dir()
                    and (d / "audio").is_dir()
                    and d.name not in self._RESERVED
                    and not d.name.startswith(".")):
                spots.append(d.name)
        return spots

    # ---- audio files ----
    def list_audio_files(self, spot: Optional[str] = None) -> list[str]:
        files = []
        if spot:
            d = self.spot_audio_dir(spot)
            if d.is_dir():
                files = sorted(p.name for p in d.iterdir() if p.is_file())
        else:
            for s in self.list_spots():
                d = self.spot_audio_dir(s)
                if d.is_dir():
                    files.extend(p.name for p in d.iterdir() if p.is_file())
            files.sort()
        return files

    def audio_count(self, spot: Optional[str] = None) -> int:
        return len(self.list_audio_files(spot))

    # ---- aggregate ----
    def has_aggregate(self) -> bool:
        return self.aggregate_path.is_file() and self.aggregate_path.stat().st_size > 0

    def aggregate_modified(self) -> Optional[str]:
        if not self.has_aggregate():
            return None
        ts = self.aggregate_path.stat().st_mtime
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

    # ---- processed ----
    def has_processed(self) -> bool:
        return self.processed_path.is_file() and self.processed_path.stat().st_size > 0

    def processed_modified(self) -> Optional[str]:
        if not self.has_processed():
            return None
        ts = self.processed_path.stat().st_mtime
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

    # ---- status ----
    def status(self) -> dict:
        spots_info = {}
        for s in self.list_spots():
            spots_info[s] = {
                "audio_count": self.audio_count(s),
                "audio_files": self.list_audio_files(s),
            }
        return {
            "project": self.name,
            "spots": spots_info,
            "total_audio": self.audio_count(),
            "has_aggregate": self.has_aggregate(),
            "has_processed": self.has_processed(),
            "aggregate_modified": self.aggregate_modified(),
            "processed_modified": self.processed_modified(),
        }

    # ---- populate a job from project files ----

    @staticmethod
    def _parse_date_from_filename(filename: str) -> Optional[str]:
        m = re.search(r'_(\d{8})_\d{6}', filename)
        return m.group(1) if m else None

    def populate_job(
        self,
        job,
        spots: list[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> dict:
        """Symlink/copy project audio into job input, filtered by spot + date."""
        linked = 0
        skipped = 0
        audio_spots = {}

        for spot in spots:
            src_dir = self.spot_audio_dir(spot)
            if not src_dir.is_dir():
                continue
            for src in src_dir.iterdir():
                if not src.is_file():
                    continue
                fname = src.name
                file_date = self._parse_date_from_filename(fname)
                if file_date:
                    if start_date and file_date < start_date:
                        skipped += 1
                        continue
                    if end_date and file_date > end_date:
                        skipped += 1
                        continue
                job.audio_dir.mkdir(parents=True, exist_ok=True)
                dest = job.audio_dir / fname
                if not dest.exists():
                    try:
                        os.symlink(src, dest)
                    except OSError:
                        try:
                            os.link(src, dest)
                        except OSError:
                            shutil.copy2(src, dest)
                audio_spots[fname] = spot
                linked += 1

        if audio_spots:
            job.set_audio_spots(audio_spots)
        if self.has_aggregate():
            job.input_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.aggregate_path, job.uploaded_aggregate)
        if self.has_processed():
            job.input_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.processed_path, job.uploaded_processed)

        return {"audio_linked": linked, "audio_skipped": skipped}

    # ---- update project files from job results ----
    def update_from_job(self, job) -> None:
        """Pull updated aggregate/processed back into project after job completes."""
        self.dataset_dir.mkdir(parents=True, exist_ok=True)
        if job.work_aggregate.is_file() and job.work_aggregate.stat().st_size > 0:
            shutil.copy2(job.work_aggregate, self.aggregate_path)
        if job.processed_file.is_file() and job.processed_file.stat().st_size > 0:
            shutil.copy2(job.processed_file, self.processed_path)
        self._touch()


# ---------------------------------------------------------------------------
# Module-level helpers (used by API routes)
# ---------------------------------------------------------------------------

def get_project(name: str) -> Optional["Project"]:
    """Return Project if it exists on disk, else None."""
    p = Project(name)
    return p if p.exists() else None


def get_or_create_project(name: str) -> "Project":
    """Return existing project or create a new one."""
    p = Project(name)
    if not p.exists():
        with _LOCK:
            p.root.mkdir(parents=True, exist_ok=True)
            p._write_meta({"created_at": _now(), "last_modified": _now()})
    return p
