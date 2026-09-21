"""
Project store: persistent project-level file storage on disk.

Layout (<DATA_DIR>/projects/<project_name>/):
    {SPOT_NAME}/audio/          uploaded WAV/MP3 files, organized by spot
    dataset/aggregate.csv       BirdNET aggregate (uploaded or produced)
    dataset/processed_files.txt processed-files list
    project.json                metadata, including visibility/retention
    {script}/{job_id}/          job workspaces (created at analysis time)

    Audio is stored per-spot. Spot membership is implicit from directory structure.
    New projects are private. Make Public marks the whole project public only
    after a successful server-compute job has produced publishable data.
"""
import json
import os
import re
import shutil
import hashlib
import threading
import wave
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .safepath import ensure_within, safe_component
from .settings import get_settings

_LOCK = threading.RLock()
PRIVATE_RETENTION_HOURS = 168


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Project:
    def __init__(self, name: str):
        self.name = safe_component(name, "project")
        projects_dir = get_settings().projects_dir
        self.root = ensure_within(projects_dir, projects_dir / self.name)

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
    def audio_manifest_path(self) -> Path:
        return self.dataset_dir / "audio_manifest.json"

    @property
    def processed_path(self) -> Path:
        return self.dataset_dir / "processed_files.txt"

    @property
    def snippets_dir(self) -> Path:
        return self.root / "snippets"

    @property
    def snippets_index_path(self) -> Path:
        return self.snippets_dir / "species_snippets.json"

    def spot_audio_dir(self, spot: str) -> Path:
        return ensure_within(self.root, self.root / safe_component(spot, "spot") / "audio")

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

    def _data_visibility_map(
        self,
        *,
        visibility: str,
        retention_hours: float | int | None,
        updated_at: str,
    ) -> dict:
        is_public = visibility == "public"
        entries = {
            ".": {
                "visibility": visibility,
                "is_public": is_public,
                "retention_hours": retention_hours,
                "kind": "directory",
                "updated_at": updated_at,
            }
        }
        if not self.root.is_dir():
            return entries

        for path in sorted(self.root.rglob("*")):
            if path == self.meta_path.with_suffix(".json.tmp"):
                continue
            rel = path.relative_to(self.root).as_posix()
            entries[rel] = {
                "visibility": visibility,
                "is_public": is_public,
                "retention_hours": retention_hours,
                "kind": "directory" if path.is_dir() else "file",
                "updated_at": updated_at,
            }
        return entries

    def _apply_data_visibility(self, meta: dict, *, visibility: str, updated_at: str) -> None:
        retention_hours = None if visibility == "public" else PRIVATE_RETENTION_HOURS
        meta["data"] = self._data_visibility_map(
            visibility=visibility,
            retention_hours=retention_hours,
            updated_at=updated_at,
        )

    def _touch(self) -> None:
        with _LOCK:
            meta = self._read_meta()
            now = _now()
            visibility = "public" if (
                bool(meta.get("is_public"))
                or str(meta.get("visibility") or "").lower() == "public"
            ) else "private"
            meta["visibility"] = visibility
            meta["is_public"] = visibility == "public"
            meta["retention_hours"] = None if visibility == "public" else PRIVATE_RETENTION_HOURS
            meta["last_modified"] = now
            self._apply_data_visibility(meta, visibility=visibility, updated_at=now)
            self._write_meta(meta)

    @staticmethod
    def _default_meta() -> dict:
        now = _now()
        return {
            "created_at": now,
            "last_modified": now,
            "visibility": "private",
            "is_public": False,
            "retention_hours": PRIVATE_RETENTION_HOURS,
            "published_at": None,
            "data": {},
        }

    def is_public(self) -> bool:
        meta = self._read_meta()
        return bool(meta.get("is_public")) or str(meta.get("visibility") or "").lower() == "public"

    def mark_public(self, *, server_jobs: list[dict], repaired_aggregate_dates: bool) -> dict:
        with _LOCK:
            meta = self._read_meta()
            published_at = _now()
            meta.update({
                "visibility": "public",
                "is_public": True,
                "published_at": published_at,
                "last_modified": published_at,
                # null means infinite retention for public project data.
                "retention_hours": None,
                "publication": {
                    "published_at": published_at,
                    "server_compute_required": True,
                    "server_compute_verified": True,
                    "server_job_count": len(server_jobs),
                    "server_jobs": server_jobs,
                    "repaired_aggregate_dates": repaired_aggregate_dates,
                },
            })
            self._apply_data_visibility(meta, visibility="public", updated_at=published_at)
            self._write_meta(meta)
            return meta

    def mark_private(self) -> dict:
        with _LOCK:
            meta = self._read_meta()
            unpublished_at = _now()
            meta.update({
                "visibility": "private",
                "is_public": False,
                "unpublished_at": unpublished_at,
                "last_modified": unpublished_at,
                "retention_hours": PRIVATE_RETENTION_HOURS,
            })
            publication = dict(meta.get("publication") or {})
            publication.update({
                "status": "private",
                "unpublished_at": unpublished_at,
            })
            meta["publication"] = publication
            self._apply_data_visibility(meta, visibility="private", updated_at=unpublished_at)
            self._write_meta(meta)
            return meta

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

    def audio_id(self, spot: str, filename: str) -> str:
        payload = "\0".join([self.name, safe_component(spot, "spot"), Path(filename).name])
        return "aud_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]

    @staticmethod
    def _wav_metadata(path: Path) -> dict:
        if path.suffix.lower() != ".wav":
            return {}
        try:
            with wave.open(str(path), "rb") as audio:
                frames = audio.getnframes()
                rate = audio.getframerate()
                return {
                    "duration_seconds": round(frames / rate, 3) if rate else None,
                    "sample_rate": rate,
                }
        except (OSError, EOFError, wave.Error):
            return {}

    def audio_record(self, spot: str, filename: str) -> dict:
        spot = safe_component(spot, "spot")
        filename = Path(filename).name
        path = self.spot_audio_dir(spot) / filename
        rel = path.relative_to(get_settings().DATA_DIR).as_posix()
        record = {
            "audio_id": self.audio_id(spot, filename),
            "project": self.name,
            "spot": spot,
            "filename": filename,
            "relative_path": rel,
        }
        if path.is_file():
            stat = path.stat()
            record.update({
                "size_bytes": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            })
            record.update(self._wav_metadata(path))
        return record

    def list_audio_records(self, spot: Optional[str] = None) -> list[dict]:
        records = []
        spots = [safe_component(spot, "spot")] if spot else self.list_spots()
        for s in spots:
            for filename in self.list_audio_files(s):
                records.append(self.audio_record(s, filename))
        return sorted(records, key=lambda r: (r["spot"], r["filename"]))

    def write_audio_manifest(self) -> list[dict]:
        records = self.list_audio_records()
        self.dataset_dir.mkdir(parents=True, exist_ok=True)
        tmp = self.audio_manifest_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({"items": records}, indent=2))
        tmp.replace(self.audio_manifest_path)
        return records

    def in_range_audio(
        self,
        spots: list[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> set[str]:
        """Basenames of audio in the given spots that fall inside the date range.
        Files without a parseable date are always included (same as populate_job)."""
        names: set[str] = set()
        for spot in spots:
            d = self.spot_audio_dir(spot)
            if not d.is_dir():
                continue
            for p in d.iterdir():
                if not p.is_file():
                    continue
                fd = self._parse_date_from_filename(p.name)
                if fd:
                    if start_date and fd < start_date:
                        continue
                    if end_date and fd > end_date:
                        continue
                names.add(p.name)
        return names

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

    def processed_set(self) -> set[str]:
        """Basenames of audio BirdNET has already processed."""
        if not self.processed_path.is_file():
            return set()
        out: set[str] = set()
        for ln in self.processed_path.read_text().splitlines():
            ln = ln.strip()
            if ln:
                out.add(Path(ln).name)
        return out

    # ---- status ----
    def status(self) -> dict:
        meta = self._read_meta()
        spots_info = {}
        for s in self.list_spots():
            spots_info[s] = {
                "audio_count": self.audio_count(s),
                "audio_files": self.list_audio_files(s),
                "audio_records": self.list_audio_records(s),
            }
        return {
            "project": self.name,
            "spots": spots_info,
            "total_audio": self.audio_count(),
            "has_aggregate": self.has_aggregate(),
            "has_processed": self.has_processed(),
            "has_audio_manifest": self.audio_manifest_path.is_file(),
            "aggregate_modified": self.aggregate_modified(),
            "processed_modified": self.processed_modified(),
            "visibility": meta.get("visibility") or "private",
            "is_public": self.is_public(),
            "retention_hours": meta.get("retention_hours"),
            "published_at": meta.get("published_at"),
            "unpublished_at": meta.get("unpublished_at"),
            "data_entry_count": len(meta.get("data") or {}),
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
        if self.has_aggregate() and self.has_processed():
            job.input_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.processed_path, job.uploaded_processed)

        return {"audio_linked": linked, "audio_skipped": skipped}

    # ---- update project files from job results ----
    def update_from_job(self, job) -> None:
        """Pull updated aggregate/processed back into project after job completes."""
        self.dataset_dir.mkdir(parents=True, exist_ok=True)
        if job.work_aggregate.is_file() and job.work_aggregate.stat().st_size > 0:
            shutil.copy2(job.work_aggregate, self.aggregate_path)
        if self.has_aggregate() and job.processed_file.is_file() and job.processed_file.stat().st_size > 0:
            shutil.copy2(job.processed_file, self.processed_path)
        self.write_audio_manifest()
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
            p._write_meta(Project._default_meta())
    return p
