"""
Job store: per-job isolated workspace on disk + thread-safe status in job.json.

Layout (<DATA_DIR>/jobs/<job_id>/):
    job.json                 metadata + task records
    input/audio/             uploaded WAV files  (birdnet --datasets)
    input/reference/         reference WAV files (birdnet --input-file-list)
    input/reference_spots.json   {basename: spot}
    input/aggregate.csv      uploaded aggregate (optional)
    input/ebird_checklist.txt, static_noise.wav, rain_noise.wav  (optional overrides)
    work/aggregate.csv       birdnet-produced aggregate
    work/processed_files.txt
    work/output.csv          birdnet filtered output
    results/<step>/...        per-step outputs + _run.log
"""
import json
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .settings import get_settings

_LOCK = threading.RLock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_id(prefix: str = "") -> str:
    return f"{prefix}{uuid.uuid4().hex[:12]}"


class Job:
    def __init__(self, job_id: str):
        self.id = job_id
        self.root = get_settings().jobs_dir / job_id

    # ---- paths ----
    @property
    def meta_path(self) -> Path: return self.root / "job.json"
    @property
    def input_dir(self) -> Path: return self.root / "input"
    @property
    def audio_dir(self) -> Path: return self.input_dir / "audio"
    @property
    def reference_dir(self) -> Path: return self.input_dir / "reference"
    @property
    def reference_spots_path(self) -> Path: return self.input_dir / "reference_spots.json"
    @property
    def geo_path(self) -> Path: return self.input_dir / "geo.json"
    @property
    def uploaded_aggregate(self) -> Path: return self.input_dir / "aggregate.csv"
    @property
    def uploaded_processed(self) -> Path: return self.input_dir / "processed_files.txt"
    @property
    def work_dir(self) -> Path: return self.root / "work"
    @property
    def work_aggregate(self) -> Path: return self.work_dir / "aggregate.csv"
    @property
    def processed_file(self) -> Path: return self.work_dir / "processed_files.txt"
    @property
    def output_csv(self) -> Path: return self.work_dir / "output.csv"
    @property
    def results_dir(self) -> Path: return self.root / "results"

    def step_results_dir(self, step: str) -> Path:
        return self.results_dir / step

    def exists(self) -> bool:
        return self.meta_path.is_file()

    # ---- asset resolution (uploaded override -> baked default) ----
    def ebird_file(self) -> Path:
        up = self.input_dir / "ebird_checklist.txt"
        return up if up.is_file() else get_settings().default_ebird

    def static_noise(self) -> Path:
        up = self.input_dir / "static_noise.wav"
        return up if up.is_file() else get_settings().default_static_noise

    def rain_noise(self) -> Path:
        up = self.input_dir / "rain_noise.wav"
        return up if up.is_file() else get_settings().default_rain_noise

    def resolve_aggregate(self) -> Optional[Path]:
        """Aggregate to feed analysis scripts: birdnet output first, else uploaded."""
        if self.work_aggregate.is_file() and self.work_aggregate.stat().st_size > 0:
            return self.work_aggregate
        if self.uploaded_aggregate.is_file() and self.uploaded_aggregate.stat().st_size > 0:
            return self.uploaded_aggregate
        return None

    def has_audio(self) -> bool:
        return self.audio_dir.is_dir() and any(self.audio_dir.iterdir())

    # ---- metadata ----
    def _read(self) -> dict:
        return json.loads(self.meta_path.read_text())

    def _write(self, meta: dict) -> None:
        tmp = self.meta_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(meta, indent=2))
        tmp.replace(self.meta_path)

    def read(self) -> dict:
        with _LOCK:
            return self._read()

    # ---- reference spot map ----
    def get_reference_spots(self) -> dict:
        if self.reference_spots_path.is_file():
            return json.loads(self.reference_spots_path.read_text())
        return {}

    def set_reference_spot(self, basename: str, spot: Optional[str]) -> None:
        with _LOCK:
            m = self.get_reference_spots()
            m[basename] = spot or ""
            self.reference_spots_path.write_text(json.dumps(m, indent=2))

    # ---- spot geolocation (for STAC items, item 9) ----
    def get_geo(self) -> list:
        """[{name, lat, lon}, ...] passed through on job submission, or []."""
        if self.geo_path.is_file():
            try:
                data = json.loads(self.geo_path.read_text())
                return data if isinstance(data, list) else []
            except Exception:
                return []
        return []

    def set_geo(self, geo: Optional[list]) -> None:
        if not geo:
            return
        with _LOCK:
            self.input_dir.mkdir(parents=True, exist_ok=True)
            self.geo_path.write_text(json.dumps(geo, indent=2))

    # ---- result listing ----
    def list_results(self) -> list[str]:
        out: list[str] = []
        if self.results_dir.is_dir():
            for p in sorted(self.results_dir.rglob("*")):
                if p.is_file():
                    out.append(str(p.relative_to(self.root)).replace("\\", "/"))
        # birdnet's primary output (and its STAC sidecars) also live in work/.
        # processed_files.txt is included so the webapp can pull the merged
        # processed list back and persist it locally (dedup parity).
        for p in (self.output_csv, self.work_aggregate, self.processed_file):
            if p.is_file():
                out.append(str(p.relative_to(self.root)).replace("\\", "/"))
        if self.work_dir.is_dir():
            for p in sorted(self.work_dir.glob("*.stac.json")):
                if p.is_file():
                    out.append(str(p.relative_to(self.root)).replace("\\", "/"))
        return out

    # ---- task records ----
    def add_task(self, step: str, params: dict) -> dict:
        with _LOCK:
            meta = self._read()
            task = {
                "task_id": new_id("t_"),
                "step": step,
                "status": "queued",
                "created_at": _now(),
                "started_at": None,
                "finished_at": None,
                "returncode": None,
                "error": None,
                "params": params,
                "results": [],
            }
            meta["tasks"].append(task)
            self._write(meta)
            return task

    def update_task(self, task_id: str, **fields) -> None:
        with _LOCK:
            meta = self._read()
            for t in meta["tasks"]:
                if t["task_id"] == task_id:
                    t.update(fields)
                    break
            self._write(meta)

    def get_task(self, task_id: str) -> Optional[dict]:
        for t in self.read().get("tasks", []):
            if t["task_id"] == task_id:
                return t
        return None


def create_job() -> Job:
    with _LOCK:
        job = Job(new_id("job_"))
        job.audio_dir.mkdir(parents=True, exist_ok=True)
        job.reference_dir.mkdir(parents=True, exist_ok=True)
        job.work_dir.mkdir(parents=True, exist_ok=True)
        job.results_dir.mkdir(parents=True, exist_ok=True)
        job._write({"job_id": job.id, "created_at": _now(), "tasks": []})
        return job


def get_job(job_id: str) -> Optional[Job]:
    job = Job(job_id)
    return job if job.exists() else None


def ensure_job(job_id: str) -> Job:
    """Get a job by a FIXED id, creating its workspace if absent.

    Used for the STACD/Airflow 'registered' workspace — a persistent job dir
    (not a random uuid) that audio is uploaded into and that accumulates the
    aggregate across DAG runs.
    """
    with _LOCK:
        job = Job(job_id)
        if not job.exists():
            job.audio_dir.mkdir(parents=True, exist_ok=True)
            job.reference_dir.mkdir(parents=True, exist_ok=True)
            job.work_dir.mkdir(parents=True, exist_ok=True)
            job.results_dir.mkdir(parents=True, exist_ok=True)
            job._write({"job_id": job.id, "created_at": _now(), "tasks": []})
        return job


def list_jobs() -> list[str]:
    d = get_settings().jobs_dir
    if not d.is_dir():
        return []
    return sorted(p.name for p in d.iterdir() if (p / "job.json").is_file())
