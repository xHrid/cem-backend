"""
Job store: per-job isolated workspace on disk + thread-safe status in job.json.

Jobs now live INSIDE their project directory, scoped by script:

    <DATA_DIR>/projects/<project>/<script>/<job_id>/
        job.json                 metadata + task records
        input/audio/             symlinked WAV files from project spots
        input/audio_spots.json   {basename: spot}
        input/aggregate.csv      copied from project dataset/
        input/processed_files.txt
        input/geo.json           spot coordinates
        work/aggregate.csv       birdnet-produced aggregate
        work/processed_files.txt
        work/output.csv          birdnet filtered output
        results/<step>/...       per-step outputs + _run.log

A global index at <DATA_DIR>/jobs_index/{job_id}.json stores {project, script}
so polling endpoints can find a job by ID alone.
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
    def __init__(self, root: Path, job_id: str):
        self.id = job_id
        self.root = root

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
    def audio_spots_path(self) -> Path: return self.input_dir / "audio_spots.json"
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
        """Aggregate to feed analysis: birdnet output first, else uploaded."""
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

    # ---- audio spot map ----
    def get_audio_spots(self) -> dict:
        if self.audio_spots_path.is_file():
            return json.loads(self.audio_spots_path.read_text())
        return {}

    def set_audio_spots(self, mapping: dict) -> None:
        with _LOCK:
            self.input_dir.mkdir(parents=True, exist_ok=True)
            self.audio_spots_path.write_text(json.dumps(mapping, indent=2))

    # ---- spot geolocation ----
    def get_geo(self) -> list:
        if self.geo_path.is_file():
            try:
                data = json.loads(self.geo_path.read_text())
                return data if isinstance(data, list) else []
            except Exception:
                return []
        return []

    # ---- reference spots (legacy compat — always empty for CEM) ----
    def get_reference_spots(self) -> dict:
        """Reference audio mapping. CEM doesn't use pre-loaded reference files."""
        return {}

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


# ---------------------------------------------------------------------------
# Job index — maps job_id -> {project, script, root_path}
# Lives at DATA_DIR/jobs_index/ so polling endpoints find jobs by ID.
# ---------------------------------------------------------------------------

def _index_dir() -> Path:
    return get_settings().DATA_DIR / "jobs_index"


def _write_index(job_id: str, project: str, script: str, root: Path) -> None:
    d = _index_dir()
    d.mkdir(parents=True, exist_ok=True)
    entry = {"job_id": job_id, "project": project, "script": script, "root": str(root)}
    (d / f"{job_id}.json").write_text(json.dumps(entry))


def _read_index(job_id: str) -> Optional[dict]:
    p = _index_dir() / f"{job_id}.json"
    if p.is_file():
        return json.loads(p.read_text())
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_job(project_name: str, script: str, job_id: str | None = None) -> Job:
    """Create a new job workspace inside the project's script directory.

    If *job_id* is supplied (e.g. by the frontend via Airflow) it is used
    as-is; otherwise a random one is generated.
    """
    with _LOCK:
        s = get_settings()
        if job_id is None:
            job_id = new_id("job_")
        root = s.projects_dir / project_name / script / job_id
        job = Job(root, job_id)
        job.audio_dir.mkdir(parents=True, exist_ok=True)
        job.work_dir.mkdir(parents=True, exist_ok=True)
        job.results_dir.mkdir(parents=True, exist_ok=True)
        job._write({
            "job_id": job_id,
            "project": project_name,
            "script": script,
            "created_at": _now(),
            "tasks": [],
        })
        _write_index(job_id, project_name, script, root)
        return job


def get_job(job_id: str) -> Optional[Job]:
    """Look up a job by ID using the index."""
    idx = _read_index(job_id)
    if idx is None:
        return None
    root = Path(idx["root"])
    job = Job(root, job_id)
    return job if job.exists() else None


def list_jobs(project_name: Optional[str] = None) -> list[str]:
    """List job IDs. Optionally filter by project."""
    d = _index_dir()
    if not d.is_dir():
        return []
    ids = []
    for p in sorted(d.glob("*.json")):
        try:
            entry = json.loads(p.read_text())
            if project_name and entry.get("project") != project_name:
                continue
            ids.append(entry["job_id"])
        except Exception:
            continue
    return ids
