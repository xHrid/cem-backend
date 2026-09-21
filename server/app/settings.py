"""
Server settings. All overridable via environment variables.

Env is read once, at first ``get_settings()`` call (i.e. app startup), inside
``Settings.__init__`` — not at import time — so the values are deterministic and
``get_settings`` is the single source of truth.
"""
import os
from pathlib import Path
from functools import lru_cache


def _bool(v: str | None, default: bool = False) -> bool:
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


class Settings:
    def __init__(self) -> None:
        self.API_VERSION: str = os.environ.get("API_VERSION", "1.1.0")
        # Shared DATA_DIR. Server compute writes project data here as private by
        # default; Make Public only flips project metadata after server compute
        # has produced publishable outputs. The master indexer reads public
        # projects from this same directory.
        self.DATA_DIR: Path = Path(os.environ.get("DATA_DIR", "/data")).resolve()
        self.LOG_DIR: Path = Path(os.environ.get("LOG_DIR", "/logs")).resolve()
        self.PIPELINE_DIR: Path = Path(os.environ.get("PIPELINE_DIR", "/app/pipeline")).resolve()
        self.PYTHON_BIN: str = os.environ.get("PYTHON_BIN", "python")
        self.MAX_UPLOAD_MB: int = int(os.environ.get("MAX_UPLOAD_MB", "2048"))
        # Cap on concurrent synchronous pipeline runs. Kept well under the AnyIO
        # threadpool (40) so sync endpoints and health never starve for a token.
        self.MAX_CONCURRENT_RUNS: int = int(os.environ.get("MAX_CONCURRENT_RUNS", "8"))
        self.STAC_ENABLED: bool = _bool(os.environ.get("STAC_ENABLED"), True)
        self.STAC_COLLECTION: str = os.environ.get("STAC_COLLECTION", "cem-bioacoustics")
        self.STAC_ASSET_BASE_URL: str = os.environ.get("STAC_ASSET_BASE_URL", "").rstrip("/")
        self.RETENTION_HOURS: float = float(os.environ.get("RETENTION_HOURS", "168"))
        self.RETENTION_SWEEP_MINUTES: float = float(os.environ.get("RETENTION_SWEEP_MINUTES", "60"))
        self.STACD_WORKSPACE_ID: str = os.environ.get("STACD_WORKSPACE_ID", "")
        self.STACD_STAC_VERSION: str = os.environ.get("STACD_STAC_VERSION", "1.1.0")
        self.STACD_ASSET_ID_PREFIX: str = os.environ.get("STACD_ASSET_ID_PREFIX", "cem/bioacoustics")
        self.ALLOWED_ORIGINS: list[str] = [
            o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "*").split(",") if o.strip()
        ]
        # ---- Airflow dispatch (optional) ----
        self.AIRFLOW_BASE_URL: str = os.environ.get("AIRFLOW_BASE_URL", "").rstrip("/")
        self.AIRFLOW_USERNAME: str = os.environ.get("AIRFLOW_USERNAME", "")
        self.AIRFLOW_PASSWORD: str = os.environ.get("AIRFLOW_PASSWORD", "")
        self.AIRFLOW_DAG_ID: str = os.environ.get("AIRFLOW_DAG_ID", "cem_pipeline")
        self.AIRFLOW_TIMEOUT: float = float(os.environ.get("AIRFLOW_TIMEOUT", "10"))
        # ---- FileBrowser (optional) ----
        self.FILEBROWSER_BASE_URL: str = os.environ.get("FILEBROWSER_BASE_URL", "").rstrip("/")
        self.FILEBROWSER_USERNAME: str = os.environ.get("FILEBROWSER_USERNAME", "")
        self.FILEBROWSER_PASSWORD: str = os.environ.get("FILEBROWSER_PASSWORD", "")
        self.FILEBROWSER_TIMEOUT: float = float(os.environ.get("FILEBROWSER_TIMEOUT", "10"))
        # ---- GEE (optional) ----
        self.GEE_PROJECT: str = os.environ.get("GEE_PROJECT", "ee-geeapi")
        self.GEE_SERVICE_ACCOUNT: str = os.environ.get("GEE_SERVICE_ACCOUNT", "")
        self.GEE_SERVICE_ACCOUNT_KEY: str = os.environ.get("GEE_SERVICE_ACCOUNT_KEY", "")
        self.GEE_DEFAULT_YEAR: int = int(os.environ.get("GEE_DEFAULT_YEAR", "2024"))
        self.GEE_DEFAULT_SCALE: int = int(os.environ.get("GEE_DEFAULT_SCALE", "10"))
        self.GEE_DEFAULT_NUM_PIXELS: int = int(os.environ.get("GEE_DEFAULT_NUM_PIXELS", "1000"))
        self.GEE_MAX_CLUSTERS: int = int(os.environ.get("GEE_MAX_CLUSTERS", "8"))

    @property
    def default_ebird(self) -> Path:
        return self.PIPELINE_DIR / "ebird_checklist.txt"

    @property
    def default_static_noise(self) -> Path:
        return self.PIPELINE_DIR / "static_noise.wav"

    @property
    def default_rain_noise(self) -> Path:
        return self.PIPELINE_DIR / "rain_noise.wav"

    @property
    def projects_dir(self) -> Path:
        return self.DATA_DIR / "projects"

    @property
    def public_projects_dir(self) -> Path:
        return self.DATA_DIR / "projects"

    @property
    def jobs_index_dir(self) -> Path:
        return self.DATA_DIR / "jobs_index"

    @property
    def run_locks_dir(self) -> Path:
        return self.DATA_DIR / "run_locks"

    @property
    def airflow_enabled(self) -> bool:
        return bool(self.AIRFLOW_BASE_URL)

    @property
    def filebrowser_enabled(self) -> bool:
        return bool(self.FILEBROWSER_BASE_URL)

@lru_cache
def get_settings() -> Settings:
    s = Settings()
    s.projects_dir.mkdir(parents=True, exist_ok=True)
    s.public_projects_dir.mkdir(parents=True, exist_ok=True)
    s.jobs_index_dir.mkdir(parents=True, exist_ok=True)
    return s
