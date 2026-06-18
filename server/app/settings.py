"""
Server settings. All overridable via environment variables.
"""
import os
from pathlib import Path
from functools import lru_cache


def _bool(v: str | None, default: bool = False) -> bool:
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "on")


class Settings:
    API_KEY: str = os.environ.get("API_KEY", "changeme")
    REQUIRE_AUTH: bool = _bool(os.environ.get("REQUIRE_AUTH"), True)
    API_VERSION: str = os.environ.get("API_VERSION", "1.1.0")
    DATA_DIR: Path = Path(os.environ.get("DATA_DIR", "/data")).resolve()
    PIPELINE_DIR: Path = Path(os.environ.get("PIPELINE_DIR", "/app/pipeline")).resolve()
    PYTHON_BIN: str = os.environ.get("PYTHON_BIN", "python")
    MAX_UPLOAD_MB: int = int(os.environ.get("MAX_UPLOAD_MB", "2048"))
    MAX_CONCURRENT_TASKS: int = int(os.environ.get("MAX_CONCURRENT_TASKS", "2"))
    STAC_ENABLED: bool = _bool(os.environ.get("STAC_ENABLED"), True)
    STAC_COLLECTION: str = os.environ.get("STAC_COLLECTION", "cem-bioacoustics")
    STAC_ASSET_BASE_URL: str = os.environ.get("STAC_ASSET_BASE_URL", "").rstrip("/")
    FILE_BROWSER_BASE_URL: str = os.environ.get("FILE_BROWSER_BASE_URL", "").rstrip("/")
    FILE_BROWSER_PATH_TEMPLATE: str = os.environ.get(
        "FILE_BROWSER_PATH_TEMPLATE", "{base}/{job_rel}"
    )
    RETENTION_HOURS: float = float(os.environ.get("RETENTION_HOURS", "168"))
    RETENTION_SWEEP_MINUTES: float = float(os.environ.get("RETENTION_SWEEP_MINUTES", "60"))
    STACD_WORKSPACE_ID: str = os.environ.get("STACD_WORKSPACE_ID", "")
    STACD_STAC_VERSION: str = os.environ.get("STACD_STAC_VERSION", "1.1.0")
    STACD_ASSET_ID_PREFIX: str = os.environ.get("STACD_ASSET_ID_PREFIX", "cem/bioacoustics")
    ALLOWED_ORIGINS: list[str] = [
        o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "*").split(",") if o.strip()
    ]

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
    def jobs_index_dir(self) -> Path:
        return self.DATA_DIR / "jobs_index"

    def file_browser_url(self, job_id: str) -> str | None:
        if not self.FILE_BROWSER_BASE_URL:
            return None
        return self.FILE_BROWSER_PATH_TEMPLATE.format(
            base=self.FILE_BROWSER_BASE_URL, job_rel=f"jobs/{job_id}"
        )


@lru_cache
def get_settings() -> Settings:
    s = Settings()
    s.projects_dir.mkdir(parents=True, exist_ok=True)
    s.jobs_index_dir.mkdir(parents=True, exist_ok=True)
    return s
