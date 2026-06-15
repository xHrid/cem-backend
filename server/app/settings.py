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
    # --- auth ---
    API_KEY: str = os.environ.get("API_KEY", "changeme")
    REQUIRE_AUTH: bool = _bool(os.environ.get("REQUIRE_AUTH"), True)

    # --- versioning (item 12) ---
    # Compute API version. Stamped into /health, /steps, output sidecars and
    # every STAC item. Bump on any breaking change to the REST contract.
    API_VERSION: str = os.environ.get("API_VERSION", "1.1.0")

    # --- paths ---
    # Where uploaded inputs + generated results live (one sub-dir per job).
    # In the cluster this is a BIND MOUNT of the common host data directory
    # (data lives outside the container) — see docker-compose HOST_DATA_DIR.
    DATA_DIR: Path = Path(os.environ.get("DATA_DIR", "/data")).resolve()
    # Directory holding the analysis scripts (config.py, *.py, assets).
    PIPELINE_DIR: Path = Path(os.environ.get("PIPELINE_DIR", "/app/pipeline")).resolve()
    # Python used to launch pipeline scripts (subprocess).
    PYTHON_BIN: str = os.environ.get("PYTHON_BIN", "python")

    # --- limits ---
    MAX_UPLOAD_MB: int = int(os.environ.get("MAX_UPLOAD_MB", "2048"))
    MAX_CONCURRENT_TASKS: int = int(os.environ.get("MAX_CONCURRENT_TASKS", "2"))

    # --- STAC provenance (item 9) ---
    # Every produced result gets a STAC 1.0 Item JSON sidecar. Field shape that
    # depends on the drone/STAC-B team is kept configurable; defaults produce a
    # valid STAC 1.0 Item. Set STAC_ENABLED=false to skip generation.
    STAC_ENABLED: bool = _bool(os.environ.get("STAC_ENABLED"), True)
    STAC_COLLECTION: str = os.environ.get("STAC_COLLECTION", "cem-bioacoustics")
    # Prefix used to build absolute asset hrefs in STAC items. If a file-browser
    # base URL is set, asset hrefs point there; otherwise hrefs stay relative.
    STAC_ASSET_BASE_URL: str = os.environ.get("STAC_ASSET_BASE_URL", "").rstrip("/")

    # --- file browser (item 11) ---
    # Base URL of the shared-data file-browser service (another team's service).
    # After a job the API returns "<FILE_BROWSER_BASE_URL>/<job rel path>" so the
    # webapp can deep-link the user to that job's output directory. The path
    # template is configurable for whatever scheme the file-browser team uses.
    FILE_BROWSER_BASE_URL: str = os.environ.get("FILE_BROWSER_BASE_URL", "").rstrip("/")
    # {base} and {job_rel} are substituted. job_rel = "jobs/<job_id>".
    FILE_BROWSER_PATH_TEMPLATE: str = os.environ.get(
        "FILE_BROWSER_PATH_TEMPLATE", "{base}/{job_rel}"
    )

    # --- output retention (item 15) ---
    # The cluster is compute, not long-term storage: job dirs older than this are
    # swept. 0 disables cleanup. Default 7 days.
    RETENTION_HOURS: float = float(os.environ.get("RETENTION_HOURS", "168"))
    # How often the background sweeper runs (minutes). 0 disables the sweeper
    # (cleanup can still be run on demand via `python -m app.cli cleanup`).
    RETENTION_SWEEP_MINUTES: float = float(os.environ.get("RETENTION_SWEEP_MINUTES", "60"))

    # --- STACD / Airflow integration (synchronous algorithm API) ---
    # The front-end uploads audio directly (POST /api/v1/datasets/audio); the
    # server mints a fresh per-job workspace (data/<job_id>/) and returns the
    # job_id. Airflow then runs each algorithm against that job
    # (POST /api/v1/jobs/{job_id}/{algo}). Per-job dirs are swept by retention.
    #
    # Optional pin: if STACD_WORKSPACE_ID is set to a non-empty name, a job dir
    # by that exact name is EXEMPT from the retention sweeper (use it if you want
    # one long-lived "registered" dataset that accumulates across runs). Empty by
    # default — nothing is exempt.
    STACD_WORKSPACE_ID: str = os.environ.get("STACD_WORKSPACE_ID", "")
    # STAC version emitted in the synchronous /api/v1 responses (STACD/CoreStack
    # catalog uses 1.1.0).
    STACD_STAC_VERSION: str = os.environ.get("STACD_STAC_VERSION", "1.1.0")
    # Prefix for the deterministic asset_id STACD registers per output.
    STACD_ASSET_ID_PREFIX: str = os.environ.get("STACD_ASSET_ID_PREFIX", "cem/bioacoustics")

    # --- CORS ---
    # The webapp is a static site served from a different origin (e.g. Render),
    # so the browser sends a CORS preflight before every authenticated call.
    # Comma-separated list of allowed origins; "*" allows any. Because auth is a
    # custom X-API-Key header (not cookies) we never use credentials, so "*" is
    # safe here. Tighten to your Render URL in production if you prefer.
    ALLOWED_ORIGINS: list[str] = [
        o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "*").split(",") if o.strip()
    ]

    # --- baked pipeline assets (defaults used when caller does not upload its own) ---
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
    def jobs_dir(self) -> Path:
        return self.DATA_DIR / "jobs"

    def file_browser_url(self, job_id: str) -> str | None:
        """Browseable URL to a job's output directory, or None if unconfigured."""
        if not self.FILE_BROWSER_BASE_URL:
            return None
        return self.FILE_BROWSER_PATH_TEMPLATE.format(
            base=self.FILE_BROWSER_BASE_URL, job_rel=f"jobs/{job_id}"
        )


@lru_cache
def get_settings() -> Settings:
    s = Settings()
    s.jobs_dir.mkdir(parents=True, exist_ok=True)
    return s
