"""
CEM BirdNET pipeline server — FastAPI app (synchronous, STACD/Airflow model).

Flow:
  1. Front-end uploads audio per-project:  POST /api/v1/projects/upload/audio
     (WAVs stored under <DATA_DIR>/projects/<project>/<spot>/audio/).
  2. Front-end runs a step:  POST /api/v1/scripts  with the script name + a
     client-minted job_id in the body. This call is SYNCHRONOUS: the server runs
     the pipeline to completion and the HTTP response carries the result
     (STAC item(s) on success, a structured error code otherwise).
  3. Read-only /api/v1/jobs/{job_id}/... endpoints expose job status, results,
     logs and downloads.

All routes live in stacd_api.router under /api/v1. This module wires the app,
the retention sweeper, and the unauthenticated health check.
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import pipeline_meta as meta
from . import retention
from . import stacd_api
from .settings import get_settings


@asynccontextmanager
async def _lifespan(app: FastAPI):
    # Background output retention sweeper. No-op when disabled.
    retention.start_background()
    yield


app = FastAPI(
    title="CEM BirdNET Pipeline API",
    version=get_settings().API_VERSION,
    description="Upload audio, run BirdNET + ecological analyses synchronously "
                "(STACD/Airflow), retrieve STAC-described results.",
    lifespan=_lifespan,
)

# The only API surface: synchronous algorithm + job routes under /api/v1.
app.include_router(stacd_api.router)



# --------------------------------------------------------------------------- #
# health (no auth)
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    return {
        "status": "ok",
        "api_version": get_settings().API_VERSION,
        "steps": list(meta.SCRIPTS.keys()),
    }
