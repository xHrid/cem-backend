"""
CEM BirdNET pipeline server — FastAPI app (synchronous, STACD/Airflow model).

Flow:
  1. Front-end uploads audio directly:  POST /api/v1/datasets/audio  -> job_id
     (WAVs saved under data/<job_id>/input/audio/).
  2. Front-end hands job_id to Airflow. Airflow runs each DAG node by calling
     POST /api/v1/jobs/{algo} with job_id in the request body, and BLOCKS on
     the response (synchronous).
  3. Results are returned inline (STAC items) and can be fetched via the
     read-only /api/v1/jobs/{job_id}/... endpoints.

All algorithm + job routes live in stacd_api.router under /api/v1. This module
only wires the app, CORS, the retention sweeper, and the unauthenticated health
check.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import pipeline_meta as meta
from . import retention
from . import stacd_api
from .settings import get_settings

app = FastAPI(
    title="CEM BirdNET Pipeline API",
    version=get_settings().API_VERSION,
    description="Upload audio, run BirdNET + ecological analyses synchronously "
                "(STACD/Airflow), retrieve STAC-described results.",
)


@app.on_event("startup")
def _on_startup() -> None:
    # Background output retention sweeper. No-op when disabled.
    retention.start_background()


# The only API surface: synchronous algorithm + job routes under /api/v1.
app.include_router(stacd_api.router)

# Allow the browser-based static site (different origin) to call this API.
# Auth is a custom X-API-Key header, not cookies, so credentials stay off and a
# wildcard origin is safe.
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],
)


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
