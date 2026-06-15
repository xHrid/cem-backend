"""Pydantic request/response models."""
from typing import Optional
from pydantic import BaseModel, Field


class RunParams(BaseModel):
    """Optional run-time overrides forwarded to config.apply_overrides()."""
    spots: Optional[list[str]] = Field(
        default=None, description="Spot names to keep (empty/None = all)."
    )
    start_date: Optional[str] = Field(
        default=None, description="Inclusive start date, YYYY-MM-DD or YYYYMMDD."
    )
    end_date: Optional[str] = Field(
        default=None, description="Inclusive end date, YYYY-MM-DD or YYYYMMDD."
    )
    snr_db: Optional[float] = Field(
        default=None, description="BirdNET denoise SNR (dB). birdnet step only."
    )
    # --- Per-step algorithm tunables (each applied by exactly one step) ---
    min_confidence: Optional[float] = Field(
        default=None, description="BirdNET minimum detection confidence, 0-1. birdnet step only."
    )
    top_n_species: Optional[int] = Field(
        default=None, description="Top N species to plot. heatmaps step only."
    )
    top_n_temporal: Optional[int] = Field(
        default=None, description="Top N species to plot. temporal_stickiness step only."
    )
    sci_threshold: Optional[float] = Field(
        default=None, description="Seasonal Concentration Index threshold. migratory_classification only."
    )
    kurtosis_threshold: Optional[float] = Field(
        default=None, description="Residual kurtosis threshold. migratory_classification only."
    )
    pmr_threshold: Optional[float] = Field(
        default=None, description="Peak-to-median ratio threshold. migratory_classification only."
    )
    window_size: Optional[int] = Field(
        default=None, description="Rolling window size in days. migratory_classification only."
    )
    min_solar_days: Optional[int] = Field(
        default=None, description="Min days with >10 detections to include a species. solar_correlation only."
    )
    max_timeseries_species: Optional[int] = Field(
        default=None, description="Max species to plot. daily_timeseries step only."
    )
    spots_geo: Optional[list[dict]] = Field(
        default=None,
        description="Spot geolocation for STAC items: [{name, lat, lon}, ...]. "
                    "Stored on the job and used as geometry/bbox; not a CLI flag.",
    )


class TaskInfo(BaseModel):
    task_id: str
    step: str
    status: str  # queued | running | success | failed
    created_at: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    returncode: Optional[int] = None
    error: Optional[str] = None
    params: dict = {}
    results: list[str] = []  # result files relative to job root


class UploadedFile(BaseModel):
    filename: str
    kind: str
    size_bytes: int
    rel_path: str


class UploadResponse(BaseModel):
    job_id: str
    uploaded: list[UploadedFile]


class JobSummary(BaseModel):
    job_id: str
    created_at: str
    inputs: dict
    has_aggregate: bool
    tasks: list[TaskInfo]
    results: list[str]
    browse_url: Optional[str] = None  # file-browser deep link (item 11)
    api_version: Optional[str] = None  # compute API version (item 12)


class CreateJobResponse(BaseModel):
    job_id: str
    created_at: str


class RunResponse(BaseModel):
    job_id: str
    task_id: str
    step: str
    status: str


class RunAllResponse(BaseModel):
    job_id: str
    tasks: list[RunResponse]
