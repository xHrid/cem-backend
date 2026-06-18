"""Pydantic request/response models -- per-step typed bodies.

Every algorithm endpoint receives ONLY the parameters it actually uses.
Common fields (project, spots, dates, spots_geo) are required on every
request so Airflow / the UI can never accidentally omit them.
"""
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field


# --------------------------------------------------------------------------- #
# Shared building-blocks
# --------------------------------------------------------------------------- #
class SpotGeo(BaseModel):
    """Single spot with required geolocation."""
    name: str = Field(..., description="Canonical spot name (e.g. 'SPOT1').")
    lat: float = Field(..., description="Latitude (WGS-84).")
    lon: float = Field(..., description="Longitude (WGS-84).")


class BaseRunParams(BaseModel):
    """Fields required by EVERY algorithm endpoint."""
    model_config = ConfigDict(extra="ignore")

    project: str = Field(
        ..., description="Project folder name on server."
    )
    spots: list[str] = Field(
        ..., min_length=1,
        description="Spot names to include in this run (at least one).",
    )
    start_date: str = Field(
        ..., description="Inclusive start date, YYYYMMDD or YYYY-MM-DD."
    )
    end_date: str = Field(
        ..., description="Inclusive end date, YYYYMMDD or YYYY-MM-DD."
    )
    spots_geo: Optional[list[SpotGeo]] = Field(
        default=None,
        description="Geolocation for spots -- drives STAC geometry/bbox. Optional.",
    )


# --------------------------------------------------------------------------- #
# Per-step models
# --------------------------------------------------------------------------- #
class BirdnetParams(BaseRunParams):
    """BirdNET species detection."""
    snr_db: float = Field(default=18.0, description="Denoise SNR (dB).")
    min_confidence: float = Field(
        default=0.25, ge=0, le=1,
        description="Minimum detection confidence (0-1).",
    )
    audio_spots: Optional[dict[str, str]] = Field(
        default=None,
        description="{filename: spot_name} mapping for uploaded audio.",
        json_schema_extra={"example": {"recording_20240101_120000.wav": "SPOT1"}},
    )


class HeatmapsParams(BaseRunParams):
    """Species activity heatmaps."""
    top_n_species: int = Field(default=25, ge=1, description="Top N species to plot.")
    filter_confidence: float = Field(default=0.3, ge=0, le=1, description="Min detection confidence.")
    filter_min_detections: int = Field(default=10, ge=1, description="Min total detections per species.")


class TemporalStickinessParams(BaseRunParams):
    """Activity regularity (temporal)."""
    top_n_temporal: int = Field(default=80, ge=1, description="Top N species to plot.")
    filter_confidence: float = Field(default=0.3, ge=0, le=1, description="Min detection confidence.")
    filter_min_detections: int = Field(default=10, ge=1, description="Min total detections per species.")


class SpatialStickinessParams(BaseRunParams):
    """Habitat affinity (spatial) -- requires >= 2 spots."""
    filter_confidence: float = Field(default=0.3, ge=0, le=1, description="Min detection confidence.")
    filter_min_detections: int = Field(default=10, ge=1, description="Min total detections per species.")


class MigratoryClassificationParams(BaseRunParams):
    """Migratory vs resident classification."""
    sci_threshold: float = Field(default=0.9, description="Seasonal Concentration Index threshold.")
    kurtosis_threshold: float = Field(default=15.0, description="Residual kurtosis threshold.")
    pmr_threshold: float = Field(default=50.0, description="Peak-to-median ratio threshold.")
    window_size: int = Field(default=60, ge=1, description="Rolling window size (days).")
    filter_confidence: float = Field(default=0.3, ge=0, le=1, description="Min detection confidence.")
    filter_min_detections: int = Field(default=10, ge=1, description="Min total detections per species.")


class SolarCorrelationParams(BaseRunParams):
    """Solar event correlation."""
    min_solar_days: int = Field(default=5, ge=1, description="Min days with >10 detections.")
    filter_confidence: float = Field(default=0.3, ge=0, le=1, description="Min detection confidence.")
    filter_min_detections: int = Field(default=10, ge=1, description="Min total detections per species.")


class DailyTimeseriesParams(BaseRunParams):
    """Daily call time series."""
    max_timeseries_species: int = Field(default=50, ge=1, description="Max species to plot.")
    filter_confidence: float = Field(default=0.3, ge=0, le=1, description="Min detection confidence.")
    filter_min_detections: int = Field(default=10, ge=1, description="Min total detections per species.")


class AcousticIndicesParams(BaseRunParams):
    """Acoustic indices computation + box plots (processes raw WAV files)."""
    snr_db: float = Field(default=18.0, description="Denoise SNR (dB).")


# Lookup for generic fallback route.
STEP_MODELS: dict[str, type[BaseRunParams]] = {
    "birdnet": BirdnetParams,
    "acoustic_indices": AcousticIndicesParams,
    "heatmaps": HeatmapsParams,
    "temporal_stickiness": TemporalStickinessParams,
    "spatial_stickiness": SpatialStickinessParams,
    "migratory_classification": MigratoryClassificationParams,
    "solar_correlation": SolarCorrelationParams,
    "daily_timeseries": DailyTimeseriesParams,
}


# --------------------------------------------------------------------------- #
# Response / utility models
# --------------------------------------------------------------------------- #
class TaskInfo(BaseModel):
    task_id: str
    step: str
    status: str
    created_at: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    returncode: Optional[int] = None
    error: Optional[str] = None
    params: dict = {}
    results: list[str] = []


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
    browse_url: Optional[str] = None
    api_version: Optional[str] = None


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
