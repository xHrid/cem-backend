"""
Pipeline step metadata. Maps API step ids -> script files, and loads the
human-readable manifest shipped alongside the scripts.

birdnet   -> raw WAV in, aggregate CSV + filtered output CSV out
the rest  -> aggregate CSV in, PNG/CSV results out
"""
import json
from functools import lru_cache

from .settings import get_settings

BIRDNET = "birdnet"
ACOUSTIC_INDICES = "acoustic_indices"

# Fallback algorithm version used when a manifest entry has no "version" field.
DEFAULT_ALGO_VERSION = "1.0.0"

# step id -> script filename in PIPELINE_DIR
ANALYSIS_SCRIPTS: dict[str, str] = {
    "heatmaps": "activity_heatmaps.py",
    "temporal_stickiness": "temporal_stickiness.py",
    "spatial_stickiness": "spatial_stickiness.py",
    "migratory_classification": "migratory_classification.py",
    "solar_correlation": "solar_correlation.py",
    "daily_timeseries": "daily_call_timeseries.py",
}

# Scripts that process raw WAV files (like birdnet)
WAV_SCRIPTS: dict[str, str] = {
    BIRDNET: "birdnet_predictions.py",
    ACOUSTIC_INDICES: "acoustic_indices.py",
}

SCRIPTS: dict[str, str] = {**WAV_SCRIPTS, **ANALYSIS_SCRIPTS}

# Order used by run/all (birdnet first; spatial needs >=2 spots so it may skip).
RUN_ORDER: list[str] = [
    BIRDNET,
    ACOUSTIC_INDICES,
    "heatmaps",
    "temporal_stickiness",
    "spatial_stickiness",
    "migratory_classification",
    "solar_correlation",
    "daily_timeseries",
]


def is_valid_step(step: str) -> bool:
    return step in SCRIPTS


def is_analysis(step: str) -> bool:
    return step in ANALYSIS_SCRIPTS


def load_manifest() -> dict[str, dict]:
    """Return {step_id: manifest_entry}. Falls back to script map if file absent.

    Not cached: the manifest is bind-mounted code (item 8), so editing a script's
    version takes effect on the next call without an image rebuild or restart.
    """
    path = get_settings().PIPELINE_DIR / "manifest.json"
    try:
        entries = json.loads(path.read_text())
        return {e["id"]: e for e in entries}
    except Exception:
        return {
            sid: {"id": sid, "name": sid, "script_file": fn}
            for sid, fn in SCRIPTS.items()
        }


def step_version(step: str) -> str:
    """Algorithm/script version for a step, from the manifest (item 12)."""
    return load_manifest().get(step, {}).get("version", DEFAULT_ALGO_VERSION)
