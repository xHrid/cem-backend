"""
Central configuration for the BirdNET pipeline.

CONSTANTS  — paths, thresholds, coordinates. Edit once, used everywhere.
INPUTS     — spot_names, date_range, input_file_list. Change per run.

The watcher overrides the INPUTS/PATHS at launch via apply_overrides();
standalone runs just use the defaults below.
"""

import os
from datetime import date, datetime

# =============================================================================
# INPUTS — change these per run
# =============================================================================
SPOT_NAMES: list[str] = []          # empty = all spots; or ["crimespot3", "crimespot5"]
DATE_START: date = date(2025, 11, 1)
DATE_END:   date = date(2025, 12, 31)
INPUT_FILE_LIST: list[str] = []     # explicit WAV paths to always include (birdnet only)
INPUT_FILE_SPOTS: list[str] = []    # spot name per INPUT_FILE_LIST entry, aligned 1:1 ("" = derive from filename)
DATASET_SPOTS: list[str] = []       # spot name per INPUT_DIRECTORIES entry, aligned 1:1 ("" = derive from filename)

# =============================================================================
# PATHS — set once for your environment
# =============================================================================
# Audio input directories (birdnet scans these recursively)
INPUT_DIRECTORIES: list[str] = [
    r"path/to/audio_dir_1",
    r"path/to/audio_dir_2",
]

# BirdNET pipeline files
AGGREGATE_FILE:     str = r"path/to/birdnet_aggregate.csv"
PROCESSED_FILE:     str = r"path/to/processed_files.txt"
OUTPUT_CSV:         str = r"path/to/birdnet_output.csv"

# Acoustic indices pipeline files
AGGREGATE_FILE_INDICES: str = r"path/to/indices_aggregate.csv"
PROCESSED_FILE_INDICES: str = r"path/to/indices_processed_files.txt"

# Denoising reference clips
STATIC_NOISE_PATH:  str = r"path/to/static_noise.wav"
RAIN_NOISE_PATH:    str = r"path/to/rain_noise.wav"

# eBird species checklist (taxonomic filter)
EBIRD_FILE:         str = r"path/to/ebird_checklist.txt"

# Analysis output directories
OUTPUT_DIR_02_HEATMAPS:     str = r"path/to/output/02_heatmaps"
OUTPUT_DIR_03_TEMPORAL:     str = r"path/to/output/03_temporal_stickiness"
OUTPUT_DIR_04_SPATIAL:      str = r"path/to/output/04_spatial_stickiness"
OUTPUT_DIR_07_MIGRATORY:    str = r"path/to/output/07_migratory"
OUTPUT_DIR_08_SOLAR:        str = r"path/to/output/08_solar"
OUTPUT_DIR_05_INDICES:      str = r"path/to/output/05_acoustic_indices"
OUTPUT_DIR_09_TIMESERIES:   str = r"path/to/output/09_timeseries"

# =============================================================================
# CONSTANTS — rarely change
# =============================================================================
# Audio
TARGET_SR:          int   = 48000
SNR_DB:             float = 18

# Location (for BirdNET species filtering + solar calculations)
LATITUDE:           float = 28.5635
LONGITUDE:          float = 77.1897
TIMEZONE_STR:       str   = "Asia/Kolkata"
LOCATION_NAME:      str   = "Sanjay Van"

# BirdNET
MIN_CONFIDENCE:     float = 0.25

# Shared filter thresholds (filter_utils 3-step filter, all analysis scripts)
FILTER_CONFIDENCE:     float = 0.3    # step 2: min detection confidence
FILTER_MIN_DETECTIONS: int   = 10     # step 3: min total detections per species

# Analysis thresholds
TOP_N_SPECIES:      int   = 25     # 02_heatmaps
TOP_N_TEMPORAL:     int   = 80     # 03_temporal
SCI_THRESHOLD:      float = 0.9    # 07_migratory
KURTOSIS_THRESHOLD: float = 15.0   # 07_migratory
PMR_THRESHOLD:      float = 50.0   # 07_migratory
WINDOW_SIZE:        int   = 60     # 07_migratory (days)
EPSILON:            float = 1e-6   # 07_migratory (avoid div/0)
MIN_SOLAR_DAYS:     int   = 5      # 08_solar (min days with >10 detections)
MAX_TIMESERIES_SP:  int   = 50     # 09_timeseries
SPECIES_TO_PLOT:    list[str] | None = None  # 09_timeseries (None = top N)


# =============================================================================
# RUNTIME OVERRIDES
# =============================================================================
# The watcher launches scripts with per-job values on the command line. Those
# override the INPUT/PATH defaults above; everything else stays constant.
# Standalone runs (no CLI args) just use the defaults — nothing changes.
#
# Recognised flags (all optional):
#   --datasets DIR [DIR ...]     -> INPUT_DIRECTORIES   (dirs scanned recursively)
#   --input-file-list F [F ...]  -> INPUT_FILE_LIST     (explicit reference files)
#   --aggregate-file PATH        -> AGGREGATE_FILE
#   --processed-file PATH        -> PROCESSED_FILE
#   --output-csv PATH            -> OUTPUT_CSV
#   --output-dir DIR             -> all OUTPUT_DIR_* (+ default OUTPUT_CSV)
#   --ebird-file PATH            -> EBIRD_FILE
#   --noise-path PATH            -> STATIC_NOISE_PATH
#   --rain-path PATH             -> RAIN_NOISE_PATH
#   --dataset-spots A B C         -> DATASET_SPOTS  (aligned 1:1 with --datasets)
#   --spots A,B,C                -> SPOT_NAMES
#   --start-date YYYYMMDD        -> DATE_START
#   --end-date YYYYMMDD          -> DATE_END
#   --snr-db FLOAT               -> SNR_DB                (birdnet)
#
# Per-step algorithm tunables (all optional; each read by exactly one step):
#   --min-confidence FLOAT       -> MIN_CONFIDENCE        (birdnet)
#   --filter-confidence FLOAT    -> FILTER_CONFIDENCE     (all analysis scripts)
#   --filter-min-detections INT  -> FILTER_MIN_DETECTIONS (all analysis scripts)
#   --top-n-species INT          -> TOP_N_SPECIES         (heatmaps)
#   --top-n-temporal INT         -> TOP_N_TEMPORAL        (temporal_stickiness)
#   --sci-threshold FLOAT        -> SCI_THRESHOLD         (migratory)
#   --kurtosis-threshold FLOAT   -> KURTOSIS_THRESHOLD    (migratory)
#   --pmr-threshold FLOAT        -> PMR_THRESHOLD         (migratory)
#   --window-size INT            -> WINDOW_SIZE           (migratory)
#   --min-solar-days INT         -> MIN_SOLAR_DAYS        (solar)
#   --max-timeseries-species INT -> MAX_TIMESERIES_SP     (daily_timeseries)
def apply_overrides(argv=None) -> None:
    import argparse

    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--datasets", nargs="*", default=None)
    p.add_argument("--input-file-list", nargs="*", default=None)
    p.add_argument("--input-file-spots", nargs="*", default=None)
    p.add_argument("--dataset-spots", nargs="*", default=None)
    p.add_argument("--aggregate-file", default=None)
    p.add_argument("--aggregate-file-indices", default=None)
    p.add_argument("--processed-file", default=None)
    p.add_argument("--processed-file-indices", default=None)
    p.add_argument("--output-csv", default=None)
    p.add_argument("--output-dir", default=None)
    p.add_argument("--ebird-file", default=None)
    p.add_argument("--noise-path", default=None)
    p.add_argument("--rain-path", default=None)
    p.add_argument("--spots", default=None)
    p.add_argument("--start-date", default=None)
    p.add_argument("--end-date", default=None)
    p.add_argument("--snr-db", default=None)
    # Per-step algorithm tunables (each consumed by exactly one script).
    p.add_argument("--min-confidence", default=None)
    p.add_argument("--filter-confidence", default=None)
    p.add_argument("--filter-min-detections", default=None)
    p.add_argument("--top-n-species", default=None)
    p.add_argument("--top-n-temporal", default=None)
    p.add_argument("--sci-threshold", default=None)
    p.add_argument("--kurtosis-threshold", default=None)
    p.add_argument("--pmr-threshold", default=None)
    p.add_argument("--window-size", default=None)
    p.add_argument("--min-solar-days", default=None)
    p.add_argument("--max-timeseries-species", default=None)
    args, _unknown = p.parse_known_args(argv)  # ignore watcher extras (--root-dir, ...)

    g = globals()
    if args.datasets:         g["INPUT_DIRECTORIES"] = list(args.datasets)
    if args.input_file_list:  g["INPUT_FILE_LIST"] = list(args.input_file_list)
    if args.input_file_spots: g["INPUT_FILE_SPOTS"] = ["" if s == "_" else s for s in args.input_file_spots]
    if args.dataset_spots:    g["DATASET_SPOTS"] = ["" if s == "_" else s for s in args.dataset_spots]
    if args.aggregate_file:   g["AGGREGATE_FILE"] = args.aggregate_file
    if args.aggregate_file_indices: g["AGGREGATE_FILE_INDICES"] = args.aggregate_file_indices
    if args.processed_file:   g["PROCESSED_FILE"] = args.processed_file
    if args.processed_file_indices: g["PROCESSED_FILE_INDICES"] = args.processed_file_indices
    if args.ebird_file:       g["EBIRD_FILE"] = args.ebird_file
    if args.noise_path:       g["STATIC_NOISE_PATH"] = args.noise_path
    if args.rain_path:        g["RAIN_NOISE_PATH"] = args.rain_path
    if args.spots:            g["SPOT_NAMES"] = [s for s in args.spots.split(",") if s]
    if args.start_date:       g["DATE_START"] = datetime.strptime(args.start_date, "%Y%m%d").date()
    if args.end_date:         g["DATE_END"] = datetime.strptime(args.end_date, "%Y%m%d").date()
    if args.snr_db:           g["SNR_DB"] = float(args.snr_db)

    # Per-step algorithm tunables. Use `is not None` (0 / 0.0 are valid) and
    # cast ints via int(float(x)) so "60" and "60.0" both parse.
    if args.min_confidence is not None:        g["MIN_CONFIDENCE"] = float(args.min_confidence)
    if args.filter_confidence is not None:     g["FILTER_CONFIDENCE"] = float(args.filter_confidence)
    if args.filter_min_detections is not None: g["FILTER_MIN_DETECTIONS"] = int(float(args.filter_min_detections))
    if args.top_n_species is not None:         g["TOP_N_SPECIES"] = int(float(args.top_n_species))
    if args.top_n_temporal is not None:        g["TOP_N_TEMPORAL"] = int(float(args.top_n_temporal))
    if args.sci_threshold is not None:         g["SCI_THRESHOLD"] = float(args.sci_threshold)
    if args.kurtosis_threshold is not None:    g["KURTOSIS_THRESHOLD"] = float(args.kurtosis_threshold)
    if args.pmr_threshold is not None:         g["PMR_THRESHOLD"] = float(args.pmr_threshold)
    if args.window_size is not None:           g["WINDOW_SIZE"] = int(float(args.window_size))
    if args.min_solar_days is not None:        g["MIN_SOLAR_DAYS"] = int(float(args.min_solar_days))
    if args.max_timeseries_species is not None: g["MAX_TIMESERIES_SP"] = int(float(args.max_timeseries_species))

    if args.output_dir:
        for k in list(g.keys()):
            if k.startswith("OUTPUT_DIR_"):
                g[k] = args.output_dir
        g["OUTPUT_CSV"] = args.output_csv or os.path.join(args.output_dir, "birdnet_output.csv")
    elif args.output_csv:
        g["OUTPUT_CSV"] = args.output_csv
