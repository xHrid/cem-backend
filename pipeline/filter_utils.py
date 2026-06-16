"""
Shared 3-step detection filter used by all analysis scripts.

Steps:
  1. Taxonomic verification (eBird checklist)
  2. Confidence thresholding (>= 0.3)
  3. Minimum activity (>= 10 total detections per species)

Also: preprocess Spot + Date from filename, apply date range + spot filter.
"""

import os
import pandas as pd
from datetime import date


def filter_detections(
    aggregate_path: str,
    ebird_file: str | None = None,
    date_start: date | None = None,
    date_end: date | None = None,
    spot_names: list[str] | None = None,
    filter_confidence: float = 0.3,
    filter_min_detections: int = 10,
) -> pd.DataFrame:
    """Load aggregate CSV -> preprocess -> 3-step filter -> date/spot filter.

    Parameters
    ----------
    aggregate_path       : path to aggregate CSV
    ebird_file           : path to eBird checklist (one line per species: "sci_name_CommonName")
    date_start           : inclusive start date (None = no lower bound)
    date_end             : inclusive end date (None = no upper bound)
    spot_names           : list of spot names to keep (empty/None = all)
    filter_confidence    : minimum detection confidence for step 2 (default 0.3)
    filter_min_detections: minimum total detections per species for step 3 (default 10)

    Returns
    -------
    Filtered DataFrame with Spot, Date, Date_Only columns added.
    """
    df = pd.read_csv(aggregate_path)
    print(f"Loaded aggregate: {len(df)} rows")

    # -- Preprocessing: use the unified metadata columns (spot, date) written by
    #    birdnet via file_metadata. Decouples analysis from raw filenames and
    #    honours the attached spot of reference imports. Falls back to parsing
    #    the filename only if those columns are absent.
    if "spot" in df.columns and "date" in df.columns:
        df["Spot"] = df["spot"].astype(str).str.strip().str.lower()
        df["Date"] = pd.to_datetime(df["date"], errors="coerce")
    else:
        from file_metadata import build_record
        recs = df["filename"].apply(lambda fn: build_record(str(fn)))
        df["Spot"] = recs.apply(lambda r: (r["spot"] or "")).str.lower()
        df["Date"] = pd.to_datetime(recs.apply(lambda r: r["date"]), errors="coerce")

    df = df[~df["Spot"].isin(["", "nan", "none"])]
    df.dropna(subset=["Spot", "Date"], inplace=True)
    df["Date_Only"] = df["Date"].dt.date

    print(f"After preprocessing: {len(df)} detections")
    print(f"Spots found: {sorted(df['Spot'].unique())}")

    # -- Step 1: Taxonomic verification --
    if ebird_file and os.path.exists(ebird_file):
        with open(ebird_file, "r") as f:
            valid_birds = [line.strip().split("_")[1] for line in f if "_" in line.strip()]
        before = df["common_name"].nunique()
        df = df[df["common_name"].isin(valid_birds)].copy()
        print(f"Step 1 (eBird): {before} -> {df['common_name'].nunique()} species")
    else:
        print("Step 1 (eBird): skipped (no checklist file)")

    # -- Step 2: Confidence threshold --
    before = len(df)
    df = df[df["confidence"] >= filter_confidence].copy()
    print(f"Step 2 (confidence >= {filter_confidence}): {before} -> {len(df)} detections")

    # -- Step 3: Minimum detections per species --
    counts = df.groupby("common_name").size()
    valid = counts[counts >= filter_min_detections].index
    before_sp = df["common_name"].nunique()
    df = df[df["common_name"].isin(valid)].copy()
    print(f"Step 3 (min {filter_min_detections}): {before_sp} -> {df['common_name'].nunique()} species")

    # -- Date range filter --
    if date_start is not None:
        df = df[df["Date_Only"] >= date_start]
    if date_end is not None:
        df = df[df["Date_Only"] <= date_end]

    # -- Spot filter --
    if spot_names:
        df = df[df["Spot"].isin([s.lower() for s in spot_names])]

    print(f"Final: {len(df)} detections, {df['common_name'].nunique()} species")
    return df
