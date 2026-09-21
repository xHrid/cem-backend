#
# Snippet Extractor: Top-Confidence Audio Snippet Extraction
# ===========================================================
# Extracts 9-second audio clips (3-second focal detection + 3 seconds of
# preceding and succeeding context) for the highest-confidence detections
# per species (per spot and per project).

# Designed to be ultra-fast and lightweight: uses Python's standard `wave`
# module with direct frame seeking (falling back to `soundfile` if available).


import os
import re
import json
import wave
from pathlib import Path
import pandas as pd
from debug_log import debug

# Optional soundfile import for non-PCM formats
try:
    import soundfile as sf
    _HAS_SOUNDFILE = True
except ImportError:
    sf = None
    _HAS_SOUNDFILE = False


def _project_root_for_aggregate(aggregate_path: str) -> Path:
    candidate = Path(aggregate_path).resolve()
    for item in [candidate, *candidate.parents]:
        if (item / "project.json").is_file():
            return item
    return candidate.parent


def _sanitize_filename(name: str) -> str:
    """Sanitize species/spot string to safe filename characters."""
    return re.sub(r'[^A-Za-z0-9_-]', '_', str(name).strip())


def get_audio_info(filepath: str) -> tuple[int, float]:
    """Get sample rate and duration in seconds without reading audio samples into RAM."""
    if _HAS_SOUNDFILE:
        try:
            info = sf.info(filepath)
            return info.samplerate, info.duration
        except Exception:
            pass

    with wave.open(filepath, 'rb') as wf:
        framerate = wf.getframerate()
        nframes = wf.getnframes()
        duration = nframes / float(framerate) if framerate > 0 else 0.0
        return framerate, duration


def extract_clip(
    source_filepath: str,
    output_filepath: str,
    detection_start: float,
    detection_end: float,
    context_seconds: float = 3.0,
    target_duration: float = 9.0,
) -> tuple[float, float, float]:
    # Extract a sample-accurate audio snippet centered on a detection.
    
    # Returns (actual_start_time, actual_end_time, actual_duration_seconds).
    
    if not os.path.isfile(source_filepath):
        raise FileNotFoundError(f"Source audio file not found: {source_filepath}")

    sr, total_duration = get_audio_info(source_filepath)
    debug("snippet.source", source=source_filepath, sample_rate=sr, duration=total_duration)

    # Calculate desired window centered on detection
    desired_start = max(0.0, detection_start - context_seconds)
    desired_end = min(total_duration, detection_end + context_seconds)

    # If near boundary, try to expand the other side to keep ~target_duration
    current_span = desired_end - desired_start
    if current_span < target_duration and total_duration >= target_duration:
        if desired_start == 0.0:
            desired_end = min(total_duration, target_duration)
        elif desired_end == total_duration:
            desired_start = max(0.0, total_duration - target_duration)

    start_frame = int(round(desired_start * sr))
    stop_frame = int(round(desired_end * sr))
    num_to_read = max(0, stop_frame - start_frame)
    debug("snippet.window", start=desired_start, end=desired_end, frames=num_to_read)

    os.makedirs(os.path.dirname(output_filepath), exist_ok=True)

    # First try standard wave (instant, zero dependency, direct byte seek)
    try:
        with wave.open(source_filepath, 'rb') as wf:
            nchannels = wf.getnchannels()
            sampwidth = wf.getsampwidth()
            framerate = wf.getframerate()
            wf.setpos(min(start_frame, wf.getnframes()))
            raw_frames = wf.readframes(num_to_read)

        with wave.open(output_filepath, 'wb') as out_wf:
            out_wf.setnchannels(nchannels)
            out_wf.setsampwidth(sampwidth)
            out_wf.setframerate(framerate)
            out_wf.writeframes(raw_frames)

        actual_duration = len(raw_frames) / float(framerate * nchannels * sampwidth) if (framerate and nchannels and sampwidth) else 0.0
        return desired_start, desired_end, actual_duration
    except Exception as e:
        # Fallback to soundfile if wave module failed on non-standard WAV headers
        debug("snippet.decoder_fallback", error=type(e).__name__, soundfile=_HAS_SOUNDFILE)
        if _HAS_SOUNDFILE:
            data, read_sr = sf.read(source_filepath, start=start_frame, stop=stop_frame, dtype="float32")
            sf.write(output_filepath, data, read_sr)
            actual_duration = len(data) / read_sr if read_sr > 0 else 0.0
            return desired_start, desired_end, actual_duration
        raise e


def extract_species_snippets(
    aggregate_path: str,
    output_dir: str | None = None,
    context_seconds: float = 3.0,
    target_duration: float = 9.0,
) -> dict:
    """Read aggregate detections, identify top-confidence occurrences per species,
    and generate 9-second snippet clips + metadata index.
    """
    debug("snippets.start", aggregate=aggregate_path)
    if not os.path.isfile(aggregate_path):
        debug("snippets.skipped", reason="aggregate_missing", aggregate=aggregate_path)
        return {}

    try:
        df = pd.read_csv(aggregate_path)
    except Exception as e:
        print(f"[SnippetExtractor] Failed to read aggregate CSV: {e}")
        return {}

    if df.empty or "common_name" not in df.columns or "confidence" not in df.columns:
        debug("snippets.skipped", reason="empty_or_invalid_aggregate", rows=len(df))
        return {}

    project_root = _project_root_for_aggregate(aggregate_path)
    snippets_dir = Path(output_dir) if output_dir else (project_root / "snippets")
    snippets_dir.mkdir(parents=True, exist_ok=True)
    index_path = snippets_dir / "species_snippets.json"

    # Load existing metadata index if present
    existing_meta = {}
    if index_path.is_file():
        try:
            existing_meta = json.loads(index_path.read_text())
        except Exception:
            existing_meta = {}

    existing_species = existing_meta.get("species", {})

    # Filter out empty or unverified labels
    clean_df = df[df["common_name"].notna() & (df["common_name"].astype(str).str.strip() != "")].copy()
    if "iucn_category" in clean_df.columns:
        clean_df = clean_df[~clean_df["iucn_category"].isin(["Unknown", "", "None", "nan"])]
        if clean_df.empty:
            clean_df = df[df["common_name"].notna()].copy()

    # Find the row with highest confidence per (spot, common_name)
    group_cols = ["spot", "common_name"] if "spot" in clean_df.columns else ["common_name"]
    best_rows_idx = clean_df.groupby(group_cols)["confidence"].idxmax()
    best_df = clean_df.loc[best_rows_idx]
    debug("snippets.candidates", rows=len(df), eligible=len(clean_df), candidates=len(best_df))

    updated_species = dict(existing_species)
    extracted_count = 0

    for _, row in best_df.iterrows():
        common = str(row.get("common_name", "")).strip()
        scientific = str(row.get("scientific_name", "")).strip()
        spot = str(row.get("spot", "default")).strip()
        confidence = float(row.get("confidence", 0.0))
        det_start = float(row.get("start_time", 0.0))
        det_end = float(row.get("end_time", det_start + 3.0))
        source_fpath = str(row.get("filepath", "")).strip()
        source_fname = str(row.get("filename", os.path.basename(source_fpath))).strip()
        iucn = str(row.get("iucn_category", "Unknown")).strip()

        # If source file path is missing or relative, resolve against project root
        if not os.path.isfile(source_fpath):
            candidate = project_root / spot / "audio" / source_fname
            if candidate.is_file():
                source_fpath = str(candidate)
            else:
                candidates = list(project_root.rglob(source_fname))
                if candidates:
                    source_fpath = str(candidates[0])

        if not os.path.isfile(source_fpath):
            debug("snippet.skipped", species=common, spot=spot, source=source_fname, reason="source_missing")
            continue

        species_key = f"{spot}_{common}" if "spot" in clean_df.columns else common
        prev_entry = existing_species.get(species_key)

        # Only extract if new or has higher confidence than previously saved snippet
        if prev_entry and prev_entry.get("max_confidence", 0.0) >= confidence:
            snippet_file = snippets_dir / os.path.basename(prev_entry.get("snippet_rel_path", ""))
            if snippet_file.is_file():
                debug("snippet.skipped", species=common, spot=spot, reason="existing_equal_or_better")
                continue

        safe_spot = _sanitize_filename(spot)
        safe_common = _sanitize_filename(common)
        snippet_filename = f"{safe_spot}_{safe_common}.wav"
        snippet_output_path = snippets_dir / snippet_filename

        try:
            act_start, act_end, act_duration = extract_clip(
                source_filepath=source_fpath,
                output_filepath=str(snippet_output_path),
                detection_start=det_start,
                detection_end=det_end,
                context_seconds=context_seconds,
                target_duration=target_duration,
            )

            updated_species[species_key] = {
                "common_name": common,
                "scientific_name": scientific,
                "spot": spot,
                "iucn_category": iucn,
                "max_confidence": round(confidence, 4),
                "source_file": source_fname,
                "detection_window": {
                    "start": round(det_start, 2),
                    "end": round(det_end, 2),
                },
                "snippet_window": {
                    "start": round(act_start, 2),
                    "end": round(act_end, 2),
                    "duration": round(act_duration, 2),
                },
                "snippet_rel_path": f"snippets/{snippet_filename}",
            }
            extracted_count += 1
            debug("snippet.extracted", species=common, spot=spot, confidence=confidence,
                  duration=act_duration, filename=snippet_filename)
        except Exception as e:
            debug("snippet.failed", species=common, spot=spot, error=type(e).__name__)
            print(f"[SnippetExtractor] Warning: Could not extract snippet for {common} ({source_fname}): {e}")

    result_payload = {
        "project": project_root.name,
        "snippet_window_seconds": target_duration,
        "context_seconds_before": context_seconds,
        "context_seconds_after": context_seconds,
        "total_species_snippets": len(updated_species),
        "species": updated_species,
    }

    index_path.write_text(json.dumps(result_payload, indent=2))
    debug("snippets.finish", extracted=extracted_count, total=len(updated_species), index=index_path)
    if extracted_count > 0:
        print(f"[SnippetExtractor] Extracted {extracted_count} top-confidence 9s snippets to {snippets_dir}")
    return result_payload


def refresh_species_snippets(aggregate_path: str) -> dict:
    """Backfill old detections too; an optional clip failure must not fail BirdNET."""
    try:
        return extract_species_snippets(aggregate_path)
    except Exception as exc:
        debug("snippets.failed", aggregate=aggregate_path, error=type(exc).__name__)
        print(f"Warning: Failed to extract species audio snippets: {exc}")
        return {}
