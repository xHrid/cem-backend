"""
00b: BirdNET Predictions Pipeline
===================================
Three-part pipeline:
  1. File listing  — discover, filter, deduplicate WAV files
  2. Main pipeline — run BirdNET on new files, append to aggregate CSV
  3. Output CSV    — filtered subset of aggregate for requested range
"""

import os
import re
import json
import hashlib
from pathlib import Path
import numpy as np
import pandas as pd
import soundfile as sf
import librosa
from datetime import date
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import io
import types
import contextlib
import multiprocessing

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"

import warnings
warnings.filterwarnings("ignore", message=".*tf.lite.Interpreter is deprecated.*")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="tensorflow")

from birdnetlib.main import RecordingBuffer
from birdnetlib.analyzer import Analyzer

import config as cfg
from file_metadata import parse_filename, build_record  # unified, source-agnostic
from snippet_extractor import refresh_species_snippets
from debug_log import debug


# =============================================================================
# PART 1 — FILE LISTING
# =============================================================================
def list_files(
    input_directories: list[str],
    date_start: date,
    date_end: date,
    processed_files: set[str],
    input_file_list: list[str] | None = None,
) -> list[str]:
    discovered: dict[str, str] = {}

    for directory in input_directories:
        directory = os.path.abspath(directory)
        if not os.path.isdir(directory):
            print(f"WARNING: input directory not found: {directory}")
            continue
        for root, _dirs, files in os.walk(directory):
            for fname in files:
                parsed = parse_filename(fname)
                if parsed is None:
                    continue
                if not (date_start <= parsed["date"] <= date_end):
                    continue
                if fname not in discovered:
                    discovered[fname] = os.path.join(root, fname)

    if input_file_list:
        for fpath in input_file_list:
            fpath = os.path.abspath(fpath)
            fname = os.path.basename(fpath)
            if fname not in discovered and os.path.isfile(fpath):
                discovered[fname] = fpath

    for pf in processed_files:
        discovered.pop(pf, None)

    result = sorted(discovered.values())
    print(f"File listing: {len(result)} to process ({len(processed_files)} already processed)")
    return result


# =============================================================================
# PART 2 — MAIN PIPELINE
# =============================================================================
_DENOISE_NFFT, _DENOISE_HOP = 2048, 512


def _noise_threshold(noise_clip: np.ndarray) -> np.ndarray:
    """Per-frequency gate threshold (mean |STFT| * 1.2) for one noise clip.

    Length-invariant (wrap-padding just repeats the clip), so it is computed
    ONCE per worker from the resampled clip instead of re-running an STFT over a
    length-matched copy of the noise for every file.
    """
    ns = librosa.stft(noise_clip, n_fft=_DENOISE_NFFT, hop_length=_DENOISE_HOP)
    return (np.mean(np.abs(ns), axis=1, keepdims=True) * 1.2).astype(np.float32)


def _denoise_combined(audio: np.ndarray, noise_clips, noise_thresholds,
                      sr: int | None = None, snr_db: float | None = None) -> np.ndarray:
    """Remove every noise source in a SINGLE STFT + ISTFT pass.

    Replaces the previous two sequential `_denoise` calls (static then rain),
    each of which ran a full STFT+ISTFT plus a per-file noise STFT. Here the
    noise refs are subtracted in the time domain, one STFT/ISTFT is taken, and
    the magnitude is gated against the max of the precomputed thresholds.
    """
    snr_db = cfg.SNR_DB if snr_db is None else snr_db

    cleaned = audio.astype(np.float32, copy=True)
    for noise_ref in noise_clips:
        if len(noise_ref) > len(cleaned):
            nr = noise_ref[:len(cleaned)]
        else:
            nr = np.pad(noise_ref, (0, len(cleaned) - len(noise_ref)), "wrap")
        audio_power = np.mean(cleaned ** 2)
        noise_power = np.mean(nr ** 2)
        if noise_power == 0:
            continue
        desired_noise_power = audio_power / (10 ** (snr_db / 10))
        nr_scaled = nr * np.sqrt(desired_noise_power / noise_power)
        cleaned = cleaned - nr_scaled

    stft = librosa.stft(cleaned, n_fft=_DENOISE_NFFT, hop_length=_DENOISE_HOP)
    magnitude, phase = np.abs(stft), np.angle(stft)

    combined_threshold = np.zeros((magnitude.shape[0], 1), dtype=np.float32)
    for th in noise_thresholds:
        combined_threshold = np.maximum(combined_threshold, th)

    gated_mag = np.where(magnitude > combined_threshold, magnitude, 0)
    return librosa.istft(gated_mag * np.exp(1j * phase), hop_length=_DENOISE_HOP)


def _fast_predict(self, sample, sensitivity=1.0):
    """Drop-in for birdnetlib Analyzer.predict that skips the redundant
    resize_tensor_input + allocate_tensors on every chunk.

    Every 3 s chunk has the identical shape (1, 144000), so re-allocating the
    interpreter's tensors per chunk is pure overhead. We allocate only when the
    batch shape actually changes. The invoke + flat_sigmoid are byte-for-byte
    the same as upstream, so detections are unchanged.
    """
    data = np.asarray([sample], dtype=np.float32)
    if getattr(self, "_cem_alloc_shape", None) != data.shape:
        self.interpreter.resize_tensor_input(self.input_layer_index, list(data.shape))
        self.interpreter.allocate_tensors()
        self._cem_alloc_shape = data.shape
    self.interpreter.set_tensor(self.input_layer_index, data)
    self.interpreter.invoke()
    prediction = self.interpreter.get_tensor(self.output_layer_index)
    return self.flat_sigmoid(np.array(prediction), sensitivity=-sensitivity)


def _analyze_file(filepath, analyzer, noise_clips, noise_thresholds):
    audio_raw, orig_sr = sf.read(filepath, dtype="float32")
    if audio_raw.ndim > 1:
        audio_raw = audio_raw.mean(axis=1)
    if orig_sr != cfg.TARGET_SR:
        # BirdNET requires 48 kHz input (3 s chunk = 144000 samples), so this
        # stays at TARGET_SR — do NOT lower it here.
        audio_raw = librosa.resample(y=audio_raw, orig_sr=orig_sr, target_sr=cfg.TARGET_SR)

    audio_clean = _denoise_combined(audio_raw, noise_clips, noise_thresholds)

    recording = RecordingBuffer(
        analyzer, audio_clean, cfg.TARGET_SR,
        lat=cfg.LATITUDE, lon=cfg.LONGITUDE, min_conf=cfg.MIN_CONFIDENCE,
    )
    with contextlib.redirect_stdout(io.StringIO()):
        recording.analyze()
    return pd.DataFrame(recording.detections)


_worker_analyzer = None
_worker_noise = None
_worker_rain = None
_worker_noise_thresholds = None


def _init_worker(noise_path, rain_path, tflite_threads):
    global _worker_analyzer, _worker_noise, _worker_rain, _worker_noise_thresholds

    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        _worker_analyzer = Analyzer()

    if tflite_threads > 1:
        try:
            interp = _worker_analyzer.interpreter
            with contextlib.redirect_stderr(io.StringIO()):
                new_interp = type(interp)(model_path=_worker_analyzer.model_path, num_threads=tflite_threads)
            new_interp.allocate_tensors()
            _worker_analyzer.interpreter = new_interp
            _worker_analyzer.input_details = new_interp.get_input_details()
            _worker_analyzer.output_details = new_interp.get_output_details()
            _worker_analyzer.input_layer_index = _worker_analyzer.input_details[0]["index"]
            _worker_analyzer.output_layer_index = _worker_analyzer.output_details[0]["index"]
        except Exception:
            pass

    # Skip resize_tensor_input+allocate_tensors on every 3s chunk (constant
    # shape) — bind the fast predict as an instance method on this worker's
    # analyzer only.
    _worker_analyzer.predict = types.MethodType(_fast_predict, _worker_analyzer)

    _worker_noise, _ = sf.read(noise_path, dtype="float32")
    _worker_rain, _ = sf.read(rain_path, dtype="float32")
    if _worker_noise.ndim > 1:
        _worker_noise = _worker_noise.mean(axis=1)
    if _worker_rain.ndim > 1:
        _worker_rain = _worker_rain.mean(axis=1)

    noise_sr = sf.info(noise_path).samplerate
    rain_sr = sf.info(rain_path).samplerate
    if noise_sr != cfg.TARGET_SR:
        _worker_noise = librosa.resample(y=_worker_noise, orig_sr=noise_sr, target_sr=cfg.TARGET_SR)
    if rain_sr != cfg.TARGET_SR:
        _worker_rain = librosa.resample(y=_worker_rain, orig_sr=rain_sr, target_sr=cfg.TARGET_SR)

    # Precompute both gate thresholds once per worker instead of per file.
    _worker_noise_thresholds = [_noise_threshold(_worker_noise), _noise_threshold(_worker_rain)]


def _process_single_file(item):
    # item = (filepath, spot_override). spot_override is the spot a reference file
    # is attached to (passed from the UI); "" / None means derive spot from the
    # filename. hour always comes from the filename (name_YYYYMMDD_HHMMSS).
    filepath, spot_override = item
    # Unified metadata: filename parse + attached-spot override in one place.
    rec = build_record(filepath, spot=spot_override)
    if rec.get("date") is None and cfg.DATE_START is not None:
        rec["date"] = cfg.DATE_START.isoformat()
        rec["hour"] = 0
        rec["minute"] = 0
        rec["second"] = 0
    filename = rec["filename"]
    try:
        df = _analyze_file(
            filepath, _worker_analyzer,
            [_worker_noise, _worker_rain], _worker_noise_thresholds,
        )
        if not df.empty:
            df["filename"] = rec["filename"]
            df["filepath"] = rec["filepath"]
            df["spot"]     = rec["spot"]
            df["date"]     = rec["date"]   # ISO YYYY-MM-DD
            df["hour"]     = rec["hour"]
            if "common_name" in df.columns and "label" not in df.columns:
                df["label"] = df["common_name"]
            return filename, df
        return filename, None
    except Exception as e:
        print(f"\n  ERROR processing {filename}: {e}")
        return filename, None


def load_processed_files(path: str) -> set[str]:
    if not os.path.isfile(path):
        return set()
    with open(path, "r") as f:
        return {line.strip() for line in f if line.strip()}


def save_processed_files(path: str, filenames: set[str]):
    with open(path, "w") as f:
        for fname in sorted(filenames):
            f.write(fname + "\n")


def _project_root_for_aggregate(aggregate_path: str) -> Path:
    candidate = Path(aggregate_path).resolve()
    for item in [candidate, *candidate.parents]:
        if (item / "project.json").is_file():
            return item
    return candidate.parent


def _local_iucn_lookup_path() -> Path:
    return Path(__file__).resolve().with_name("birdlife_iucn_lookup.csv")


def _load_local_iucn_lookup() -> dict[str, str]:
    lookup_path = _local_iucn_lookup_path()
    if not lookup_path.is_file():
        return {}

    try:
        df = pd.read_csv(lookup_path)
    except Exception:
        return {}

    if df.empty:
        return {}

    required = {"scientific_name", "iucn_category"}
    if not required.issubset(df.columns):
        return {}

    mapping: dict[str, str] = {}
    for _, row in df.iterrows():
        sci = str(row.get("scientific_name", "")).strip()
        category = str(row.get("iucn_category", "")).strip()
        if not sci or not category:
            continue
        mapping[sci.lower()] = category
    return mapping


def _load_project_iucn_cache(aggregate_path: str) -> dict[str, str]:
    cache_path = _project_root_for_aggregate(aggregate_path) / "species_iucn_cache.json"
    if not cache_path.is_file():
        return {}
    try:
        payload = json.loads(cache_path.read_text())
    except Exception:
        return {}
    if isinstance(payload, dict):
        return {str(k).lower(): str(v).strip() for k, v in payload.items() if str(v).strip()}
    return {}


def _write_project_iucn_cache(aggregate_path: str, cache: dict[str, str]) -> None:
    project_root = _project_root_for_aggregate(aggregate_path)
    project_root.mkdir(parents=True, exist_ok=True)
    cache_path = project_root / "species_iucn_cache.json"
    cache_path.write_text(json.dumps(cache, indent=2, sort_keys=True))


def make_audio_id(project: str, spot: str, filename: str) -> str:
    payload = "\0".join([str(project).strip(), str(spot).strip(), os.path.basename(str(filename))])
    return "aud_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def attach_audio_ids(df: pd.DataFrame, aggregate_path: str) -> pd.DataFrame:
    if df.empty or "filename" not in df.columns or "spot" not in df.columns:
        return df
    result = df.copy()
    project = _project_root_for_aggregate(aggregate_path).name
    generated = result.apply(
        lambda row: make_audio_id(project, row.get("spot", ""), row.get("filename", "")),
        axis=1,
    )
    if "audio_id" not in result.columns:
        result["audio_id"] = generated
    else:
        missing = result["audio_id"].isna() | (result["audio_id"].astype(str).str.strip() == "")
        result.loc[missing, "audio_id"] = generated[missing]
    return result


def ensure_aggregate_audio_ids(
    aggregate_path: str,
    required_columns: list[str] | None = None,
) -> list[str] | None:
    if not os.path.isfile(aggregate_path) or os.path.getsize(aggregate_path) == 0:
        return None
    try:
        existing = pd.read_csv(aggregate_path)
    except Exception:
        return None
    if existing.empty or "filename" not in existing.columns or "spot" not in existing.columns:
        columns = list(existing.columns)
        for col in required_columns or []:
            if col not in columns:
                columns.append(col)
        if columns != list(existing.columns):
            existing.reindex(columns=columns).to_csv(aggregate_path, index=False)
            print(f"Backfilled aggregate metadata columns: {aggregate_path}")
        return columns

    changed = False
    needs_audio_id = "audio_id" not in existing.columns
    if not needs_audio_id:
        needs_audio_id = bool(
            (existing["audio_id"].isna() | (existing["audio_id"].astype(str).str.strip() == "")).any()
        )
    if needs_audio_id:
        existing = attach_audio_ids(existing, aggregate_path)
        changed = True

    columns = list(existing.columns)
    for col in required_columns or []:
        if col not in columns:
            columns.append(col)
            changed = True

    if changed:
        existing = existing.reindex(columns=columns)
        existing.to_csv(aggregate_path, index=False)
        print(f"Backfilled aggregate metadata columns: {aggregate_path}")
    return list(existing.columns)


def enrich_iucn_category(df: pd.DataFrame, aggregate_path: str) -> pd.DataFrame:
    if df.empty:
        return df

    lookup_key = "scientific_name" if "scientific_name" in df.columns else "common_name"
    if lookup_key not in df.columns:
        return df

    global_lookup = _load_local_iucn_lookup()
    project_cache = _load_project_iucn_cache(aggregate_path)
    result = df.copy()
    out_map: dict[str, str] = {}

    for species_name in sorted({str(value).strip() for value in result[lookup_key].dropna().astype(str) if str(value).strip()}):
        norm = species_name.lower()
        if norm in project_cache:
            out_map[species_name] = project_cache[norm]
            continue
        if norm in global_lookup:
            out_map[species_name] = global_lookup[norm]
            project_cache[norm] = global_lookup[norm]
            continue
        out_map[species_name] = "Unknown"

    _write_project_iucn_cache(aggregate_path, project_cache)
    result["iucn_category"] = result[lookup_key].map(lambda value: out_map.get(str(value).strip(), "Unknown"))
    return result


def run_pipeline(file_list, aggregate_path, processed_files_path, spot_overrides=None):
    debug("birdnet.start", files=len(file_list), aggregate=aggregate_path)
    spot_overrides = spot_overrides or {}   # {basename: spot_name}
    if not file_list:
        print("No new files to process.")
        refresh_species_snippets(aggregate_path)
        return pd.DataFrame()

    total_cpus = multiprocessing.cpu_count()
    # Each worker process loads its OWN BirdNET/TensorFlow interpreter (several
    # hundred MB of RAM). Spawning too many at once on a memory-limited host
    # (e.g. Docker Desktop's default ~2 GB) spikes memory and the kernel SIGKILLs
    # a worker mid-run -> "BrokenProcessPool". Default conservatively to 2 and
    # allow an override via BIRDNET_MAX_WORKERS (set to 1 for the tightest RAM).
    env_workers = os.environ.get("BIRDNET_MAX_WORKERS", "").strip()
    if env_workers.isdigit() and int(env_workers) > 0:
        n_workers = min(int(env_workers), max(1, total_cpus))
    else:
        n_workers = max(1, min(total_cpus // 2, 2))
    threads_per = max(1, total_cpus // n_workers)
    debug("birdnet.workers", workers=n_workers, threads_per_worker=threads_per)
    print(f"Parallelism: {n_workers} workers × {threads_per} TFLite threads ({total_cpus} CPUs)")

    all_detections = []
    processed_this_run = set()
    already_processed = load_processed_files(processed_files_path)

    with ProcessPoolExecutor(
        max_workers=n_workers,
        initializer=_init_worker,
        initargs=(cfg.STATIC_NOISE_PATH, cfg.RAIN_NOISE_PATH, threads_per),
    ) as executor:
        items = [(fp, spot_overrides.get(os.path.basename(fp))) for fp in file_list]
        futures = {executor.submit(_process_single_file, it): it[0] for it in items}
        with tqdm(total=len(file_list), desc="BirdNET") as pbar:
            for future in as_completed(futures):
                filename, result = future.result()
                debug("birdnet.file_finished", filename=filename, detections=0 if result is None else len(result))
                processed_this_run.add(filename)
                if result is not None:
                    all_detections.append(result)
                pbar.update(1)

    new_df = pd.DataFrame()
    if all_detections:
        new_df = pd.concat(all_detections, ignore_index=True)
        new_df = attach_audio_ids(new_df, aggregate_path)
        new_df = enrich_iucn_category(new_df, aggregate_path)
        existing_columns = ensure_aggregate_audio_ids(aggregate_path, list(new_df.columns))
        if existing_columns:
            columns = list(existing_columns)
            columns.extend([col for col in new_df.columns if col not in columns])
            new_df = new_df.reindex(columns=columns)
        header = not os.path.isfile(aggregate_path)
        new_df.to_csv(aggregate_path, mode="a", header=header, index=False)
        print(f"Appended {len(new_df)} detections to {aggregate_path}")

    else:
        print("No detections in this batch.")

    refresh_species_snippets(aggregate_path)
    debug("birdnet.finish", new_detections=len(new_df), processed=len(processed_this_run))
    already_processed.update(processed_this_run)
    save_processed_files(processed_files_path, already_processed)
    print(f"Marked {len(processed_this_run)} files as processed (total: {len(already_processed)})")
    return new_df


# =============================================================================
# PART 3 — OUTPUT CSV
# =============================================================================
def write_output_csv(aggregate_path, output_path, input_directories, date_start, date_end,
                     reference_basenames=None):
    reference_basenames = set(reference_basenames or ())
    if not os.path.isfile(aggregate_path):
        print("No aggregate file found.")
        return

    df = pd.read_csv(aggregate_path)
    if df.empty:
        print("Aggregate file is empty.")
        return

    if "filepath" in df.columns:
        # Vectorized prefix match instead of a per-row .apply(os.path.abspath):
        # normalize the column once, then test against precomputed prefixes.
        abs_dirs = [os.path.abspath(d) + os.sep for d in input_directories]
        fp_norm = df["filepath"].astype(str).str.replace("/", os.sep, regex=False)
        in_dirs = pd.Series(False, index=df.index)
        for prefix in abs_dirs:
            in_dirs |= fp_norm.str.startswith(prefix)
        in_dirs &= df["filepath"].notna()
        # Reference files live OUTSIDE input_directories — keep them too so their
        # detections (with hour + spot) appear in the output CSV.
        in_refs = df["filename"].isin(reference_basenames) if "filename" in df.columns else False
        df = df[in_dirs | in_refs]

    # Date filter on the unified `date` column (name-agnostic); fall back to
    # parsing the filename only if the column is missing.
    if "date" in df.columns:
        # Compare datetime64 vs pandas Timestamps (never mix datetime64 with
        # python date objects — that raises InvalidComparison). End is inclusive
        # of the whole day via [start, end+1day).
        dts = pd.to_datetime(df["date"], errors="coerce")
        start_ts = pd.Timestamp(date_start)
        end_ts = pd.Timestamp(date_end) + pd.Timedelta(days=1)
        df = df[dts.notna() & (dts >= start_ts) & (dts < end_ts)]
    elif "filename" in df.columns:
        df = df[df["filename"].apply(
            lambda fn: (p := parse_filename(str(fn))) is not None and date_start <= p["date"] <= date_end
        )]

    if df.empty:
        print("No detections match requested directories + date range.")
        return

    df.to_csv(output_path, index=False)
    print(f"Output: {len(df)} detections -> {output_path}")


# =============================================================================
# MAIN
# =============================================================================
def main():
    cfg.apply_overrides()
    processed_set = load_processed_files(cfg.PROCESSED_FILE)
    files_to_process = list_files(
        input_directories=cfg.INPUT_DIRECTORIES,
        date_start=cfg.DATE_START,
        date_end=cfg.DATE_END,
        processed_files=processed_set,
        input_file_list=cfg.INPUT_FILE_LIST,
    )

    # Map reference-file basename -> attached spot (aligned INPUT_FILE_LIST/SPOTS).
    spot_overrides = {}
    ref_basenames = set()
    spots_aligned = list(cfg.INPUT_FILE_SPOTS) + [""] * (len(cfg.INPUT_FILE_LIST) - len(cfg.INPUT_FILE_SPOTS))
    for pth, sp in zip(cfg.INPUT_FILE_LIST, spots_aligned):
        base = os.path.basename(os.path.abspath(pth))
        ref_basenames.add(base)
        if sp:
            spot_overrides[base] = sp

    # Map directory-based spot label (aligned INPUT_DIRECTORIES/DATASET_SPOTS) onto
    # every file discovered under that directory. Without this, the "spot" column
    # falls back to whatever parse_filename extracts from the filename's own
    # device-ID prefix (e.g. "04213SPOT1"), which can differ from the spot name a
    # user chose in the UI — acoustic_indices.py already does this; birdnet was
    # missing it, so its aggregate's spot values were inconsistent with every
    # other script's.
    if cfg.DATASET_SPOTS:
        ds_aligned = list(cfg.DATASET_SPOTS) + [""] * max(0, len(cfg.INPUT_DIRECTORIES) - len(cfg.DATASET_SPOTS))
        for d, s in zip(cfg.INPUT_DIRECTORIES, ds_aligned):
            if s:
                for filepath in files_to_process:
                    base = os.path.basename(filepath)
                    if base in spot_overrides:
                        continue
                    parent = os.path.dirname(os.path.abspath(filepath))
                    dir_path = os.path.abspath(d)
                    if parent == dir_path or parent.startswith(dir_path + os.sep):
                        spot_overrides[base] = s

    run_pipeline(
        file_list=files_to_process,
        aggregate_path=cfg.AGGREGATE_FILE,
        processed_files_path=cfg.PROCESSED_FILE,
        spot_overrides=spot_overrides,
    )

    write_output_csv(
        aggregate_path=cfg.AGGREGATE_FILE,
        output_path=cfg.OUTPUT_CSV,
        input_directories=cfg.INPUT_DIRECTORIES,
        date_start=cfg.DATE_START,
        date_end=cfg.DATE_END,
        reference_basenames=ref_basenames,
    )


if __name__ == "__main__":
    main()
