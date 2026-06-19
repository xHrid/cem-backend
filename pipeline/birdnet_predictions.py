"""
00b: BirdNET Predictions Pipeline
===================================
Three-part pipeline:
  1. File listing  — discover, filter, deduplicate WAV files
  2. Main pipeline — run BirdNET on new files, append to aggregate CSV
  3. Output CSV    — filtered subset of aggregate for requested range

Performance optimizations (vs. original):
  - Hardware-adaptive parallelism via hw_profile (CPU cores, RAM, GPU)
  - GPU inference path using `birdnet` library ProtoBuf model when NVIDIA GPU detected
  - Combined denoise: static + rain noise removal in a single STFT pass
  - Faster resampling (kaiser_fast instead of kaiser_best — 3-5x speedup)
  - Noise STFT pre-computed once per worker, not per file
"""

import os
import tempfile
import numpy as np
import pandas as pd
import soundfile as sf
import librosa
from datetime import date
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import io
import contextlib

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
os.environ["TF_ENABLE_ONEDNN_OPTS"] = "0"

import warnings
warnings.filterwarnings("ignore", message=".*tf.lite.Interpreter is deprecated.*")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="tensorflow")

import config as cfg
from file_metadata import parse_filename, build_record
from hw_profile import get_profile, print_profile

# Faster resample method — kaiser_fast is 3-5x faster than default kaiser_best
# with negligible quality loss for bird audio analysis at 48 kHz.
_RESAMPLE_TYPE = "kaiser_fast"


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

# ── Combined denoise (single STFT pass for both noise sources) ───────────────

def _denoise_combined(audio: np.ndarray,
                      noise_clips: list[np.ndarray],
                      noise_stfts: list[np.ndarray] | None = None,
                      sr: int | None = None,
                      snr_db: float | None = None) -> np.ndarray:
    """Remove multiple noise sources in a single STFT pass.

    If noise_stfts are pre-computed (from worker init), skip re-computing them.
    This halves the STFT work compared to calling _denoise twice.
    """
    sr = cfg.TARGET_SR if sr is None else sr
    snr_db = cfg.SNR_DB if snr_db is None else snr_db
    n_fft, hop = 2048, 512

    # Time-domain subtraction for each noise source
    cleaned = audio.copy()
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

    # Single STFT pass for spectral gating
    stft = librosa.stft(cleaned, n_fft=n_fft, hop_length=hop)
    magnitude, phase = np.abs(stft), np.angle(stft)

    # Combine noise thresholds from all sources (take max)
    combined_threshold = np.zeros((magnitude.shape[0], 1), dtype=np.float32)
    if noise_stfts:
        for ns in noise_stfts:
            threshold = np.mean(np.abs(ns), axis=1, keepdims=True) * 1.2
            combined_threshold = np.maximum(combined_threshold, threshold)
    else:
        for noise_ref in noise_clips:
            if len(noise_ref) > len(audio):
                nr = noise_ref[:len(audio)]
            else:
                nr = np.pad(noise_ref, (0, len(audio) - len(noise_ref)), "wrap")
            ns = librosa.stft(nr, n_fft=n_fft, hop_length=hop)
            threshold = np.mean(np.abs(ns), axis=1, keepdims=True) * 1.2
            combined_threshold = np.maximum(combined_threshold, threshold)

    gated_mag = np.where(magnitude > combined_threshold, magnitude, 0)
    return librosa.istft(gated_mag * np.exp(1j * phase), hop_length=hop)


# ── CPU path: birdnetlib (TFLite) ───────────────────────────────────────────

def _analyze_file_tflite(filepath, analyzer, noise_clips, noise_stfts):
    """Analyze a single file using birdnetlib (TFLite, CPU)."""
    audio_raw, orig_sr = sf.read(filepath, dtype="float32")
    if audio_raw.ndim > 1:
        audio_raw = audio_raw.mean(axis=1)
    if orig_sr != cfg.TARGET_SR:
        audio_raw = librosa.resample(y=audio_raw, orig_sr=orig_sr,
                                     target_sr=cfg.TARGET_SR, res_type=_RESAMPLE_TYPE)

    audio_clean = _denoise_combined(audio_raw, noise_clips, noise_stfts)

    from birdnetlib.main import RecordingBuffer
    recording = RecordingBuffer(
        analyzer, audio_clean, cfg.TARGET_SR,
        lat=cfg.LATITUDE, lon=cfg.LONGITUDE, min_conf=cfg.MIN_CONFIDENCE,
    )
    with contextlib.redirect_stdout(io.StringIO()):
        recording.analyze()
    return pd.DataFrame(recording.detections)


# ── GPU path: birdnet library (ProtoBuf model) ──────────────────────────────

def _analyze_file_gpu(filepath, model, noise_clips, noise_stfts):
    """Analyze a single file using birdnet library (ProtoBuf, GPU-capable).

    Denoise → write temp WAV → predict with GPU model → return DataFrame.
    """
    audio_raw, orig_sr = sf.read(filepath, dtype="float32")
    if audio_raw.ndim > 1:
        audio_raw = audio_raw.mean(axis=1)
    if orig_sr != cfg.TARGET_SR:
        audio_raw = librosa.resample(y=audio_raw, orig_sr=orig_sr,
                                     target_sr=cfg.TARGET_SR, res_type=_RESAMPLE_TYPE)

    audio_clean = _denoise_combined(audio_raw, noise_clips, noise_stfts)

    # Write denoised audio to temp file for birdnet library
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        sf.write(tmp_path, audio_clean, cfg.TARGET_SR)
        predictions = model.predict(
            tmp_path,
            min_confidence=cfg.MIN_CONFIDENCE,
            lat=cfg.LATITUDE,
            lon=cfg.LONGITUDE,
        )
        if predictions is None or (hasattr(predictions, "empty") and predictions.empty):
            return pd.DataFrame()

        # Normalize column names to match birdnetlib output format
        df = predictions if isinstance(predictions, pd.DataFrame) else pd.DataFrame(predictions)
        if "species_name" in df.columns and "common_name" not in df.columns:
            # birdnet returns "Sci_name_Common Name" format
            df["common_name"] = df["species_name"].apply(
                lambda s: s.split("_", 1)[1] if "_" in str(s) else str(s)
            )
            df["scientific_name"] = df["species_name"].apply(
                lambda s: s.split("_", 1)[0] if "_" in str(s) else str(s)
            )
        return df
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


# ── Worker state (per-process) ───────────────────────────────────────────────

_worker_analyzer = None
_worker_model_gpu = None
_worker_noise_clips = None
_worker_noise_stfts = None
_worker_use_gpu = False


def _init_worker(noise_path, rain_path, tflite_threads, use_gpu):
    global _worker_analyzer, _worker_model_gpu
    global _worker_noise_clips, _worker_noise_stfts, _worker_use_gpu
    _worker_use_gpu = use_gpu

    n_fft, hop = 2048, 512

    # Load and resample noise clips once
    clips = []
    stfts = []
    for path in (noise_path, rain_path):
        clip, clip_sr = sf.read(path, dtype="float32")
        if clip.ndim > 1:
            clip = clip.mean(axis=1)
        if clip_sr != cfg.TARGET_SR:
            clip = librosa.resample(y=clip, orig_sr=clip_sr,
                                    target_sr=cfg.TARGET_SR, res_type=_RESAMPLE_TYPE)
        clips.append(clip)
        # Pre-compute noise STFT so we skip it per-file
        stfts.append(librosa.stft(clip, n_fft=n_fft, hop_length=hop))
    _worker_noise_clips = clips
    _worker_noise_stfts = stfts

    if use_gpu:
        try:
            import birdnet as bn
            _worker_model_gpu = bn.load("acoustic", "2.4", "tf")
            return
        except Exception as e:
            print(f"  GPU model load failed ({e}), falling back to TFLite CPU")
            _worker_use_gpu = False

    # CPU path: birdnetlib
    from birdnetlib.analyzer import Analyzer
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        _worker_analyzer = Analyzer()

    if tflite_threads > 1:
        try:
            interp = _worker_analyzer.interpreter
            with contextlib.redirect_stderr(io.StringIO()):
                new_interp = type(interp)(
                    model_path=_worker_analyzer.model_path,
                    num_threads=tflite_threads,
                )
            new_interp.allocate_tensors()
            _worker_analyzer.interpreter = new_interp
            _worker_analyzer.input_details = new_interp.get_input_details()
            _worker_analyzer.output_details = new_interp.get_output_details()
            _worker_analyzer.input_layer_index = _worker_analyzer.input_details[0]["index"]
            _worker_analyzer.output_layer_index = _worker_analyzer.output_details[0]["index"]
        except Exception:
            pass


def _process_single_file(item):
    filepath, spot_override = item
    rec = build_record(filepath, spot=spot_override)
    filename = rec["filename"]
    try:
        if _worker_use_gpu and _worker_model_gpu is not None:
            df = _analyze_file_gpu(filepath, _worker_model_gpu,
                                   _worker_noise_clips, _worker_noise_stfts)
        else:
            df = _analyze_file_tflite(filepath, _worker_analyzer,
                                      _worker_noise_clips, _worker_noise_stfts)
        if not df.empty:
            df["filename"] = rec["filename"]
            df["filepath"] = rec["filepath"]
            df["spot"]     = rec["spot"]
            df["date"]     = rec["date"]
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


def run_pipeline(file_list, aggregate_path, processed_files_path, spot_overrides=None):
    spot_overrides = spot_overrides or {}
    if not file_list:
        print("No new files to process.")
        return pd.DataFrame()

    profile = get_profile()
    print_profile(profile)

    n_workers = profile["birdnet_workers"]
    tflite_threads = profile["tflite_threads"]
    use_gpu = profile["use_gpu_model"]

    mode = "GPU (birdnet ProtoBuf)" if use_gpu else f"CPU (birdnetlib TFLite, {tflite_threads} threads/worker)"
    print(f"Inference: {mode}, {n_workers} workers")

    all_detections = []
    processed_this_run = set()
    already_processed = load_processed_files(processed_files_path)

    with ProcessPoolExecutor(
        max_workers=n_workers,
        initializer=_init_worker,
        initargs=(cfg.STATIC_NOISE_PATH, cfg.RAIN_NOISE_PATH, tflite_threads, use_gpu),
    ) as executor:
        items = [(fp, spot_overrides.get(os.path.basename(fp))) for fp in file_list]
        futures = {executor.submit(_process_single_file, it): it[0] for it in items}
        with tqdm(total=len(file_list), desc="BirdNET") as pbar:
            for future in as_completed(futures):
                filename, result = future.result()
                processed_this_run.add(filename)
                if result is not None:
                    all_detections.append(result)
                pbar.update(1)

    new_df = pd.DataFrame()
    if all_detections:
        new_df = pd.concat(all_detections, ignore_index=True)
        header = not os.path.isfile(aggregate_path)
        new_df.to_csv(aggregate_path, mode="a", header=header, index=False)
        print(f"Appended {len(new_df)} detections to {aggregate_path}")
    else:
        print("No detections in this batch.")

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
        abs_dirs = [os.path.abspath(d) for d in input_directories]
        in_dirs = df["filepath"].apply(
            lambda fp: not pd.isna(fp) and any(os.path.abspath(str(fp)).startswith(d + os.sep) for d in abs_dirs)
        )
        in_refs = df["filename"].isin(reference_basenames) if "filename" in df.columns else False
        df = df[in_dirs | in_refs]

    if "date" in df.columns:
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

    spot_overrides = {}
    ref_basenames = set()
    spots_aligned = list(cfg.INPUT_FILE_SPOTS) + [""] * max(0, len(cfg.INPUT_FILE_LIST) - len(cfg.INPUT_FILE_SPOTS))
    for pth, sp in zip(cfg.INPUT_FILE_LIST, spots_aligned):
        base = os.path.basename(os.path.abspath(pth))
        ref_basenames.add(base)
        if sp:
            spot_overrides[base] = sp

    dir_spot_map = {}
    if cfg.DATASET_SPOTS:
        ds_aligned = list(cfg.DATASET_SPOTS) + [""] * max(0, len(cfg.INPUT_DIRECTORIES) - len(cfg.DATASET_SPOTS))
        for d, s in zip(cfg.INPUT_DIRECTORIES, ds_aligned):
            if s:
                dir_spot_map[os.path.abspath(d)] = s
    if dir_spot_map:
        for filepath in files_to_process:
            base = os.path.basename(filepath)
            if base in spot_overrides:
                continue
            parent = os.path.dirname(os.path.abspath(filepath))
            for dir_path, spot_name in dir_spot_map.items():
                if parent == dir_path or parent.startswith(dir_path + os.sep):
                    spot_overrides[base] = spot_name
                    break

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
