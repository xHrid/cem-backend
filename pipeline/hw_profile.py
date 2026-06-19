"""
Hardware detection and resource allocation.

Probes CPU cores, system RAM, and NVIDIA GPUs at import time.
Pipeline scripts call get_profile() to decide worker counts,
TFLite thread counts, and whether to use a GPU-accelerated model.

All detection is best-effort with safe fallbacks.
"""

import multiprocessing
import os
import shutil
import subprocess


# ── GPU detection ────────────────────────────────────────────────────────────

def _detect_gpus_nvidia_smi() -> list[dict]:
    """Query nvidia-smi for GPU name + VRAM."""
    if not shutil.which("nvidia-smi"):
        return []
    try:
        r = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,memory.total",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5,
        )
        if r.returncode != 0 or not r.stdout.strip():
            return []
        gpus = []
        for line in r.stdout.strip().splitlines():
            parts = [p.strip() for p in line.split(",")]
            gpus.append({"name": parts[0], "memory_mb": int(float(parts[1]))})
        return gpus
    except Exception:
        return []


def _detect_gpus_tf() -> list[dict]:
    """Fall back to TensorFlow device listing."""
    try:
        os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")
        import tensorflow as tf  # noqa: delayed import
        devices = tf.config.list_physical_devices("GPU")
        if devices:
            return [{"name": d.name, "memory_mb": 0} for d in devices]
    except Exception:
        pass
    return []


def detect_gpus() -> list[dict]:
    gpus = _detect_gpus_nvidia_smi()
    if not gpus:
        gpus = _detect_gpus_tf()
    return gpus


# ── RAM detection ────────────────────────────────────────────────────────────

def _get_ram_gb() -> float:
    try:
        import psutil  # noqa: delayed import
        return psutil.virtual_memory().total / (1024 ** 3)
    except ImportError:
        pass
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal"):
                    return int(line.split()[1]) / (1024 ** 2)
    except Exception:
        pass
    return 4.0  # conservative fallback


# ── Check if birdnet (new library with GPU ProtoBuf) is importable ───────────

def _has_birdnet_lib() -> bool:
    try:
        import birdnet  # noqa
        return True
    except ImportError:
        return False


# ── Main profile ─────────────────────────────────────────────────────────────

def get_profile() -> dict:
    """Return a hardware profile dict used by pipeline scripts.

    Keys:
        cpus            – logical CPU count
        ram_gb          – system RAM (float)
        gpus            – list of {"name", "memory_mb"}
        has_gpu         – bool, True if NVIDIA GPU detected
        has_birdnet_lib – bool, True if `birdnet` package (GPU-capable) installed
        use_gpu_model   – bool, True if both GPU + birdnet lib available
        birdnet_workers – recommended ProcessPoolExecutor workers for BirdNET
        tflite_threads  – threads per TFLite interpreter (CPU path)
        indices_workers – recommended workers for acoustic indices
    """
    cpus = multiprocessing.cpu_count()
    ram_gb = _get_ram_gb()
    gpus = detect_gpus()
    has_gpu = len(gpus) > 0
    has_bn = _has_birdnet_lib()

    # ── BirdNET workers ──────────────────────────────────────────────────
    # Each worker loads its own TF/TFLite model (~500 MB).
    # Cap by RAM (1.5 GB headroom per worker) to avoid OOM / SIGKILL.
    max_by_ram = max(1, int(ram_gb / 1.5))

    env_override = os.environ.get("BIRDNET_MAX_WORKERS", "").strip()
    if env_override.isdigit() and int(env_override) > 0:
        n_workers = min(int(env_override), cpus)
    elif has_gpu and has_bn:
        # GPU does heavy inference; CPU workers handle I/O + denoise only
        n_workers = max(1, min(cpus // 2, 4, max_by_ram))
    else:
        n_workers = max(1, min(cpus // 2, max_by_ram))

    tflite_threads = max(1, cpus // n_workers)

    # ── Acoustic indices workers ─────────────────────────────────────────
    # Much lighter memory footprint (~200 MB each).
    max_idx_by_ram = max(1, int(ram_gb / 0.5))
    n_idx = max(1, min(cpus, max_idx_by_ram, 8))

    profile = {
        "cpus": cpus,
        "ram_gb": round(ram_gb, 1),
        "gpus": gpus,
        "has_gpu": has_gpu,
        "has_birdnet_lib": has_bn,
        "use_gpu_model": has_gpu and has_bn,
        "birdnet_workers": n_workers,
        "tflite_threads": tflite_threads,
        "indices_workers": n_idx,
    }
    return profile


def print_profile(profile: dict | None = None) -> None:
    """Pretty-print hardware profile (for log output)."""
    p = profile or get_profile()
    gpu_str = ", ".join(
        f"{g['name']} ({g['memory_mb']} MB)" for g in p["gpus"]
    ) if p["gpus"] else "none"
    print(
        f"Hardware: {p['cpus']} CPUs, {p['ram_gb']} GB RAM, GPU: {gpu_str}\n"
        f"  BirdNET workers={p['birdnet_workers']}, "
        f"TFLite threads/worker={p['tflite_threads']}, "
        f"GPU model={'yes' if p['use_gpu_model'] else 'no'}\n"
        f"  Indices workers={p['indices_workers']}"
    )
