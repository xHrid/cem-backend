# CEM BirdNET Pipeline API
# Default build = CPU. For GPU acceleration, build with:
#   docker build --build-arg BASE_IMAGE=nvidia/cuda:12.2.0-runtime-ubuntu22.04 \
#                --build-arg TF_PACKAGE=tensorflow --build-arg BN_EXTRA=[and-cuda] .
#
# The pipeline auto-detects GPU at runtime. If NVIDIA GPU + birdnet[and-cuda]
# are present it uses the ProtoBuf model on GPU; otherwise falls back to
# birdnetlib TFLite on CPU.
#
# Base image pinned to Debian 12 "bookworm" (stable).
ARG BASE_IMAGE=python:3.10-slim-bookworm
FROM ${BASE_IMAGE}

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DEFAULT_TIMEOUT=180 \
    TF_CPP_MIN_LOG_LEVEL=3 \
    DATA_DIR=/data \
    PIPELINE_DIR=/app/pipeline \
    PYTHON_BIN=python

# System libs: soundfile/librosa need libsndfile + ffmpeg; libgomp for TF/sklearn.
#
# apt hardening -- fixes the "Could not connect to deb.debian.org ... connection
# timed out" / "Unable to locate package" failures seen on some networks:
#   * ForceIPv4        avoids hangs where the host advertises but can't route IPv6
#   * https:// mirrors  work where plain HTTP (port 80) egress is firewalled
#                       (ca-certificates already ships in the python:*-slim base)
#   * Retries/Timeout   survive a flaky or slow mirror instead of aborting
RUN set -eux; \
    printf '%s\n' \
        'Acquire::ForceIPv4 "true";' \
        'Acquire::Retries "8";' \
        'Acquire::http::Timeout "30";' \
        'Acquire::https::Timeout "30";' \
        > /etc/apt/apt.conf.d/99-cem-network; \
    sed -i 's|http://deb.debian.org|https://deb.debian.org|g; s|http://security.debian.org|https://security.debian.org|g' \
        /etc/apt/sources.list /etc/apt/sources.list.d/debian.sources 2>/dev/null || true; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        libsndfile1 ffmpeg libgomp1; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# --- Python dependencies (pipeline + server) ---
# --retries/--timeout keep the heavy tensorflow-cpu/birdnetlib downloads from
# dying on a slow connection.
COPY requirements.txt ./requirements-pipeline.txt
COPY server/requirements-server.txt ./requirements-server.txt
RUN pip install --upgrade pip \
    && pip install --retries 8 --timeout 180 -r requirements-pipeline.txt \
    && pip install --retries 8 --timeout 180 -r requirements-server.txt

# --- Application code ---
# Pipeline scripts are NOT baked in -- mount them at /app/pipeline (or set
# PIPELINE_DIR) so code changes don't require a Docker rebuild.
COPY server/app ./app

# Persist uploads + results here (compose mounts a named volume).
VOLUME ["/data"]
EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
