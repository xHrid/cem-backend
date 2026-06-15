# CEM BirdNET Pipeline API
# Default build = CPU. BirdNET runs on the TFLite interpreter (CPU), so a GPU
# image gives ~no speedup for inference; the optional GPU bits below exist only
# for users who later swap in a GPU-delegated model. See README.
#
# Base image is pinned to Debian 12 "bookworm" (stable). The unpinned
# python:3.10-slim tag can resolve to "trixie" (Debian testing), whose mirrors
# are slower/less reliable and were the cause of the apt download stalls.
FROM python:3.10-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DEFAULT_TIMEOUT=180 \
    TF_CPP_MIN_LOG_LEVEL=3 \
    DATA_DIR=/data \
    PIPELINE_DIR=/app/pipeline \
    PYTHON_BIN=python

# System libs: soundfile/librosa need libsndfile + ffmpeg; libgomp for TF/sklearn.
# Acquire::Retries makes apt survive a flaky/slow mirror instead of aborting.
RUN apt-get update -o Acquire::Retries=8 \
    && apt-get install -y --no-install-recommends -o Acquire::Retries=8 \
        libsndfile1 ffmpeg libgomp1 \
    && rm -rf /var/lib/apt/lists/*

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
COPY pipeline ./pipeline
COPY server/app ./app

# Persist uploads + results here (compose mounts a named volume).
VOLUME ["/data"]
EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
