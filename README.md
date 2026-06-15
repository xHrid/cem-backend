# CEM Bioacoustics Compute API

A dockerized HTTP service that wraps the **BirdNET** acoustic-monitoring pipeline. Upload field audio (or a precomputed aggregate CSV), run BirdNET species detection plus six ecological analyses, and download the generated CSVs and plots — all over a small REST API.

This is the **backend / compute** half of the CEM Cloud toolkit. The web frontend lives in a separate repo:

> **Frontend:** https://github.com/xHrid/CEM-Cloud

The published Docker image is on DockerHub:

> **Image:** [`hridayansh/cem-bioacoustics`](https://hub.docker.com/r/hridayansh/cem-bioacoustics) — current tag `1.1.0`

---

## Contents

- [What it does](#what-it-does)
- [Run it — Option A: pull from DockerHub (fastest)](#option-a--run-from-dockerhub-fastest)
- [Run it — Option B: build from source](#option-b--build-from-source)
- [Configuration](#configuration)
- [End-to-end API example](#end-to-end-api-example)
- [Endpoint reference](#endpoint-reference)
- [Connecting the frontend](#connecting-the-frontend)
- [For maintainers: publish to DockerHub](#for-maintainers-publish-a-new-image-to-dockerhub)
- [What is / isn't in this repo](#what-is-and-isnt-in-this-repo)
- [Run without Docker (dev)](#run-without-docker-dev)

---

## What it does

The pipeline has one detection script and six analysis scripts:

| Step id | Script | Input | Output |
|---|---|---|---|
| `birdnet` | `birdnet_predictions.py` | raw `.wav` files | aggregate CSV + filtered output CSV |
| `heatmaps` | `activity_heatmaps.py` | aggregate CSV | per-spot hourly activity heatmaps (PNG) |
| `temporal_stickiness` | `temporal_stickiness.py` | aggregate CSV | activity-regularity chart + CSV |
| `spatial_stickiness` | `spatial_stickiness.py` | aggregate CSV (needs ≥2 spots) | habitat-affinity chart + heatmap + CSV |
| `migratory_classification` | `migratory_classification.py` | aggregate CSV | migratory/resident plots + CSV |
| `solar_correlation` | `solar_correlation.py` | aggregate CSV | peak-vs-sunrise plots + CSV |
| `daily_timeseries` | `daily_call_timeseries.py` | aggregate CSV | per-species daily plots + availability heatmap |

`birdnet` produces the aggregate the other six consume. A typical job is: **upload audio → run `birdnet` → run analyses → download**. If you already have an aggregate CSV, upload that and skip `birdnet`.

The server adds a **job** model (each upload gets an isolated workspace), **async execution** (run calls return a `task_id` you poll), and **upload/download** endpoints on top of the scripts.

---

## Option A — run from DockerHub (fastest)

No source checkout, no build. You only need Docker and two files. Good for deploying or just trying it out.

**Prerequisites:** Docker Desktop / Docker Engine with Compose v2 (≥ 4 GB memory recommended — BirdNET loads TensorFlow per worker).

```bash
# 1. Make an empty folder and grab the two files you need.
mkdir cem-bioacoustics && cd cem-bioacoustics
curl -O https://raw.githubusercontent.com/xHrid/<BACKEND-REPO>/main/docker-compose.hub.yml
curl -O https://raw.githubusercontent.com/xHrid/<BACKEND-REPO>/main/.env.example

# 2. Create your config and set a real API key.
cp .env.example .env
#   edit .env  ->  API_KEY=<a long random string>

# 3. Pull + start. The image downloads from DockerHub.
docker compose -f docker-compose.hub.yml up -d
```

Or pull/run the image directly without compose:

```bash
docker pull hridayansh/cem-bioacoustics:1.1.0
docker run -d --name cem-bioacoustics-api \
  -p 8000:8000 \
  -e API_KEY="your-long-random-key" \
  -v "$(pwd)/data:/data" \
  hridayansh/cem-bioacoustics:1.1.0
```

Verify it's up (the health check needs no auth):

```bash
curl localhost:8000/health
```

Interactive API docs: **http://localhost:8000/docs**

> Replace `<BACKEND-REPO>` with the backend repo name once it's pushed to GitHub (see below). Until then, copy `docker-compose.hub.yml` and `.env.example` out of this folder.

---

## Option B — build from source

Use this when you're changing the pipeline scripts or server code.

```bash
git clone https://github.com/xHrid/<BACKEND-REPO>.git
cd <BACKEND-REPO>

cp .env.example .env          # set a real API_KEY
docker compose up --build     # builds the image locally, starts API on :8000
```

`docker-compose.yml` (the dev compose) bind-mounts `./pipeline` and `./server/app` into the container, so script/code edits ship with a `docker compose restart` — no rebuild. Rebuild (`--build`) only when a dependency changes (`requirements.txt` / `server/requirements-server.txt`).

---

## Configuration

Copy `.env.example` to `.env` and edit. `docker compose` reads it automatically.

| Env var | Default | Meaning |
|---|---|---|
| `API_KEY` | `changeme` | Shared key for the `X-API-Key` header. **Change it.** |
| `REQUIRE_AUTH` | `true` | Set `false` to open all endpoints (no key needed). |
| `MAX_UPLOAD_MB` | `2048` | Per-file upload cap. |
| `MAX_CONCURRENT_TASKS` | `2` | Parallel script runs (bounded thread pool). |
| `BIRDNET_MAX_WORKERS` | `2` | BirdNET worker processes; each loads its own TF model. Lower to `1` if BirdNET dies with `BrokenProcessPool` on a low-memory host. |
| `ALLOWED_ORIGINS` | `*` | CORS: which web origins may call the API from a browser. Set to your frontend URL in production. |
| `HOST_DATA_DIR` | `./data` | Host directory bind-mounted to `/data`. Use an absolute path on a shared lab node, e.g. `/srv/cem/data`. |
| `API_VERSION` | `1.1.0` | Compute version stamped into outputs + STAC items. |
| `STAC_ENABLED` | `true` | Write a STAC 1.0 sidecar (`<file>.stac.json`) next to each output. |
| `RETENTION_HOURS` | `168` | Job dirs older than this are swept. `0` disables. |

See `.env.example` for the full annotated list (STAC, file-browser deep links, retention sweep interval).

---

## End-to-end API example

```bash
KEY="your-api-key"
BASE="http://localhost:8000"

# 1. Upload audio — creates a new job and returns its job_id.
JOB=$(curl -s -H "X-API-Key: $KEY" \
  -F kind=audio \
  -F files=@CRIMESPOT3_20251130_093100.wav \
  -F files=@CRIMESPOT5_20251130_101500.wav \
  $BASE/upload | python -c "import sys,json;print(json.load(sys.stdin)['job_id'])")

# 2. Run BirdNET (async -> returns a task_id).
TASK=$(curl -s -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  -d '{"snr_db":18}' \
  $BASE/jobs/$JOB/run/birdnet | python -c "import sys,json;print(json.load(sys.stdin)['task_id'])")

# 3. Poll until status == success | failed.
curl -s -H "X-API-Key: $KEY" $BASE/jobs/$JOB/tasks/$TASK

# 4. Run analyses individually, or everything at once:
curl -s -H "X-API-Key: $KEY" $BASE/jobs/$JOB/run/heatmaps
curl -s -H "X-API-Key: $KEY" $BASE/jobs/$JOB/run-all

# 5. Download results.
curl -s -H "X-API-Key: $KEY" -OJ $BASE/jobs/$JOB/download       