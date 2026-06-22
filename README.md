# CEM Bioacoustics API

A Docker container that runs BirdNET bird detection and six ecological analysis scripts over a simple REST API.

**Frontend:** https://github.com/xHrid/cem-toolkit  
**Docker image:** `hridayansh/cem-bioacoustics` (DockerHub)

---

## What it does

Upload field audio → run BirdNET detection → run analysis scripts → download results.

| Step | Input | Output |
|---|---|---|
| `birdnet` | `.wav` audio files | aggregate CSV |
| `heatmaps` | aggregate CSV | per-spot hourly heatmaps (PNG) |
| `temporal_stickiness` | aggregate CSV | activity-regularity chart + CSV |
| `spatial_stickiness` | aggregate CSV (needs ≥2 spots) | habitat-affinity chart + CSV |
| `migratory_classification` | aggregate CSV | migratory/resident plots + CSV |
| `solar_correlation` | aggregate CSV | peak-vs-sunrise plots + CSV |
| `daily_timeseries` | aggregate CSV | per-species daily plots + CSV |

Run `birdnet` first — it produces the CSV that all other steps consume. If you already have a CSV, skip `birdnet`.

---

## Quick start (pull from DockerHub)

You only need Docker. No source checkout required.

```bash
# 1. Create a folder and grab the two config files
mkdir cem-bioacoustics && cd cem-bioacoustics
curl -O https://raw.githubusercontent.com/xHrid/cem-backend/main/docker-compose.hub.yml
curl -O https://raw.githubusercontent.com/xHrid/cem-backend/main/.env.example

# 2. Create your env file
cp .env.example .env
# Open .env and adjust ALLOWED_ORIGINS / paths as needed

# 3. Start the server
docker compose -f docker-compose.hub.yml up -d

# 4. Verify it's running
curl localhost:8000/health
```

API docs (interactive): http://localhost:8000/docs

---

## Build from source

Use this when you want to edit pipeline scripts or server code.

```bash
git clone https://github.com/xHrid/cem-backend.git
cd cem-backend

cp .env.example .env   # adjust as needed

docker compose up --build
```

The compose file bind-mounts `./pipeline` and `./server/app` into the container. After editing a script, `docker compose restart` is enough — no rebuild. Only rebuild (`--build`) when `requirements.txt` or `server/requirements-server.txt` changes.

---

## Configuration

Edit `.env` (copied from `.env.example`). Key variables:

| Variable | Default | Description |
|---|---|---|
| `ALLOWED_ORIGINS` | `*` | CORS origins allowed to call the API. Set to your frontend URL in production. |
| `HOST_DATA_DIR` | `./data` | Where uploaded audio and results are stored on the host. |
| `MAX_UPLOAD_MB` | `2048` | Upload size cap per file. |
| `BIRDNET_MAX_WORKERS` | `2` | BirdNET worker processes. Each loads its own TensorFlow model (~500 MB). Lower to `1` if it crashes with `BrokenProcessPool`. |
| `RETENTION_HOURS` | `168` | Delete job directories older than this many hours. `0` disables cleanup. |

See `.env.example` for the full list including STAC and file-browser options.

---

## API usage example

```bash
BASE="http://localhost:8000"
PROJECT="demo"
JOB="job_$(openssl rand -hex 6)"   # client mints the job_id

# Upload audio files into a project spot
curl -s -F project=$PROJECT -F spot=SPOT1 \
  -F files=@recording1.wav -F files=@recording2.wav \
  $BASE/api/v1/projects/upload/audio

# Run BirdNET (blocks until done, returns STAC item)
curl -s -H "Content-Type: application/json" \
  -d "{\"script\":\"birdnet\",\"project\":\"$PROJECT\",\"spots\":[\"SPOT1\"],\"job_id\":\"$JOB\",\"snr_db\":18}" \
  $BASE/api/v1/scripts

# Run an analysis (reuses the project aggregate; needs its own job_id)
curl -s -H "Content-Type: application/json" \
  -d "{\"script\":\"heatmaps\",\"project\":\"$PROJECT\",\"spots\":[\"SPOT1\"],\"job_id\":\"job_$(openssl rand -hex 6)\"}" \
  $BASE/api/v1/scripts

# Download all results of a job as a zip
curl -s -OJ $BASE/api/v1/jobs/$JOB/download
```

---

## Pushing a new image to DockerHub

Do this when dependencies change (`requirements.txt` or `server/requirements-server.txt`). Script edits don't need a new image.

```bash
# Build
docker build -t hridayansh/cem-bioacoustics:1.2.0 .
docker tag hridayansh/cem-bioacoustics:1.2.0 hridayansh/cem-bioacoustics:latest

# Push
docker login
docker push hridayansh/cem-bioacoustics:1.2.0
docker push hridayansh/cem-bioacoustics:latest
```

Then update the version tag in `docker-compose.hub.yml` and commit.

---

## Repo layout

```
cem-backend/
├── pipeline/          ← analysis scripts (also fetched by the local watcher)
├── server/
│   ├── app/           ← FastAPI server code
│   └── requirements-server.txt
├── requirements.txt   ← pipeline dependencies
├── Dockerfile
├── docker-compose.yml         ← dev (bind-mounts code, build from source)
└── docker-compose.hub.yml     ← prod (pulls image from DockerHub)
```

Pipeline scripts in `pipeline/` are the single source of truth. The local watcher pulls them from this repo via GitHub raw URLs.
