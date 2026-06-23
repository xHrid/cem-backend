# CEM Bioacoustics API

A Docker container that runs BirdNET bird detection and six ecological analysis scripts over a simple REST API.

**Frontend:** https://github.com/xHrid/cem-toolkit  
**Docker image:** `hridayansh/cem-backend` (DockerHub)

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

## Quick start (build from source)

You need Docker and git.

```bash
git clone https://github.com/xHrid/cem-backend.git
cd cem-backend

cp .env.example .env
# Open .env and set ALLOWED_ORIGINS to your frontend URL (see Configuration)

docker compose up --build -d

# Verify it's running
curl localhost:8000/health
```

API docs (interactive): http://localhost:8000/docs

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
docker build -t hridayansh/cem-backend:1.2.0 .
docker tag hridayansh/cem-backend:1.2.0 hridayansh/cem-backend:latest

# Push
docker login
docker push hridayansh/cem-backend:1.2.0
docker push hridayansh/cem-backend:latest
```

Then update the `image:` tag in `docker-compose.yml` and commit.

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
└── docker-compose.yml         ← builds from source, bind-mounts code
```

Pipeline scripts in `pipeline/` are the single source of truth. The local watcher pulls them from this repo via GitHub raw URLs.
