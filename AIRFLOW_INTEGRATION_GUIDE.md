# CEM Backend — API Reference for Airflow Integration

**Docker Image**: [`hridayansh/cem-backend`](https://hub.docker.com/repository/docker/hridayansh/cem-backend)
**Source**: [github.com/xHrid/cem-backend](https://github.com/xHrid/cem-backend)

---

## 1. System Overview

The CEM Backend is a Dockerised FastAPI server that runs bioacoustic analysis pipelines. It exposes a synchronous REST API — each algorithm call blocks until completion and returns results inline.

There are **7 algorithms**. `birdnet` runs first and produces an aggregate CSV. The remaining 6 consume that aggregate:

```
birdnet  →  heatmaps
         →  temporal_stickiness
         →  spatial_stickiness         (needs ≥2 spots)
         →  migratory_classification
         →  solar_correlation
         →  daily_timeseries
```

The 6 analysis steps are independent of each other — only `birdnet` is a prerequisite.

---

## 2. Current Application Flow (Without Airflow)

This is how the frontend currently calls the backend. The frontend code lives in `ServerService.js`.

```
   FRONTEND                                          CEM BACKEND
   ========                                          ===========

   ┌─────────────────────────────────────────────────────────────┐
   │ STEP 1: Upload audio files — mint a job                     │
   └─────────────────────────────────────────────────────────────┘

   POST /api/v1/datasets/audio
   Content-Type: multipart/form-data
   Body: files = [file1.wav, file2.wav, ...]
                                                ──────────────►
                                                { "status": "ok",
                                                  "job_id": "job_87c8741dcf2a",
                                                  "uploaded": [...] }
                                                ◄──────────────

   ┌─────────────────────────────────────────────────────────────┐
   │ STEP 2a (BirdNET only): Upload existing aggregate           │
   │         so BirdNET can APPEND new detections to it           │
   └─────────────────────────────────────────────────────────────┘

   POST /api/v1/jobs/{job_id}/datasets/aggregate
   Content-Type: multipart/form-data
   Body: files = [aggregate.csv]
                                                ──────────────►
                                                { "status": "ok",
                                                  "job_id": "...",
                                                  "uploaded": [...] }
                                                ◄──────────────

   ┌─────────────────────────────────────────────────────────────┐
   │ STEP 2b (BirdNET only): Upload processed-files list         │
   │         for dedup — server skips already-processed files     │
   └─────────────────────────────────────────────────────────────┘

   POST /api/v1/jobs/{job_id}/datasets/processed
   Content-Type: multipart/form-data
   Body: files = [processed_files.txt]
                                                ──────────────►
                                                { "status": "ok" }
                                                ◄──────────────

   ┌─────────────────────────────────────────────────────────────┐
   │ STEP 2c (Analysis steps only): Upload aggregate as input    │
   │         Analysis scripts need the BirdNET aggregate but     │
   │         the server is stateless per job — so we upload it   │
   └─────────────────────────────────────────────────────────────┘

   (First, mint a job via POST /api/v1/datasets/audio with a placeholder)
   Then:
   POST /api/v1/jobs/{job_id}/datasets/aggregate
   Content-Type: multipart/form-data
   Body: files = [aggregate.csv]
                                                ──────────────►
                                                ◄──────────────

   ┌─────────────────────────────────────────────────────────────┐
   │ STEP 3: Run the algorithm (SYNCHRONOUS — blocks until done) │
   └─────────────────────────────────────────────────────────────┘

   POST /api/v1/jobs/{job_id}/{algorithm}
   Content-Type: application/json
   Body: { ...params }
                                                ──────────────►
                                                (blocks 1s – 30min)
                                                ◄──────────────
                                                200 / 400 / 404 / 500

   ┌─────────────────────────────────────────────────────────────┐
   │ STEP 4: Download results                                    │
   └─────────────────────────────────────────────────────────────┘

   GET /api/v1/jobs/{job_id}/results
                                                ──────────────►
                                                { "results": [
                                                    "work/output.csv",
                                                    "work/aggregate.csv",
                                                    "results/heatmaps/..."
                                                  ] }
                                                ◄──────────────

   GET /api/v1/jobs/{job_id}/file?path={rel_path}
                                                ──────────────►
                                                (file bytes)
                                                ◄──────────────
```

---

## 3. API Endpoints — Detailed

### 3.1 Health Check

```
GET /health
```

No auth. Returns:
```json
{
  "status": "ok",
  "api_version": "1.1.0",
  "steps": ["birdnet", "heatmaps", "temporal_stickiness", "spatial_stickiness",
            "migratory_classification", "solar_correlation", "daily_timeseries"]
}
```

### 3.2 Steps Catalogue

```
GET /api/v1/steps
```

Returns full manifest for each algorithm — name, description, dependencies, parameters with defaults, input spec.

### 3.3 Upload Audio (Create Job)

```
POST /api/v1/datasets/audio
Content-Type: multipart/form-data
```

| Form Field | Type | Description |
|-----------|------|-------------|
| `files` | `File[]` | One or more `.wav` files |

**Response** (200):
```json
{
  "status": "ok",
  "job_id": "job_87c8741dcf2a",
  "uploaded": [
    { "filename": "04213SPOT1_20250831_083006.wav", "size_bytes": 1843200 }
  ],
  "audio_dir": "/data/jobs/job_87c8741dcf2a/input/audio"
}
```

This creates `data/jobs/{job_id}/` on disk with subdirs `input/audio/`, `work/`, `results/`.

### 3.4 Upload Additional Files

```
POST /api/v1/jobs/{job_id}/datasets/{kind}
Content-Type: multipart/form-data
```

| Path Param | Values |
|-----------|--------|
| `kind` | `audio`, `aggregate`, `processed`, `ebird`, `static_noise`, `rain_noise` |

| Form Field | Type | Description |
|-----------|------|-------------|
| `files` | `File[]` | Files to upload |

Behavior by kind:

| Kind | Filename on disk | Purpose |
|------|-----------------|---------|
| `audio` | Original name → `input/audio/` | Append more WAVs |
| `aggregate` | `input/aggregate.csv` | Seed BirdNET or feed analysis steps |
| `processed` | `input/processed_files.txt` | Dedup list — BirdNET skips these files |
| `ebird` | `input/ebird_checklist.txt` | Override default eBird checklist |
| `static_noise` | `input/static_noise.wav` | Override default noise profile |
| `rain_noise` | `input/rain_noise.wav` | Override default rain noise profile |

**The aggregate upload is critical**: the server is stateless per job. For BirdNET, the frontend uploads its cached aggregate so new detections are **appended** to it rather than starting from scratch. For analysis steps, the aggregate **is** the input — without it the algorithm returns 404.

### 3.5 Run Algorithm (Synchronous)

```
POST /api/v1/jobs/{job_id}/{algorithm}
Content-Type: application/json
```

`{algorithm}` is one of: `birdnet`, `heatmaps`, `temporal_stickiness`, `spatial_stickiness`, `migratory_classification`, `solar_correlation`, `daily_timeseries`.

#### Request Body

All fields are optional. If omitted, the server uses config defaults.

```json
{
  "spots": "04213SPOT1,71301SPOT2",
  "start_date": "20251101",
  "end_date": "20251231",
  "snr_db": 18,
  "min_confidence": 0.25,
  "top_n_species": 25,
  "top_n_temporal": 80,
  "sci_threshold": 0.9,
  "kurtosis_threshold": 15,
  "pmr_threshold": 50,
  "window_size": 60,
  "min_solar_days": 5,
  "max_timeseries_species": 50,
  "filter_confidence": 0.3,
  "filter_min_detections": 10,
  "spots_geo": [
    { "name": "04213SPOT1", "lat": 28.5635, "lon": 77.1897 },
    { "name": "71301SPOT2", "lat": 28.1234, "lon": 77.5678 }
  ],
  "audio_spots": {
    "04213SPOT1_20250831_083006.wav": "04213SPOT1",
    "71301SPOT2_20250720_095544.wav": "71301SPOT2"
  }
}
```

**Parameter breakdown by algorithm:**

| Parameter | Used By | Description |
|-----------|---------|-------------|
| `spots` | All | Comma-separated spot names to filter |
| `start_date` | All | Inclusive start, `YYYYMMDD` |
| `end_date` | All | Inclusive end, `YYYYMMDD` |
| `snr_db` | birdnet | Denoise SNR in dB |
| `min_confidence` | birdnet | Detection confidence threshold, 0–1 |
| `top_n_species` | heatmaps | Top N species to plot |
| `top_n_temporal` | temporal_stickiness | Top N species to plot |
| `sci_threshold` | migratory_classification | Seasonal Concentration Index threshold |
| `kurtosis_threshold` | migratory_classification | Residual kurtosis threshold |
| `pmr_threshold` | migratory_classification | Peak-to-median ratio threshold |
| `window_size` | migratory_classification | Rolling window size (days) |
| `min_solar_days` | solar_correlation | Min days with >10 detections |
| `max_timeseries_species` | daily_timeseries | Max species to plot |
| `filter_confidence` | All 6 analyses | Min confidence for 3-step filter (default 0.3) |
| `filter_min_detections` | All 6 analyses | Min total detections per species (default 10) |
| `spots_geo` | All | Spot geolocation for STAC items |
| `audio_spots` | birdnet | Maps `{filename: spot_name}` — controls which spot name BirdNET writes into the aggregate CSV |

#### Success Response (200)

```json
{
  "status": "completed",
  "Success": "BirdNET Species Detection completed",
  "message": "BirdNET Species Detection completed",
  "task_id": "f5e2b620fdc440678b7a58295c02c1c4",
  "job_id": "job_87c8741dcf2a",
  "asset_id": "cem/bioacoustics/job_87c8741dcf2a/birdnet/20251101_20251231/output.csv",
  "stac": {
    "type": "Feature",
    "stac_version": "1.1.0",
    "id": "cem_bioacoustics_job_87c8741dcf2a_birdnet_...",
    "geometry": { "type": "Point", "coordinates": [77.1897, 28.5635] },
    "bbox": [77.1897, 28.5635, 77.1897, 28.5635],
    "properties": {
      "title": "BirdNET Species Detection",
      "datetime": "2026-06-10T09:27:38Z",
      "start_datetime": "2025-11-01T00:00:00Z",
      "end_datetime": "2025-12-31T00:00:00Z",
      "cem:algorithm_version": "1.0.0",
      "cem:api_version": "1.1.0",
      "cem:parameters": { "start_date": "20251101", "end_date": "20251231" },
      "cem:spots": [{ "name": "04213SPOT1", "lat": 28.5635, "lon": 77.1897 }]
    },
    "assets": { "data": { "href": "output.csv", "type": "text/csv", "roles": ["data"] } },
    "collection": "cem-bioacoustics"
  }
}
```

When an algorithm produces multiple output files, `asset_id` and `stac` become arrays:
```json
{
  "asset_id": ["cem/.../file1.csv", "cem/.../file2.png"],
  "asset_ids": ["cem/.../file1.csv", "cem/.../file2.png"],
  "stac": [ { ... }, { ... } ]
}
```

#### Error Responses

**400 — Bad Input** (map to STACD "skipped"):
```json
{
  "status": "skipped",
  "error": "BAD_REQUEST",
  "message": "Bad date '2025-13-01', expected YYYY-MM-DD or YYYYMMDD",
  "task_id": "b1c2d3e4..."
}
```

**404 — No Data** (map to STACD "skipped"):
```json
{
  "status": "skipped",
  "error": "NO_DATA",
  "message": "No aggregate available. Run birdnet first, or upload an aggregate CSV.",
  "task_id": "0f1e2d3c..."
}
```

**500 — Pipeline Failure** (map to STACD "failed"):
```json
{
  "status": "failed",
  "error": "PIPELINE_ERROR",
  "message": "Script exited with code 1. See _run.log.",
  "task_id": "9a8b7c6d..."
}
```

### 3.6 Read-Only Endpoints

| Endpoint | Returns |
|----------|---------|
| `GET /api/v1/jobs/{job_id}` | Job summary — inputs, tasks, results list, browse URL |
| `GET /api/v1/jobs/{job_id}/results` | List of result file paths relative to job root |
| `GET /api/v1/jobs/{job_id}/download` | ZIP of all results |
| `GET /api/v1/jobs/{job_id}/download/{step}` | ZIP of one step's results |
| `GET /api/v1/jobs/{job_id}/file?path={rel}` | Single file download |
| `GET /api/v1/jobs/{job_id}/tasks/{task_id}/log` | Plain text execution log |

---

## 4. Proposed Airflow Integration Flow

With Airflow in the middle, the frontend no longer calls the algorithm endpoints directly. Instead it triggers an Airflow DAG, which calls the backend.

```
   FRONTEND                    AIRFLOW                      CEM BACKEND
   ========                    =======                      ===========

   1. Upload audio
   ─────────────────────────────────────────────────────────► POST /datasets/audio
   ◄───────────────────────────────────────────────────────── { job_id }

   2. Upload aggregate (if exists)
   ─────────────────────────────────────────────────────────► POST /jobs/{id}/datasets/aggregate
   ◄─────────────────────────────────────────────────────────

   3. Upload processed list (if exists)
   ─────────────────────────────────────────────────────────► POST /jobs/{id}/datasets/processed
   ◄─────────────────────────────────────────────────────────

   4. Trigger Airflow DAG
      conf = { job_id, spots, start_date, end_date, ...params }
   ──────────────────────► POST /api/v1/dags/{dag}/dagRuns
   ◄────────────────────── { dag_run_id, state: "queued" }

                           5a. Run birdnet
                           ──────────────────────────────► POST /jobs/{id}/birdnet
                           ◄────────────────────────────── 200 { status: "completed" }

                           5b. Run analyses (parallel)
                           ──────────────────────────────► POST /jobs/{id}/heatmaps
                           ──────────────────────────────► POST /jobs/{id}/temporal_stickiness
                           ──────────────────────────────► POST /jobs/{id}/spatial_stickiness
                           ──────────────────────────────► POST /jobs/{id}/migratory_classification
                           ──────────────────────────────► POST /jobs/{id}/solar_correlation
                           ──────────────────────────────► POST /jobs/{id}/daily_timeseries
                           ◄────────────────────────────── 200 / 400 / 404 / 500 each

   6. Poll Airflow for status
   ──────────────────────► GET /api/v1/dags/{dag}/dagRuns/{run_id}
   ◄────────────────────── { state: "running" }
        ...repeat...
   ◄────────────────────── { state: "success" | "failed" }

   7. Fetch results from backend
   ─────────────────────────────────────────────────────────► GET /jobs/{id}/results
   ─────────────────────────────────────────────────────────► GET /jobs/{id}/file?path=...
```

**Key change**: Steps 1–3 (uploads) still go **directly** from frontend to CEM Backend. Airflow only handles step 5 (running algorithms). The frontend polls Airflow (step 6) instead of waiting on the synchronous HTTP response.

---

## 5. STACD Response Mapping

| HTTP Code | `status` field | STACD Task State | Downstream |
|-----------|---------------|-----------------|------------|
| 200 | `"completed"` | success | Register `asset_id` in catalog |
| 400 | `"skipped"` | skipped | Skip downstream — no asset |
| 404 | `"skipped"` | skipped | Skip downstream — no asset |
| 500 | `"failed"` | failed | Mark DAG run failed |

The `stac` field in 200 responses is a valid **STAC 1.1.0 Item** ready for catalog registration.

---

## 6. Docker Container Configuration

```bash
docker pull hridayansh/cem-backend

docker run -d \
  -p 8000:8000 \
  -v /host/data:/data \
  -e API_KEY=your-key \
  -e REQUIRE_AUTH=true \
  hridayansh/cem-backend
```

| Env Variable | Default | Description |
|-------------|---------|-------------|
| `API_KEY` | `changeme` | Shared secret for `X-API-Key` header |
| `REQUIRE_AUTH` | `true` | Set `false` for trusted networks |
| `DATA_DIR` | `/data` | Job storage root inside container |
| `MAX_UPLOAD_MB` | `2048` | Per-file upload limit |
| `MAX_CONCURRENT_TASKS` | `2` | Parallel task cap |
| `RETENTION_HOURS` | `168` | Auto-delete jobs older than this (0 = off) |
| `API_VERSION` | `1.1.0` | Stamped into responses and STAC items |
| `STAC_COLLECTION` | `cem-bioacoustics` | STAC collection name |
| `STACD_STAC_VERSION` | `1.1.0` | STAC version in sync API responses |
| `STACD_ASSET_ID_PREFIX` | `cem/bioacoustics` | Prefix for generated asset IDs |

**Volume mount**: `/data` contains all job workspaces. Mount it to persist across container restarts.

### Job Disk Layout

```
/data/jobs/{job_id}/
  job.json                        # metadata + task records
  input/
    audio/                        # uploaded WAV files
    aggregate.csv                 # uploaded aggregate (if any)
    processed_files.txt           # uploaded dedup list (if any)
    ebird_checklist.txt           # override (falls back to baked-in)
    static_noise.wav              # override (falls back to baked-in)
    rain_noise.wav                # override (falls back to baked-in)
  work/
    output.csv                    # birdnet filtered output
    aggregate.csv                 # birdnet-produced aggregate (APPENDED to uploaded one)
    processed_files.txt           # merged processed list
    *.stac.json                   # STAC sidecars for work outputs
  results/
    {step}/                       # per-step outputs
      _run.log                    # execution log
      *.csv, *.png                # result files
      *.stac.json                 # STAC sidecars
```

---

## 7. Swagger Docs

Run the container and open:

```
http://localhost:8000/docs
```

Full interactive API documentation with example request/response bodies for every endpoint.
