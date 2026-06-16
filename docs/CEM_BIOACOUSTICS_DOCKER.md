# CEM Bioacoustics Compute Docker — Reference

**Audience:** the maintainer (running it), the professor (what it is / how it fits
the cluster model), and the integration teams (NGINX, Airflow, file-browser,
STAC-B, user management).

**What this is:** a single Dockerised FastAPI service that runs BirdNET species
detection plus six ecological analyses over field audio. It is the **bioacoustics
compute box** in the lab's monitoring cluster: it does work and writes outputs
into a shared host data directory. It is driven either directly over REST (by the
web app) or headlessly by an orchestrator (Airflow). It produces STAC provenance
for every output.

This is the only documentation file for the service. It is generated to match the
current code (`server/app/`, `Dockerfile`, `docker-compose.yml`,
`pipeline/manifest.json`); when those change, update this file.

---

## 1. Table of contents

1. Table of contents
2. System overview & where this fits
3. Quick start — build, run, expose, wire to the web app
4. Configuration (environment variables)
5. Data layout on disk
6. Compute steps & parameters
7. REST API reference (async `/jobs`)
8. STACD / Airflow synchronous algorithm API (`/api/v1`)
9. Two compute modes & the watcher/server data split
10. Provenance: STAC items & versioning
11. File-browser link
12. Headless / Airflow triggering (CLI)
13. Output retention
14. Build / update / add-a-library
15. Integration points for other teams
16. Troubleshooting
17. Assumptions & ownership boundaries

---

## 2. System overview & where this fits

The lab runs acoustic + drone ecological monitoring. The target architecture is
one NGINX entry point routing by URL to one or more compute dockers; heavy compute
is triggered through an Airflow orchestrator; every output produces a STAC item;
a file-browser service exposes the shared data directory to users. **This repo is
only the bioacoustics compute docker.** NGINX, Airflow, the file-browser service,
STAC-B ingest, and central user management are other teams' work — this service
produces what plugs into them.

```
                    ┌────────────────────────── lab node ──────────────────────────┐
   Render web app   │                                                               │
   (browser)        │   NGINX  ──►  [ THIS DOCKER: FastAPI + pipeline ]             │
        │           │   (other     │  reads/writes                                  │
        ├── HTTPS ──┼──► team)      ▼                                               │
        │           │        ┌─────────────────────┐     Airflow (other team)      │
   Airflow ─────────┼───────►│  shared host DATA dir│◄──── triggers via CLI/REST    │
   (other team)     │        │  (bind-mounted /data)│                               │
                    │        └──────────┬──────────┘                               │
                    │                   │ served by                                 │
                    │         file-browser service (other team)  ──► users browse  │
                    │                   │ STAC items ingested by                    │
                    │              STAC-B catalogue (other team)                    │
                    └───────────────────────────────────────────────────────────────┘
```

Key principle: **data lives outside the container.** The container is disposable
compute; all inputs and outputs are written to the bind-mounted host directory so
the file-browser, Airflow, and a future Kubernetes layer all see the same files.

---

## 3. Quick start — build, run, expose, wire to the web app

Prerequisites: Docker Desktop / Docker Engine with Compose v2 (≥ 4 GB memory
recommended — BirdNET loads TensorFlow per worker).

### 3.1 Build and run

```bash
cd cem-backend

# 1. Create the shared data dir the container writes to (bind mount).
mkdir -p data

# 2. Configure. Copy the template and set at least API_KEY.
cp .env.example .env          # Windows: copy .env.example .env
#   edit .env -> API_KEY=<long-random-string>

# 3. Build the image and start the service (standard one-liner).
docker compose up -d --build

# 4. Verify.
curl http://localhost:8000/health
#   -> {"status":"ok","api_version":"1.1.0","steps":[...]}
#   http://localhost:8000/docs  = interactive Swagger UI
```

`docker compose up -d --build` is the standard build-and-run command. To split the
steps (useful on a slow link, so build progress is visible before start):

```bash
docker compose build      # build the image only
docker compose up -d      # create + start the container
```

The image is tagged `cem-birdnet-api` (set in `docker-compose.yml`). It can also
be built and run standalone, without Compose:

```bash
docker build -t cem-birdnet-api .
docker run -d -p 8000:8000 --env-file .env -v "$PWD/data:/data" cem-birdnet-api
```

First build pulls TensorFlow + BirdNET (hundreds of MB); on a slow link this can
take a while. The Dockerfile pins the base image to `python:3.10-slim-bookworm`
and adds apt + pip retries so a flaky mirror does not abort the build. See §16 if
a build stalls.

### 3.2 Expose over HTTPS (required for the Render web app)

The web app is served over HTTPS; browsers block calls to `http://localhost`
(mixed content). Put an HTTPS tunnel / reverse proxy in front of the docker. The
quickest is ngrok:

```bash
ngrok http 8000          # copy the https://….ngrok-free.app URL it prints
```

In production the NGINX entry point replaces the tunnel. The URL from ngrok
changes on each restart unless you have a reserved domain.

### 3.3 Wire the Render web app to the server

Set these env vars on Render, then redeploy so `generate_config.sh` rebuilds
`Config.js` with the server block:

| Render env var    | Value |
|-------------------|-------|
| `SERVER_BASE_URL` | the `https://….ngrok-free.app` URL from §3.2 (trailing slash is stripped) |
| `SERVER_API_KEY`  | must equal the docker's `API_KEY` |
| `GOOGLE_CLIENT_ID`| (already set) |
| `PICKER_API_KEY`  | (already set, optional) |

### 3.4 End-to-end test

Open the Render site → **Connect to Server**. Health should go green and the step
list loads from `/steps`. Then run a BirdNET job: the app uploads audio → runs →
polls → downloads results into the Jobs dashboard.

---

## 4. Configuration (environment variables)

All knobs are env vars (read in `server/app/settings.py`); `docker-compose.yml`
passes them through with safe defaults. Edit `.env`, then `docker compose up -d`.

| Variable | Default | Purpose |
|----------|---------|---------|
| `API_KEY` | `changeme` | Value required in the `X-API-Key` header. |
| `REQUIRE_AUTH` | `true` | Set `false` only on a trusted LAN-only deployment. |
| `ALLOWED_ORIGINS` | `*` | CORS allow-list (comma-separated). Tighten to the Render URL in prod. |
| `MAX_UPLOAD_MB` | `2048` | Per-file upload cap. |
| `MAX_CONCURRENT_TASKS` | `2` | Thread-pool size for concurrent async jobs. |
| `BIRDNET_MAX_WORKERS` | `2` | BirdNET parallel processes. Each loads its own TensorFlow model (~hundreds of MB). Lower to `1` if BirdNET dies with `BrokenProcessPool`. |
| `HOST_DATA_DIR` | `./data` | Host dir bind-mounted to `/data`. Point at the shared deployment dir on the lab node (absolute path, e.g. `/srv/cem/data`). |
| `API_VERSION` | `1.1.0` | Compute API version, stamped into `/health`, `/steps`, outputs + STAC. |
| `STAC_ENABLED` | `true` | Write a STAC item sidecar per output. |
| `STAC_COLLECTION` | `cem-bioacoustics` | `collection` field on STAC items. |
| `STAC_ASSET_BASE_URL` | (blank) | If set, STAC asset `href`s become absolute under this base; blank = relative. |
| `FILE_BROWSER_BASE_URL` | (blank) | Base URL of the file-browser service. Blank = no browse link returned. |
| `FILE_BROWSER_PATH_TEMPLATE` | `{base}/{job_rel}` | Link scheme; `{job_rel}` = `jobs/<job_id>`. |
| `RETENTION_HOURS` | `168` | Delete job dirs older than this (cluster = compute, not storage). `0` disables. |
| `RETENTION_SWEEP_MINUTES` | `60` | Background sweep interval. `0` disables the sweeper. |
| `DATA_DIR` | `/data` | In-container data path (don't change; map the host side via `HOST_DATA_DIR`). |
| `PIPELINE_DIR` | `/app/pipeline` | In-container pipeline path. |
| `PYTHON_BIN` | `python` | Interpreter used to launch pipeline scripts. |
| `STACD_WORKSPACE_ID` | `registered` | Fixed job dir used by the STACD/Airflow sync API (§8). Audio is uploaded here; exempt from retention. |
| `STACD_STAC_VERSION` | `1.1.0` | STAC version emitted in `/api/v1` responses (matches the STACD catalog). |
| `STACD_ASSET_ID_PREFIX` | `cem/bioacoustics` | Prefix for the deterministic `asset_id` STACD registers per output. |

---

## 5. Data layout on disk

Everything lives under `HOST_DATA_DIR` (`/data` in the container), one directory
per job:

```
<DATA_DIR>/jobs/<job_id>/
  job.json                      # metadata + task records
  input/
    audio/                      # uploaded .wav (kind=audio)
    reference/                  # reference .wav (kind=reference) + reference_spots.json
    aggregate.csv               # seeded BirdNET aggregate (kind=aggregate)
    processed_files.txt         # seeded processed list (kind=processed)
    geo.json                    # spot lat/lon for STAC (from the job submission)
    ebird_checklist.txt / static_noise.wav / rain_noise.wav   # optional overrides
  work/
    aggregate.csv               # BirdNET aggregate this job produced (seeded + new)
    output.csv                  # BirdNET filtered output for the requested range
    processed_files.txt         # merged processed list (returned to the caller)
    *.stac.json                 # STAC item per work output
  results/<step>/
    <plots/CSVs>                # analysis outputs
    <file>.stac.json            # STAC item per result
    _run.log                    # combined stdout/stderr for that step
```

Because this is a host bind mount, the file-browser service and any other cluster
node read these same files directly.

---

## 6. Compute steps & parameters

Seven steps (`pipeline/manifest.json`). `birdnet` turns raw audio into the
detections aggregate; the six analyses consume that aggregate.

| Step id | Name | Needs |
|---------|------|-------|
| `birdnet` | BirdNET Species Detection | audio (kind=audio) |
| `heatmaps` | Species Activity Heatmaps | aggregate |
| `temporal_stickiness` | Activity Regularity (Temporal) | aggregate |
| `spatial_stickiness` | Habitat Affinity (Spatial) | aggregate (≥ 2 spots) |
| `migratory_classification` | Migratory vs Resident | aggregate |
| `solar_correlation` | Solar Event Correlation | aggregate |
| `daily_timeseries` | Daily Call Time Series | aggregate |

**Run parameters** (JSON body on `run` endpoints; all optional):

| Field | Type | Meaning |
|-------|------|---------|
| `spots` | `string[]` | Spot names to keep (empty/omitted = all). |
| `start_date` | `string` | Inclusive, `YYYY-MM-DD` or `YYYYMMDD`. |
| `end_date` | `string` | Inclusive, same format. |
| `snr_db` | `number` | BirdNET denoise SNR (dB). `birdnet` only. |
| `min_confidence` | `number` | BirdNET min detection confidence, 0–1 (default 0.25). `birdnet` only. |
| `top_n_species` | `number` | Top N species to plot (default 25). `heatmaps` only. |
| `top_n_temporal` | `number` | Top N species to plot (default 80). `temporal_stickiness` only. |
| `sci_threshold` | `number` | Seasonal Concentration Index threshold (default 0.9). `migratory_classification` only. |
| `kurtosis_threshold` | `number` | Residual kurtosis threshold (default 15). `migratory_classification` only. |
| `pmr_threshold` | `number` | Peak-to-median ratio threshold (default 50). `migratory_classification` only. |
| `window_size` | `number` | Rolling window size, days (default 60). `migratory_classification` only. |
| `min_solar_days` | `number` | Min days with >10 detections to include a species (default 5). `solar_correlation` only. |
| `max_timeseries_species` | `number` | Max species to plot (default 50). `daily_timeseries` only. |
| `spots_geo` | `[{name,lat,lon}]` | Spot geolocation for STAC geometry/bbox. Stored on the job, not passed to the script. |

Each tunable is read by exactly one step; a step ignores tunables that aren't its
own. Defaults live in `pipeline/config.py` and are pre-filled in
`pipeline/manifest.json`, so omitting a field keeps the documented default. The
manifest's per-step `parameters[]` is the contract the web app renders as the
input form.

---

## 7. REST API reference (async `/jobs`)

Base URL = the docker origin (e.g. the ngrok/NGINX URL). Auth = header
`X-API-Key: <API_KEY>` on every endpoint except `/health`. The web app also sends
`ngrok-skip-browser-warning: true` (harmless on non-ngrok backends).

This is the **asynchronous** API: a `run` call returns a `task_id` immediately and
the caller polls task status. (The synchronous STACD/Airflow API is §8.)

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/health` | Liveness (no auth). Returns `status`, `api_version`, step ids. |
| GET | `/steps` | Step catalogue with names, descriptions, `is_analysis`, `depends_on`, `version`, `api_version`. |
| POST | `/jobs` | Create an empty job → `{job_id, created_at}`. |
| GET | `/jobs` | List job ids. |
| GET | `/jobs/{id}` | Job summary: inputs, tasks, results, `browse_url`, `api_version`. |
| POST | `/upload` | Create a job **and** upload files in one call. Form fields: `kind`, `spot`, `files`. |
| POST | `/jobs/{id}/upload` | Upload files to an existing job. Form fields: `kind`, `spot`, `files`. |
| POST | `/jobs/{id}/run/{step}` | Run one step (async). JSON body = run parameters. Returns `task_id`. Named routes exist for each step plus this generic fallback. |
| POST | `/jobs/{id}/run-all` | Run birdnet (if audio present) then all six analyses in order. |
| GET | `/jobs/{id}/tasks/{task_id}` | Task status: `queued \| running \| success \| failed`. |
| GET | `/jobs/{id}/tasks/{task_id}/log` | Plain-text run log. |
| GET | `/jobs/{id}/results` | Result file list + `browse_url` + `api_version`. |
| GET | `/jobs/{id}/stac` | STAC item sidecars produced for the job. |
| GET | `/jobs/{id}/download` | Zip of all result files. |
| GET | `/jobs/{id}/download/{step}` | Zip of one step's results. |
| GET | `/jobs/{id}/file?path=<rel>` | Download a single result file by its job-relative path. |

**Upload `kind` values:** `audio`, `reference`, `aggregate`, `processed`,
`ebird`, `static_noise`, `rain_noise`. (`reference` also takes a `spot` form
field; `aggregate`/`processed`/`ebird`/`static_noise`/`rain_noise` each save to a
fixed filename and overwrite on re-upload.)

### Worked example (curl)

```bash
SERVER=https://abc123.ngrok-free.app
KEY=your-api-key
H="-H X-API-Key:$KEY -H ngrok-skip-browser-warning:true"

# create a job
JOB=$(curl -s $H -X POST $SERVER/jobs | python -c "import sys,json;print(json.load(sys.stdin)['job_id'])")

# upload audio
curl -s $H -F kind=audio -F files=@SPOTA_20251105_060000.wav $SERVER/jobs/$JOB/upload

# run birdnet with geo (for STAC) + a date range
curl -s $H -H "Content-Type: application/json" \
  -d '{"start_date":"20251101","end_date":"20251231","spots_geo":[{"name":"SPOTA","lat":28.5635,"lon":77.1897}]}' \
  -X POST $SERVER/jobs/$JOB/run/birdnet

# poll the task, then list + download results
curl -s $H $SERVER/jobs/$JOB/tasks/<task_id>
curl -s $H $SERVER/jobs/$JOB/results
curl -s $H "$SERVER/jobs/$JOB/file?path=work/aggregate.csv" -o aggregate.csv
```

---

## 8. STACD / Airflow synchronous algorithm API (`/api/v1`)

This is the path the professor's **STACD** framework (Airflow + YAML-generated
DAGs) uses. It is **additive** — the async `/jobs` API (§7) is unchanged and still
used by the web app directly.

**Two planes:** this docker is the **data plane** (upload/download) and a compute
node; STACD/Airflow is the **control plane** (orchestration, lineage, catalog).
Airflow moves no bulk data — it only calls compute.

**Trigger flow (confirmed model):**

```
Render web app ── uploads audio ──► POST /api/v1/datasets/audio   (registered dir)
Render web app ── triggers DAG ───► Airflow REST  (returns 200 immediately)
   Airflow DAG node ── calls ──────► POST /api/v1/<algo>   (BLOCKS until done)
        this docker runs the step, writes to the shared dir, returns:
            200 + { asset_id, stac[] }   ──► STACD registers asset + exports STAC
   web app learns completion by polling Airflow's dagRun status / the catalog
        (NOT by polling this docker — that's the local-watcher model).
```

**Completion = the HTTP response.** Unlike the async `/jobs` API (which returns a
`task_id` to poll), each `/api/v1/<algo>` call is **synchronous**: it runs the
step to completion and the response code is the result:

| Response | Airflow task | Meaning |
|----------|--------------|---------|
| `200` + body | success | asset produced + registered + STAC exported |
| `400` | skipped | bad input parameters (e.g. bad date) |
| `404` | skipped | no data (no audio uploaded, empty aggregate, no detections) |
| `500` | failed | pipeline/computation error |

**Registered audio dataset.** Audio is **not** passed by Airflow. The web app
always uploads WAVs into one fixed workspace dir
(`<DATA_DIR>/jobs/<STACD_WORKSPACE_ID>/input/audio/`) via
`POST /api/v1/datasets/audio`; BirdNET reads it and accumulates the aggregate
there across DAG runs. This workspace is exempt from the retention sweeper.

**Endpoints:** one upload endpoint plus **seven explicit named algorithm
wrappers** — one per DAG node — and a generic fallback. Each wrapper is
synchronous (blocks until the step finishes and returns the asset response, not a
`task_id`) and is a thin wrapper over a shared core. The algorithm-repo YAML
points each DAG node at the matching named URL.

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/api/v1/datasets/audio` | Upload WAVs into the registered audio dir (multipart `files`). |
| POST | `/api/v1/birdnet` | BirdNET species detection. Reads `snr_db`, `min_confidence`. Needs registered audio. |
| POST | `/api/v1/heatmaps` | Species activity heatmaps. Reads `top_n_species`. Needs the aggregate. |
| POST | `/api/v1/temporal_stickiness` | Temporal activity correlation. Reads `top_n_temporal`. Needs the aggregate. |
| POST | `/api/v1/spatial_stickiness` | Spatial distribution correlation (≥ 2 spots). Needs the aggregate. |
| POST | `/api/v1/migratory_classification` | Migratory vs resident. Reads `sci_threshold`, `kurtosis_threshold`, `pmr_threshold`, `window_size`. Needs the aggregate. |
| POST | `/api/v1/solar_correlation` | Peak-activity vs sunrise/sunset. Reads `min_solar_days`. Needs the aggregate. |
| POST | `/api/v1/daily_timeseries` | Per-species daily call-count series. Reads `max_timeseries_species`. Needs the aggregate. |
| POST | `/api/v1/{algo}` | Generic fallback (registered last; the seven named routes match first). Runs any valid `algo` id. |

Every algorithm endpoint takes the same optional JSON body — `{spots, start_date,
end_date, snr_db, spots_geo}` plus the per-step tunables from §6 — and each algo
applies only the tunables it reads. The interactive Swagger UI (`/docs`) shows
each of the seven with success/skip/fail example payloads.

**Success response** (STAC 1.1.0). One output → scalar `asset_id` + object `stac`;
several outputs → `asset_id`/`asset_ids` arrays + `stac` array:

```json
{
  "status": "completed",
  "Success": "Solar Event Correlation completed",
  "message": "Solar Event Correlation completed",
  "task_id": "f33b9ab7b5a24650821da8e41107b696",
  "asset_id": "cem/bioacoustics/solar_correlation/20251101_20251231/solar_summary.csv",
  "stac": { "type": "Feature", "stac_version": "1.1.0", "geometry": {"type":"Polygon", "...": "..."},
            "bbox": ["..."], "properties": { "cem:algorithm_version": "1.0.0",
            "cem:api_version": "1.1.0", "...": "..." }, "assets": {"data": {"href": "...", "roles":["data"]}},
            "collection": "cem-bioacoustics" }
}
```

Failure body: `{ "status": "skipped"|"failed", "error": "...", "message": "...", "task_id": "..." }`.

**Registering with STACD.** Three YAMLs are provided in `stacd/` — upload them via
the STACD dashboard:

- `cem_bioacoustics_dag.yaml` — the workflow graph (birdnet → six analyses).
- `cem_bioacoustics_algorithm_repo.yaml` — each algorithm's API URL
  (`/api/v1/<algo>`). **Replace the host** with the docker's reachable address
  (the NGINX route, or the ngrok URL while testing).
- `cem_bioacoustics_dataset_repo.yaml` — the registered audio root dataset.

---

## 9. Two compute modes & the watcher/server data split

The web app has two compute backends that run the **same pipeline scripts**:

- **Local (watcher):** `watcher.py` runs on the user's machine, executes the
  scripts against on-disk audio.
- **Server (cluster):** the browser uploads audio to this docker and runs the
  pipeline on the lab node.

The two modes keep **separate** master files locally so neither overwrites the
other (the compute scripts are byte-identical between modes, so results match
given the same inputs):

| | Watcher | Server |
|---|---|---|
| BirdNET aggregate | `system/database/birdnet_results_watcher.csv` | `system/database/birdnet_results_server.csv` |
| Processed list | `processed_<script>_watcher.txt` | `processed_<script>_server.txt` |

The **server stays stateless** per job. The `_server` master files live on the
user's machine; on each server BirdNET run the web app ships them to the docker
(`kind=aggregate` + `kind=processed`), BirdNET appends new detections and skips
already-processed files, and the merged results are saved back to the `_server`
files. This gives the server the same dedup behaviour as the watcher without any
server-side persistence. The spot label is derived from the filename
(`SPOT_YYYYMMDD_HHMMSS.wav`) in both modes.

> On first watcher run after this split, a legacy un-suffixed
> `birdnet_results.csv` / `processed_*.txt` is migrated once into the `_watcher`
> file so history is preserved.

---

## 10. Provenance: STAC items & versioning

Every produced result (CSV/PNG) gets a sibling STAC Item JSON,
`<file>.stac.json`. The async `/jobs` sidecars are **STAC 1.0.0**; the synchronous
`/api/v1` responses (§8) emit **STAC 1.1.0** to match the STACD catalog. Each item
records: a stable asset id, datetime, a short description (from the manifest), the
algorithm version and compute API version, the run parameters, and the spot
geolocation as `geometry`/`bbox` (Point for one spot, MultiPoint for several;
`/api/v1` promotes a multi-spot bbox to a Polygon) from the `spots_geo` passed on
the job.

CEM-specific fields are namespaced under `cem:` so they don't collide with
whatever extension schema STAC-B settles on. An async sidecar looks like:

```json
{
  "type": "Feature", "stac_version": "1.0.0", "stac_extensions": [],
  "id": "job_3f9a2b1c7d4e_heatmaps_heatmap_SPOTA.csv",
  "collection": "cem-bioacoustics",
  "geometry": {"type": "Point", "coordinates": [77.1897, 28.5635]},
  "bbox": [77.1897, 28.5635, 77.1897, 28.5635],
  "properties": {
    "datetime": "2026-06-11T20:35:55Z",
    "created": "2026-06-11T20:35:55Z",
    "description": "Species Activity Heatmaps — heatmap_SPOTA.csv ...",
    "processing:software": {"cem-bioacoustics-api": "1.1.0"},
    "cem:job_id": "job_3f9a2b1c7d4e",
    "cem:step": "heatmaps",
    "cem:algorithm": "activity_heatmaps.py",
    "cem:algorithm_version": "1.0.0",
    "cem:api_version": "1.1.0",
    "cem:parameters": {"spots": ["SPOTA"], "start_date": "20251101", "end_date": "20251231"},
    "cem:spots": [{"name": "SPOTA", "lat": 28.5635, "lon": 77.1897}]
  },
  "assets": {"data": {"href": "heatmap_SPOTA.csv", "type": "text/csv", "title": "heatmap_SPOTA.csv", "roles": ["data"]}},
  "links": []
}
```

**Versioning (one source of truth):** `API_VERSION` (env) is the compute API
version; each step's `version` lives in `pipeline/manifest.json`. Both appear in
`/health`, `/steps`, and every STAC item — bump them when behaviour changes. STAC
generation is best-effort: a sidecar failure never fails the compute job, and
`STAC_ENABLED=false` skips it entirely.

---

## 11. File-browser link

After a job, `GET /jobs/{id}/results` and `GET /jobs/{id}` return a `browse_url`
pointing at that job's output directory, and the web app surfaces it in the Jobs
dashboard ("Open output folder in file browser"). **The link only appears when
`FILE_BROWSER_BASE_URL` is set** — the file-browser service itself is another
team's piece. With it blank, `browse_url` is `null` and no link shows (by design).

Demo it locally by pointing any file browser at the same shared data dir:

```bash
docker run -d --name cem-files -p 8081:80 \
  -v /absolute/path/to/cem-backend/data:/srv \
  filebrowser/filebrowser
# .env:  FILE_BROWSER_BASE_URL=http://localhost:8081/files
```

The resulting link is `<FILE_BROWSER_BASE_URL>/jobs/<job_id>`; change the scheme
with `FILE_BROWSER_PATH_TEMPLATE` (`{base}`, `{job_rel}`) to match the team's
service.

---

## 12. Headless / Airflow triggering (CLI)

Compute is invokable without the web app via an in-container CLI that drives the
same job store and runner against the shared data dir, so Airflow (or any
orchestrator) can trigger jobs on a user's behalf. The REST API (§7) and the
synchronous STACD API (§8) remain the other supported triggers.

```bash
docker compose exec api python -m app.cli ingest \
    --audio /data/incoming/spotA --step all --wait \
    --spots SPOTA --start 20251101 --end 20251231 --geo /data/incoming/geo.json
```

Subcommands: `create-job`, `upload` (`--kind audio|reference|aggregate|processed|ebird|static_noise|rain_noise`),
`run`, `run-all`, `ingest`, `status`, `cleanup`. `--wait` blocks until tasks
finish and the process exits non-zero if any task fails, so an Airflow
`DockerOperator`/`BashOperator` can gate on it. Run parameters and per-step
tunables map to flags (`--spots`, `--start`, `--end`, `--snr`, `--min-confidence`,
`--top-n-species`, etc.). See `python -m app.cli --help`.

---

## 13. Output retention

The cluster is compute, not long-term storage. A background sweeper (started on
app startup) deletes job directories whose newest file mtime is older than
`RETENTION_HOURS` (default 7 days), running every `RETENTION_SWEEP_MINUTES`. Age
is the most-recent mtime across the job dir, so a job still being written or
downloaded is not reaped mid-flight. The STACD registered workspace
(`STACD_WORKSPACE_ID`) is never swept. Set `RETENTION_HOURS=0` to disable. Force a
sweep on demand:

```bash
docker compose exec api python -m app.cli cleanup --hours 168
```

Long-term copies are expected to live in the shared data dir / STAC catalogue via
the other teams' services, not in this compute box.

---

## 14. Build / update / add-a-library

The container has three layers; only one needs an image rebuild:

| Change | Lives in | Action |
|--------|----------|--------|
| Dependencies (TensorFlow, birdnetlib, FastAPI…) | baked into the image | **rebuild** |
| Code (`pipeline/`, `server/app/`) | host, bind-mounted | **restart only** |
| Config / env (`.env`) | host | **recreate** (`up -d`) |
| Data (uploads, results, STAC) | host `HOST_DATA_DIR` | nothing (live) |

The image still `COPY`s the code at build time, so it runs standalone without the
mounts (e.g. a plain `docker run`). In Compose, the bind mounts overlay that baked
copy with the live host code — so day-to-day script fixes never touch the image.
`pipeline/manifest.json` is re-read per request, so step metadata/version edits
show up without even a restart; a restart is only needed for imported Python
modules.

Standard commands for each kind of change:

```bash
# edited a .py / manifest version ............ docker compose restart api
# added/upgraded a pip package ............... docker compose up -d --build
# changed .env (keys, URLs, retention) ....... docker compose up -d
# changed code AND deps ...................... docker compose up -d --build
```

To add a library: add it to `requirements.txt` (pipeline runtime deps) or
`server/requirements-server.txt` (API deps), then `docker compose up -d --build`.
Only the dependency layers rebuild if you changed only a requirements file (Docker
layer cache); if a build stalls on a slow link, re-run it — completed layers are
cached (see §16).

Other useful standard commands: `docker compose logs -f api` (follow logs),
`docker compose ps` (status), `docker compose down` (stop + remove the container).

---

## 15. Integration points for other teams

This docker is built to plug into the cluster without further code changes — each
external contract is configurable, not hardcoded.

| Team / piece | How this docker plugs in |
|--------------|--------------------------|
| **NGINX entry point** | The service listens on `:8000` and is path-agnostic; route to it by URL. Auth is a header (`X-API-Key`), CORS is configurable (`ALLOWED_ORIGINS`). Map a unique external port on the node if `:8000` clashes. |
| **Airflow orchestrator (STACD)** | Each DAG node calls the synchronous `POST /api/v1/<algo>` endpoint (§8), which blocks and returns `200 + {asset_id, stac}`. Three ready-to-load YAMLs in `stacd/`. The CLI (§12) is an alternative headless trigger. |
| **File-browser service** | Set `FILE_BROWSER_BASE_URL` (+ optional `FILE_BROWSER_PATH_TEMPLATE`); the API returns `browse_url` per job. Point the browser at the shared `HOST_DATA_DIR`. |
| **STAC-B catalogue** | One STAC Item JSON per output (`*.stac.json`) in the shared dir; CEM fields under `cem:`. Confirm exact field names with this team and adjust `server/app/stac.py` / `STAC_COLLECTION` if needed. |
| **Central user management / SSO** | Not handled here; front with NGINX/SSO. The static API key is for the current lab tool and can be rotated freely. |

---

## 16. Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| Web app can't reach server, CORS / mixed-content error | Web app is HTTPS; expose the docker over HTTPS (ngrok/NGINX). Check `ALLOWED_ORIGINS`. |
| BirdNET dies with `BrokenProcessPool` | Out of memory. Lower `BIRDNET_MAX_WORKERS` to `1` and/or raise Docker memory ≥ 4 GB. |
| Build fails: `rpc error … EOF` / slow apt | Network, not code. Re-run `docker compose build` (completed layers are cached); pre-pull the base with `docker pull python:3.10-slim-bookworm`; give Docker ≥ 4 GB; use a stable connection. Restart Docker Desktop if a wedged WSL2 network keeps failing apt. |
| No "Open output folder" link | `FILE_BROWSER_BASE_URL` is blank and/or no file-browser service is running (§11). |
| `api_version` shows old value after a code edit | Container not recreated; `docker compose restart api` (or `up -d`). |
| Analysis step fails "No aggregate available" | Run BirdNET first, or upload an aggregate (`kind=aggregate`). |
| Duplicate detection rows on re-run | Ensure the web app ships the `_server` processed list (`kind=processed`) before BirdNET so already-processed files are skipped. |

---

## 17. Assumptions & ownership boundaries

- **This repo = bioacoustics compute docker only.** NGINX, Airflow, the
  file-browser service, STAC-B ingest, and user management are other teams'.
- **STAC field shape** follows valid STAC (1.0.0 async sidecars, 1.1.0 sync) with
  CEM fields under `cem:`; confirm the exact schema with the drone/STAC-B team.
  Hooks: `STAC_COLLECTION`, `STAC_ASSET_BASE_URL`.
- **File-browser URL scheme** assumed `<base>/jobs/<job_id>`; override with
  `FILE_BROWSER_PATH_TEMPLATE`.
- **Airflow trigger** is the synchronous `/api/v1` API plus the CLI; no DAG is
  included (the orchestrator is the other team's). The `stacd/` YAMLs register the
  algorithms/dataset.
- **Data migration:** moving `/data` from a Docker named volume to a host bind
  mount means old volume data isn't auto-carried — point `HOST_DATA_DIR` at the
  shared dir and copy any prior data across once if needed.
- **Spot labels** come from filenames (`SPOT_YYYYMMDD_HHMMSS.wav`) in both compute
  modes; no per-file spot is sent from the client.

---

*Compute API version 1.1.0. Steps: birdnet + 6 analyses. Generated against the
current `server/app/`, `Dockerfile`, `docker-compose.yml`, and
`pipeline/manifest.json` — keep this file in sync when those change.*
