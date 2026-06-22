# CEM Backend — Code Review + Airflow Architecture Change

_Review only. Nothing implemented. Two parts: (1) findings list, (2) Airflow dispatch redesign plan._

---

## Part 1 — Review findings

Grouped by severity. "File:line" points at the spot.

### A. Bugs / broken paths (fix before prod)

1. **Auth is dead. All endpoints open.**
   `auth.require_api_key` defined (`auth.py:10`) but never attached as a `Depends` on the router or in `main.py`. `REQUIRE_AUTH` defaults `true` but nothing enforces it → every route public. (You said ignore auth/multi-user — noting anyway because the env implies it's on.)

2. **`cli.py` reference upload crashes.**
   `cli.py:54` calls `job.set_reference_spot(...)`. No such method on `Job` (`jobs.py`). Any `upload --kind reference` → `AttributeError`. Dead/broken since `get_reference_spots()` always returns `{}` anyway (`jobs.py:139`).

3. **`schemas.py` fully dead.**
   Not imported anywhere. `/scripts` takes raw `body: dict` (`stacd_api.py:284`), so the Pydantic models + the docstring promise "Airflow can never omit required fields" are false. Zero request validation. Either wire the models in or delete the file.

4. **`/scripts` blocks but docs/runner claim polling.**
   `stacd_api.py:232` uses `runner.run_sync` (blocks calling thread to completion). But `runner.py:1-7` docstring says "async model: returns task_id immediately and caller polls." Contradiction. Matters for Part 2.

5. **Client-supplied `job_id` clobbers existing job.**
   `create_job(... job_id=client_job_id)` (`jobs.py:229`) re-writes `job.json` with empty `tasks` even if the id already exists. Reused id → prior tasks/results wiped. Critical for Part 2 (dispatch + airflow-callback share one id).

### B. Concurrency / resource (matters at prod load)

6. **`MAX_CONCURRENT_TASKS` does not bound `/scripts`.**
   The `_POOL` (`runner.py:25`) only governs `submit`/`submit_all` (CLI). The HTTP `/scripts` path calls `run_sync` → runs on the request thread, ignores the pool. N concurrent requests = N concurrent BirdNET subprocesses (each loads TF, hundreds of MB) → OOM. No semaphore guards the synchronous path.

7. **Settings frozen at import; `lru_cache` misleading.**
   `Settings` reads env into **class attributes** at import (`settings.py:16-38`). `get_settings()` re-instantiation can't pick up changed env; `@lru_cache` is redundant. `runner.py:24` and `_POOL` size are captured once at import — env/Compose changes need full restart, not obvious.

8. **`main.py` uses deprecated `@app.on_event("startup")`** (`main.py:33`). Replace with lifespan handler before pinning a newer FastAPI.

### C. Fragile logic

9. **`_classify_error` is substring-matching on free text** (`stacd_api.py:167`). Maps `"no audio"`,`"bad date"`… to HTTP codes. Any pipeline message reword silently flips 404↔500. Should be structured error codes from the runner, not English matching.

10. **`/scripts` response shape is non-uniform** (`stacd_api.py:273-279`). Single result → `asset_id` is a string + `stac` object; multiple → `asset_id` is a list + extra `asset_ids` + `stac` array. Forces the frontend to branch. Pick one shape (always arrays).

11. **STAC failures swallowed silently** (`runner.py:237-238`, `except Exception: pass`). A broken sidecar write leaves results looking clean with no log. At least log it.

12. **`get_job` route uses `meta_d["created_at"]` hard key** (`stacd_api.py:305`) while everything else uses `.get`. Malformed `job.json` → 500.

13. **`audio_spots` body param is dead** (`schemas.py:54`). `_extract_script_params` passes it into `run_params` but the runner never reads it (birdnet uses `job.get_audio_spots()` set by `populate_job`). Silently dropped.

### D. Performance / cleanup

14. **Retention `_newest_mtime` rglobs every file of every job, every sweep** (`retention.py:17`). O(total files) each pass. Use `job.json` mtime or `created_at` instead; rglob only as tiebreak.

15. **Dead/legacy surface:** `get_reference_spots()` hardwired `{}` (`jobs.py:139`) + all `reference_dir` handling in `runner.py:107-112,144-150` and `cli.py` is inert for CEM. `JobSummary.browse_url`, several `schemas.py` response models, unused. Trim.

16. **Doc drift (high-risk pre-launch).** `docs/CEM_BIOACOUSTICS_DOCKER.md` §8 describes `/api/v1/datasets/audio`, an async `/jobs` API, and "seven named algorithm wrappers" — **none exist** in current `stacd_api.py` (it's unified `/scripts` + project uploads). `main.py` top docstring references the same ghost routes. Stale docs will mislead the Airflow/integration team.

17. **`download_file` path-escape check** (`stacd_api.py:377`) compares `target.resolve()` against `job.root.resolve()`. Inputs are symlinks to the project dir (outside job.root), so resolving them would fail the check — fine for results, but if anyone points `/file` at `input/audio/...` it 400s confusingly. Note, low priority.

### E. Architectural improvements (bigger swings)

- **Make the run path async + bounded** behind a job/task state machine; have `/scripts` enqueue and return `job_id`, poll via `GET /jobs/{id}`. (Required by Part 2 anyway.) Use a real queue/semaphore so `MAX_CONCURRENT_TASKS` actually caps subprocesses.
- **Structured task status** (`queued|running|success|failed|skipped` + machine error code) instead of inferring from strings + HTTP codes.
- **Single source of truth for config** — instance-level settings read at startup, injected, so tests + env changes behave.
- **Idempotent job creation** — `create_job` should refuse-or-attach when id exists, not overwrite.
- **Validate input** — wire `schemas.py` (or delete it) so the public POST has a typed contract.

---

## Part 2 — Airflow dispatch architecture change

### Current flow (as-is)

```
Frontend ── upload/poll/download ─────────────► Server   (direct)
Frontend ── analysis (if airflow configured) ─► Airflow ── calls ──► Server /scripts (runs, blocks)
Frontend ── polls GET /jobs/{job_id} ─────────► Server
```

Frontend owns the "airflow vs direct" decision. It pre-mints `job_id`, hands it to Airflow, Airflow calls `/scripts` with that id, frontend polls the same id.

### Target flow (to-be)

```
Frontend ── ALWAYS ──► Server POST /scripts
                          │
                          ├─ airflow configured?  ── yes ─► call Airflow REST (fire), return job_id now
                          │                                     │
                          │                          Airflow ── calls back ─► Server (EXECUTE mode) ─► runs pipeline
                          │
                          └─ no ─► run locally (background), return job_id now
Frontend ── polls GET /jobs/{job_id} ─────────► Server   (only the server, always)
```

Frontend stops knowing about Airflow entirely. Server decides. Polling target never changes.

### The core problem: same endpoint, two roles

`/scripts` now plays **dispatcher** (frontend call → maybe forward to Airflow) AND **executor** (Airflow calls back → must actually run). Must not re-dispatch on the callback or you get an infinite Airflow→Server→Airflow loop.

**Resolve with an explicit mode flag, not host-sniffing.** Two clean options:

- **Option A (recommended): separate internal execute route.**
  - `POST /api/v1/scripts` = dispatcher. Creates job, returns `job_id` immediately, never runs inline.
  - `POST /api/v1/scripts/execute` (or `/internal/run`) = executor. Airflow's DAG node and the local-fallback path both call this. Runs the pipeline. Not meant for the frontend.
  - Clear, greppable, no magic param leaking to clients.

- **Option B: one route + `execute` flag in body.**
  - Dispatcher call has no flag → forwards/forks. Airflow callback includes `execute: true` → runs.
  - Fewer routes but the flag is load-bearing and easy to misuse; reject `execute:true` from non-internal callers once auth exists.

### Behavioural change: dispatch must be non-blocking

Today `/scripts` blocks (`run_sync`). New model polls, so dispatch returns right away and work happens in background. Concretely:

1. **Dispatcher** (`_run_script` rewrite):
   - Validate, `create_job` (idempotent — see finding #5), `populate_job`, set geo.
   - Add a `queued` task record up front so the first poll sees the job.
   - If `settings.AIRFLOW_API_URL` set → fire Airflow REST trigger (DAG run) with `{script, project, spots, dates, job_id, ...params}`. Don't wait for completion.
   - Else → submit local background run (reuse the `_POOL`/`submit` async path, not `run_sync`).
   - Return `{job_id, status:"queued"}` immediately.

2. **Executor** (new route / flag): exactly today's `run_sync` body — run the step, write results + STAC, update task status. This is what Airflow hits. It may stay blocking (Airflow's DAG node wants the synchronous completion signal); its result updates `job.json` which the frontend is polling.

3. **Polling** stays `GET /jobs/{job_id}` — already returns `tasks[].status` + `results`. Frontend waits for the task to reach `success`/`failed`. (Add an explicit top-level `status` so the frontend doesn't reduce the tasks array itself.)

### New config (settings.py + .env.example)

```
AIRFLOW_API_URL=            # blank = run locally. Set = dispatch via Airflow.
AIRFLOW_DAG_ID=cem_pipeline
AIRFLOW_AUTH=               # token/basic for Airflow REST (separate concern)
AIRFLOW_TRIGGER_TIMEOUT=10  # seconds, just for the fire call
```

Decision is purely "is `AIRFLOW_API_URL` set" — matches your "if configured" rule.

### STACD YAML impact

`stacd/cem_pipeline_algorithm_repo.yaml` currently points the Airflow node at `…/api/v1/scripts`. Repoint the DAG node to the **executor** route (`…/api/v1/scripts/execute`), so Airflow's callback runs instead of re-dispatching. The frontend-facing `/scripts` stays the dispatcher.

### Edge cases to nail down (prod)

- **Loop guard:** executor must never forward to Airflow, regardless of config. Enforce by route separation (Option A).
- **Idempotent job_id:** dispatcher creates the job; Airflow callback reuses the same id and must **attach a task, not recreate** the job (fixes finding #5). Decide who creates the job — recommend dispatcher creates, executor only adds/runs the task.
- **Airflow trigger failure:** if the REST fire fails (Airflow down), either mark job `failed` immediately or fall back to local run. Pick one policy explicitly.
- **Double execution:** ensure only the executor runs the pipeline; dispatcher never does. Otherwise the step runs twice.
- **Concurrency:** with executor still synchronous + multiple Airflow nodes calling in, add the subprocess semaphore (finding #6) or the lab node OOMs.
- **Status semantics:** define the job's `status` while Airflow is mid-flight (`queued`→`running`→…) so a poll between dispatch and callback isn't ambiguous.

### Minimal change set

- `settings.py`: add `AIRFLOW_*`.
- `stacd_api.py`: split `_run_script` into `dispatch` (new `/scripts` body) + `execute` (new route). Add Airflow REST client call.
- `runner.py`: dispatcher uses async `submit`; keep `run_sync` for the executor route.
- `jobs.py`: make `create_job` idempotent for reused ids.
- `stacd/*.yaml`: repoint DAG node to executor route.
- `.env.example` + docs: document the new flow (and fix the §8 drift while there).
