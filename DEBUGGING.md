# Debug Logging

Set `DEBUG=true` in this repo's `.env`, then run:

```sh
docker compose up -d --build
docker compose logs -f api
```

This enables the compute API, pipeline subprocesses, and compute frontend.
Use `DEBUG=false` (the default) and run `docker compose up -d` to disable it.
Compose must recreate containers after an env change; `restart` alone does not apply it.
Accepted true values: `true`, `1`, `yes`, `on` (case-insensitive).

API logs include request IDs, route templates, status, timing, uploads, and job outcomes.
Pipeline debug messages appear in each task's `results/<step>/_run.log`, available
through the existing job log endpoint. Snippet events cover candidate counts,
missing sources, reused clips, extraction windows, and generated files.
Normal operational output and errors are unchanged.

Backend helpers: `server/app/debug.py`; standalone pipeline helper: `pipeline/debug_log.py`.
Use `debug("event.name", count=value)`, keeping fields to identifiers and counts.
Request logging excludes headers, bodies, query strings, and credential values.
