"""
Thin Airflow REST client used by the /analyze dispatch endpoint.

Two calls only:
  - trigger_dag(conf)      -> POST  /api/v1/dags/<dag>/dagRuns   (returns dag_run_id)
  - get_dag_run(run_id)    -> GET   /api/v1/dags/<dag>/dagRuns/<run_id>  (state)

Airflow is OPTIONAL. ``is_configured()`` is false when AIRFLOW_BASE_URL is blank,
in which case the dispatcher runs the pipeline locally instead.

Both calls use short timeouts: dispatch fires the trigger and returns; the poll
endpoint queries run state only as a fallback. Neither blocks for the DAG's
duration, so no long-lived connections are held.
"""
import httpx

from .settings import get_settings


def is_configured() -> bool:
    return get_settings().airflow_enabled


def _auth() -> tuple[str, str]:
    s = get_settings()
    return (s.AIRFLOW_USERNAME, s.AIRFLOW_PASSWORD)


def _dagruns_url() -> str:
    s = get_settings()
    return f"{s.AIRFLOW_BASE_URL}/api/v1/dags/{s.AIRFLOW_DAG_ID}/dagRuns"


def trigger_dag(conf: dict) -> dict:
    """Trigger a DAG run. Returns the Airflow dagRun JSON (incl. dag_run_id, state).
    Raises httpx.HTTPError on transport failure or non-2xx response."""
    s = get_settings()
    r = httpx.post(_dagruns_url(), json={"conf": conf}, auth=_auth(), timeout=s.AIRFLOW_TIMEOUT)
    r.raise_for_status()
    return r.json()


def get_dag_run(dag_run_id: str) -> dict:
    """Fetch a dagRun's current state. Raises httpx.HTTPError on failure."""
    s = get_settings()
    url = f"{_dagruns_url()}/{dag_run_id}"
    r = httpx.get(url, auth=_auth(), timeout=s.AIRFLOW_TIMEOUT)
    r.raise_for_status()
    return r.json()
