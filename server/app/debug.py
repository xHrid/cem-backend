"""Opt-in diagnostics. Enable with DEBUG=true before starting the process."""
import json
import logging
import os
from time import perf_counter
from uuid import uuid4

DEBUG = os.getenv("DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
_logger = logging.getLogger("cem.compute")
_logger.setLevel(logging.DEBUG if DEBUG else logging.WARNING)
_logger.propagate = False
if not _logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(message)s"))
    _logger.addHandler(handler)


def debug(event: str, **fields) -> None:
    if DEBUG:
        _logger.debug("%s %s", event, json.dumps(fields, default=str, sort_keys=True))


class DebugRequests:
    """Observe ASGI status/timing without reading bodies or buffering audio."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if not DEBUG or scope["type"] != "http":
            return await self.app(scope, receive, send)
        request_id = uuid4().hex[:12]
        started = perf_counter()
        status = 500
        debug("http.start", request_id=request_id, method=scope["method"])

        async def observe(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, observe)
        except Exception as exc:
            debug("http.error", request_id=request_id, error=type(exc).__name__)
            raise
        finally:
            route = getattr(scope.get("route"), "path", "<unmatched>")
            debug("http.finish", request_id=request_id, method=scope["method"],
                  route=route, status=status,
                  elapsed_ms=round((perf_counter() - started) * 1000, 1))

