"""Standalone pipeline diagnostics, inherited from the server's DEBUG environment."""
import json
import logging
import os

DEBUG = os.getenv("DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
_logger = logging.getLogger("cem.pipeline")
_logger.setLevel(logging.DEBUG if DEBUG else logging.WARNING)
_logger.propagate = False
if not _logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(message)s"))
    _logger.addHandler(handler)


def debug(event: str, **fields) -> None:
    if DEBUG:
        _logger.debug("%s %s", event, json.dumps(fields, default=str, sort_keys=True))
