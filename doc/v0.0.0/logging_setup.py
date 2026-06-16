"""Structured (JSON-lines) logging with per-request context.

Every log line is one JSON object. Contextual fields (profile_id, delivery_id, stage, job_id,
duration_ms, attempt, status_code, count) are attached via `extra=` on the logging call and only
appear when set, so the same logger serves the API layer, the pipeline, and webhook delivery.
"""
import json
import logging
import sys
import time

from config import Config

# fields we promote from `extra=` into the JSON line, in a stable order
_CONTEXT_KEYS = (
    "profile_id", "delivery_id", "job_id", "stage", "event",
    "file", "page", "count", "attempt", "status_code", "duration_ms",
)


class JsonFormatter(logging.Formatter):
    def format(self, record):
        out = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)) + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for k in _CONTEXT_KEYS:
            v = getattr(record, k, None)
            if v is not None:
                out[k] = v
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, ensure_ascii=False)


def configure_logging(level=None):
    """Install the JSON formatter on the root logger (replacing any handlers uvicorn added)."""
    level = (level or Config.LOG_LEVEL).upper()
    root = logging.getLogger()
    root.setLevel(level)
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    # quiet the access-log spam; keep our structured lines
    logging.getLogger("uvicorn.access").setLevel("WARNING")
    return root


def get_logger(name):
    return logging.getLogger(name)
