from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from typing import Any

from .config import settings


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover
        base: dict[str, Any] = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            base["exc_info"] = self.formatException(record.exc_info)
        for k, v in record.__dict__.items():
            if k.startswith("_") or k in base or k in {"args", "msg", "levelname", "levelno", "pathname", "filename", "module", "lineno", "funcName", "created", "msecs", "relativeCreated", "thread", "threadName", "processName", "process"}:
                continue
            try:
                json.dumps(v)
                base[k] = v
            except Exception:
                base[k] = str(v)
        return json.dumps(base, separators=(",", ":"))


def configure_logging() -> None:
    if not settings.json_logs:
        return
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(logging.INFO)

__all__ = ["configure_logging", "JsonFormatter"]
