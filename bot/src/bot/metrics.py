from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict

from .config import settings

_COUNTERS: Dict[str, int] = {}

def inc(name: str, amount: int = 1) -> None:
    _COUNTERS[name] = _COUNTERS.get(name, 0) + amount

class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # type: ignore[override]
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        body_lines = []
        for k, v in _COUNTERS.items():
            body_lines.append(f"socionics_bot_counter{{name=\"{k}\"}} {v}")
        body = ("\n".join(body_lines) + "\n").encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

def start_server() -> None:
    if not settings.enable_metrics:
        return
    server = HTTPServer((settings.metrics_host, settings.metrics_port), MetricsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

__all__ = ["inc", "start_server"]
