"""Loopback-only local API and packaged application. No third-party server."""

from __future__ import annotations

import json
import mimetypes
import re
import secrets
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from .core import (
    InputError,
    MAX_BYTES,
    comparison,
    ingest,
    mermaid,
    redact,
    validate_bundle,
)

WEB = Path(__file__).parent / "web"


def standalone_html(bundle: dict) -> str:
    """Single HTML file with all application code and supplied evidence inline."""
    html = (WEB / "index.html").read_text()
    css = (WEB / "styles.css").read_text()
    js = (WEB / "app.js").read_text()
    engine = (WEB / "engine.js").read_text()
    encoded = (
        json.dumps(bundle, ensure_ascii=True)
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
    )
    html = re.sub(
        r'<link rel="stylesheet" href="styles\.css"\s*/?>',
        lambda _: f"<style>{css}</style>",
        html,
    )
    html = html.replace(
        '<script src="engine.js" defer></script>', f"<script>{engine}</script>"
    )
    html = html.replace(
        '<script src="app.js" defer></script>',
        f"<script>window.UNDERTOW_EMBEDDED={encoded};</script><script defer>{js}</script>",
    )
    return html


def make_server(port: int = 8767, directory: Path | None = None) -> ThreadingHTTPServer:
    cases = (directory or Path.cwd() / ".undertow" / "cases").resolve()
    cases.mkdir(parents=True, exist_ok=True)
    token = secrets.token_urlsafe(32)
    lock = threading.Lock()

    def save(case: dict) -> dict:
        key = secrets.token_hex(12)
        with lock:
            target = cases / f"{key}.json"
            target.write_text(json.dumps(case, ensure_ascii=False))
        return {"id": key, "case": case}

    class Handler(BaseHTTPRequestHandler):
        server_version = "Undertow/0.1"

        def log_message(self, fmt, *args):
            # Never print user log contents or paths.
            if args and isinstance(args[0], str):
                print(
                    f"[Undertow] {self.command} {urlparse(self.path).path} {args[1] if len(args) > 1 else ''}"
                )

        def send(self, value: bytes, content_type: str, status: int = 200):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(value)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; object-src 'none'; frame-ancestors 'none'",
            )
            self.end_headers()
            self.wfile.write(value)

        def json(self, value, status=200):
            self.send(
                json.dumps(value).encode(), "application/json; charset=utf-8", status
            )

        def allowed_host(self) -> bool:
            return self.headers.get("Host") in {
                f"127.0.0.1:{self.server.server_port}",
                f"localhost:{self.server.server_port}",
            }

        def do_GET(self):
            if not self.allowed_host():
                return self.json({"error": "Invalid local host"}, 403)
            path = urlparse(self.path).path
            if path == "/api/config":
                return self.json(
                    {"mode": "local", "token": token, "max_bytes": MAX_BYTES}
                )
            if path == "/api/cases":
                result = []
                for file in sorted(
                    cases.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True
                ):
                    try:
                        case = json.loads(file.read_text())
                        result.append(
                            {
                                "id": file.stem,
                                "title": case["title"],
                                "events": len(case["events"]),
                                "consumers": len(case["analysis"]["consumers"]),
                                "saved": file.stat().st_mtime,
                            }
                        )
                    except (ValueError, KeyError):
                        pass
                return self.json(result)
            if path.startswith("/api/cases/"):
                key = path.rsplit("/", 1)[-1]
                if len(key) != 24 or any(c not in "0123456789abcdef" for c in key):
                    return self.json({"error": "Invalid case ID"}, 400)
                file = cases / f"{key}.json"
                if not file.is_file():
                    return self.json({"error": "Case not found"}, 404)
                return self.send(file.read_bytes(), "application/json")
            relative = "index.html" if path == "/" else path.lstrip("/")
            target = (WEB / relative).resolve()
            if not target.is_relative_to(WEB.resolve()) or not target.is_file():
                return self.json({"error": "Not found"}, 404)
            self.send(
                target.read_bytes(),
                mimetypes.guess_type(str(target))[0] or "application/octet-stream",
            )

        def do_POST(self):
            if not self.allowed_host() or not secrets.compare_digest(
                self.headers.get("X-Undertow-Token", ""), token
            ):
                return self.json({"error": "Local session token required"}, 403)
            origin = self.headers.get("Origin")
            if origin and origin not in {
                f"http://127.0.0.1:{self.server.server_port}",
                f"http://localhost:{self.server.server_port}",
            }:
                return self.json({"error": "Cross-origin requests are disabled"}, 403)
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 1 <= length <= MAX_BYTES * 3:
                    raise InputError("Request size must be between 1 byte and 60 MiB")
                body = json.loads(self.rfile.read(length))
                if not isinstance(body, dict):
                    raise InputError("Request must be an object")
                path = urlparse(self.path).path
                if path == "/api/analyze":
                    case = ingest(
                        body.get("sources"),
                        body.get("title", "Imported incident"),
                        bool(body.get("assume_utc")),
                    )
                    return self.json(save(case))
                if path == "/api/import":
                    return self.json(save(validate_bundle(body)))
                if path == "/api/compare":
                    return self.json(
                        comparison(
                            validate_bundle(body.get("baseline")),
                            validate_bundle(body.get("candidate")),
                        )
                    )
                if path in {"/api/redact", "/api/mermaid", "/api/html"}:
                    case = validate_bundle(body)
                    if path == "/api/redact":
                        return self.json(redact(case))
                    if path == "/api/mermaid":
                        return self.send(
                            mermaid(case).encode(), "text/plain; charset=utf-8"
                        )
                    return self.send(
                        standalone_html(case).encode(), "text/html; charset=utf-8"
                    )
                return self.json({"error": "Not found"}, 404)
            except (InputError, ValueError, TypeError, KeyError) as exc:
                return self.json({"error": str(exc)[:250]}, 400)

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.daemon_threads = True
    return server


def serve(port: int, directory: Path | None = None):
    server = make_server(port, directory)
    print(f"Undertow is ready at http://127.0.0.1:{server.server_port}/", flush=True)
    print("Log imports stay on this computer. Press Ctrl+C to stop.", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
