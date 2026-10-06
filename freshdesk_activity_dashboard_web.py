#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import subprocess
import sys
import threading
import time
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, Optional, Tuple

import freshdesk_activity_dashboard as dashboard_exporter


DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8787
DEFAULT_AUTO_REFRESH_SECONDS = 24 * 60 * 60


def load_env(env_path: Path) -> Dict[str, str]:
    data: Dict[str, str] = {}
    if not env_path.exists():
        return data
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip().strip('"').strip("'")
    return data


def env_or_file(name: str, env_values: Dict[str, str], default: str = "") -> str:
    return os.environ.get(name, env_values.get(name, default)).strip()


def env_as_bool(name: str, env_values: Dict[str, str], default: bool = False) -> bool:
    raw = env_or_file(name, env_values, "1" if default else "0").lower()
    return raw in {"1", "true", "yes", "y", "on"}


class DashboardApp:
    def __init__(
        self,
        *,
        env_file: Path,
        output_dir: Path,
        host: str,
        port: int,
        auto_refresh_seconds: int,
        refresh_on_start: bool,
    ):
        self.env_file = env_file
        self.output_dir = output_dir
        self.host = host
        self.port = port
        self.auto_refresh_seconds = auto_refresh_seconds
        self.refresh_on_start = refresh_on_start
        self.exporter_script = Path(__file__).with_name("freshdesk_activity_dashboard.py")
        self.html_path = output_dir / "freshdesk_activity_dashboard.html"
        self.json_path = output_dir / "freshdesk_activity_dashboard.json"
        self.csv_path = output_dir / "freshdesk_activity_dashboard.csv"
        self.status_path = output_dir / ".freshdesk_activity_dashboard_status.json"
        self._lock = threading.Lock()
        self._refresh_thread: Optional[threading.Thread] = None

    def ensure_dashboard(self) -> Tuple[bool, str]:
        if self.html_path.exists() and self.json_path.exists():
            return True, "existing"
        return self.refresh_dashboard()

    def refresh_dashboard(self) -> Tuple[bool, str]:
        with self._lock:
            cmd = [sys.executable, str(self.exporter_script), "--env-file", str(self.env_file), "--output-dir", str(self.output_dir)]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                return True, (result.stdout or "ok").strip()
            detail = (result.stderr or result.stdout or "refresh failed").strip()
            return False, detail[-2000:]

    def read_json(self) -> Dict[str, object]:
        if not self.json_path.exists():
            return {"summary": {}, "rows": []}
        return json.loads(self.json_path.read_text(encoding="utf-8"))

    def read_status(self) -> Dict[str, object]:
        if not self.status_path.exists():
            return {}
        return json.loads(self.status_path.read_text(encoding="utf-8"))

    def index_html(self, last_refresh_message: str = "") -> str:
        payload = self.read_json()
        return dashboard_exporter.build_dashboard_html(
            payload,
            refresh_enabled=True,
            last_refresh_message=last_refresh_message,
            csv_url="/download.csv",
            json_url="/api/dashboard",
        )

    def start_background_refresh(self) -> None:
        if self.auto_refresh_seconds <= 0:
            return
        if self._refresh_thread is not None:
            return

        def loop() -> None:
            if self.refresh_on_start:
                ok, detail = self.refresh_dashboard()
                print(f"Startup refresh {'ok' if ok else 'failed'}: {detail}", file=sys.stderr)
            while True:
                time.sleep(self.auto_refresh_seconds)
                ok, detail = self.refresh_dashboard()
                print(f"Scheduled refresh {'ok' if ok else 'failed'}: {detail}", file=sys.stderr)

        self._refresh_thread = threading.Thread(target=loop, name="dashboard-refresh", daemon=True)
        self._refresh_thread.start()



class DashboardHandler(BaseHTTPRequestHandler):
    app: DashboardApp

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/":
            ok, detail = self.app.ensure_dashboard()
            if not ok:
                self.send_json({"ok": False, "detail": detail}, status=HTTPStatus.BAD_GATEWAY)
                return
            self.send_html(self.app.index_html(detail if detail != "existing" else ""))
            return
        if parsed.path == "/api/dashboard":
            ok, detail = self.app.ensure_dashboard()
            if not ok:
                self.send_json({"ok": False, "detail": detail}, status=HTTPStatus.BAD_GATEWAY)
                return
            self.send_json(self.app.read_json())
            return
        if parsed.path == "/healthz":
            self.send_json({"ok": True, "status": self.app.read_status()})
            return
        if parsed.path == "/download.csv":
            ok, detail = self.app.ensure_dashboard()
            if not ok:
                self.send_json({"ok": False, "detail": detail}, status=HTTPStatus.BAD_GATEWAY)
                return
            self.send_file(self.app.csv_path, download_name="freshdesk_activity_dashboard.csv")
            return
        if parsed.path == "/download.html":
            ok, detail = self.app.ensure_dashboard()
            if not ok:
                self.send_json({"ok": False, "detail": detail}, status=HTTPStatus.BAD_GATEWAY)
                return
            self.send_file(self.app.html_path, download_name="freshdesk_activity_dashboard.html")
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/refresh":
            ok, detail = self.app.refresh_dashboard()
            status = HTTPStatus.OK if ok else HTTPStatus.BAD_GATEWAY
            self.send_json({"ok": ok, "message": detail}, status=status)
            return
        self.send_error(HTTPStatus.NOT_FOUND)

    def log_message(self, format: str, *args: object) -> None:
        sys.stderr.write("%s - - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), format % args))

    def send_html(self, body: str) -> None:
        payload = body.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def send_json(self, payload: object, *, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = (json.dumps(payload, indent=2) + "\n").encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_file(self, path: Path, *, download_name: Optional[str] = None) -> None:
        if not path.exists():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        body = path.read_bytes()
        content_type = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        if download_name:
            self.send_header("Content-Disposition", f'attachment; filename="{download_name}"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def build_parser() -> argparse.ArgumentParser:
    env = load_env(Path(__file__).with_name(".env"))
    parser = argparse.ArgumentParser(description="Serve the Freshdesk activity dashboard as a web app.")
    parser.add_argument("--env-file", default=str(Path(__file__).with_name(".env")), help="Path to env file.")
    parser.add_argument(
        "--output-dir",
        default=env_or_file("DASHBOARD_OUTPUT_DIR", env, str(Path(__file__).with_name("dashboard"))),
        help="Dashboard export directory.",
    )
    parser.add_argument("--host", default=env_or_file("HOST", env, DEFAULT_HOST), help="Host to bind. Use 0.0.0.0 for LAN sharing.")
    parser.add_argument(
        "--port",
        type=int,
        default=int(env_or_file("PORT", env, str(DEFAULT_PORT))),
        help="Port to bind.",
    )
    parser.add_argument(
        "--auto-refresh-seconds",
        type=int,
        default=int(env_or_file("DASHBOARD_AUTO_REFRESH_SECONDS", env, str(DEFAULT_AUTO_REFRESH_SECONDS))),
        help="Automatically refresh the dashboard in the background on this interval. Set 0 to disable.",
    )
    parser.add_argument(
        "--refresh-on-start",
        action="store_true",
        default=env_as_bool("DASHBOARD_REFRESH_ON_START", env, True),
        help="Refresh the dashboard once in the background when the server starts.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    app = DashboardApp(
        env_file=Path(args.env_file),
        output_dir=Path(args.output_dir),
        host=args.host,
        port=args.port,
        auto_refresh_seconds=args.auto_refresh_seconds,
        refresh_on_start=args.refresh_on_start,
    )
    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    DashboardHandler.app = app
    app.start_background_refresh()
    print(f"Serving Freshdesk dashboard on http://{args.host}:{args.port}")
    print("Use your machine IP instead of 0.0.0.0 when sharing on the same network.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping server.")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
