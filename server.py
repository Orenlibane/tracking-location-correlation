import json
import os
import sqlite3
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse


BASE_DIR = Path(__file__).resolve().parent
PORT = int(os.environ.get("PORT", 8080))
DB_PATH = Path(os.environ.get("MAP_TRACKING_DB_PATH", BASE_DIR / "map_tracking.db"))
LOCATIONS_PATH = BASE_DIR / "locations.json"
ALLOWED_STATUSES = {"unreviewed", "ok", "bad"}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def load_base_locations():
    with LOCATIONS_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data["locations"]


BASE_LOCATIONS = load_base_locations()
BASE_LOCATION_BY_NAME = {loc["name"]: loc for loc in BASE_LOCATIONS}
AREA_NAMES = sorted({loc["area"] for loc in BASE_LOCATIONS})


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS location_reviews (
              name TEXT PRIMARY KEY,
              status TEXT NOT NULL DEFAULT 'unreviewed',
              assigned_area TEXT,
              updated_at TEXT NOT NULL
            )
            """
        )


def merge_locations():
    with get_db() as conn:
        rows = conn.execute(
            "SELECT name, status, assigned_area, updated_at FROM location_reviews"
        ).fetchall()

    reviews = {row["name"]: dict(row) for row in rows}
    merged = []
    for loc in BASE_LOCATIONS:
        review = reviews.get(loc["name"], {})
        status = review.get("status") or "unreviewed"
        assigned_area = review.get("assigned_area")
        effective_area = assigned_area if status == "bad" and assigned_area else loc["area"]
        merged.append(
            {
                **loc,
                "status": status,
                "assigned_area": assigned_area,
                "effective_area": effective_area,
                "updated_at": review.get("updated_at"),
            }
        )
    return merged


class AppHandler(SimpleHTTPRequestHandler):
    def end_json(self, status_code, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def parse_json_body(self):
        content_length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(content_length) if content_length else b"{}"
        try:
            return json.loads(raw.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            return None

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/locations":
            return self.end_json(
                200,
                {
                    "locations": merge_locations(),
                    "areas": AREA_NAMES,
                    "db_path": str(DB_PATH),
                },
            )

        if parsed.path == "/api/health":
            return self.end_json(200, {"ok": True})

        return super().do_GET()

    def do_PATCH(self):
        parsed = urlparse(self.path)
        prefix = "/api/locations/"
        if not parsed.path.startswith(prefix):
            return self.end_json(404, {"error": "Not found"})

        name = unquote(parsed.path[len(prefix):])
        if name not in BASE_LOCATION_BY_NAME:
            return self.end_json(404, {"error": "Unknown location"})

        payload = self.parse_json_body()
        if payload is None:
            return self.end_json(400, {"error": "Invalid JSON body"})

        status = payload.get("status")
        assigned_area = payload.get("assigned_area")

        if status not in ALLOWED_STATUSES:
            return self.end_json(400, {"error": "Invalid status"})

        base_area = BASE_LOCATION_BY_NAME[name]["area"]
        if status == "bad":
            if not assigned_area or assigned_area not in AREA_NAMES:
                return self.end_json(400, {"error": "Assigned area is required for invalid locations"})
            if assigned_area == base_area:
                return self.end_json(400, {"error": "Assigned area must be different from the original area"})
        else:
            assigned_area = None

        updated_at = utc_now()
        with get_db() as conn:
            conn.execute(
                """
                INSERT INTO location_reviews (name, status, assigned_area, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                  status = excluded.status,
                  assigned_area = excluded.assigned_area,
                  updated_at = excluded.updated_at
                """,
                (name, status, assigned_area, updated_at),
            )

        loc = BASE_LOCATION_BY_NAME[name]
        return self.end_json(
            200,
            {
                **loc,
                "status": status,
                "assigned_area": assigned_area,
                "effective_area": assigned_area if status == "bad" and assigned_area else loc["area"],
                "updated_at": updated_at,
            },
        )

    def log_message(self, format, *args):
        super().log_message(format, *args)


if __name__ == "__main__":
    init_db()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), AppHandler)
    print(f"Serving on port {PORT} with DB at {DB_PATH}")
    server.serve_forever()
