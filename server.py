import json
import os
import sqlite3
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from uuid import uuid4
from urllib.parse import unquote, urlparse

try:
    import psycopg
except ImportError:  # pragma: no cover - dependency is installed in production
    psycopg = None


BASE_DIR = Path(__file__).resolve().parent
PORT = int(os.environ.get("PORT", 8080))
DB_PATH = Path(os.environ.get("MAP_TRACKING_DB_PATH", BASE_DIR / "map_tracking.db"))
DATABASE_URL = os.environ.get("DATABASE_URL")
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


def using_postgres():
    return bool(DATABASE_URL)


def get_sqlite_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_postgres_db():
    if psycopg is None:
        raise RuntimeError("DATABASE_URL is set but psycopg is not installed.")
    return psycopg.connect(DATABASE_URL)


def init_postgres():
    with get_postgres_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS location_reviews (
                  name TEXT PRIMARY KEY,
                  status TEXT NOT NULL DEFAULT 'unreviewed',
                  assigned_area TEXT,
                  updated_at TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS review_history (
                  id BIGSERIAL PRIMARY KEY,
                  revision_group TEXT NOT NULL,
                  name TEXT NOT NULL,
                  previous_status TEXT NOT NULL,
                  previous_assigned_area TEXT,
                  new_status TEXT NOT NULL,
                  new_assigned_area TEXT,
                  changed_at TEXT NOT NULL,
                  action TEXT NOT NULL DEFAULT 'update',
                  restored_from_id BIGINT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS review_snapshots (
                  id BIGSERIAL PRIMARY KEY,
                  created_at TEXT NOT NULL,
                  label TEXT,
                  payload_json TEXT NOT NULL
                )
                """
            )
        conn.commit()


def init_sqlite():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_sqlite_db() as conn:
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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS review_history (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              revision_group TEXT NOT NULL,
              name TEXT NOT NULL,
              previous_status TEXT NOT NULL,
              previous_assigned_area TEXT,
              new_status TEXT NOT NULL,
              new_assigned_area TEXT,
              changed_at TEXT NOT NULL,
              action TEXT NOT NULL DEFAULT 'update',
              restored_from_id INTEGER
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS review_snapshots (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              created_at TEXT NOT NULL,
              label TEXT,
              payload_json TEXT NOT NULL
            )
            """
        )


def init_db():
    if using_postgres():
        init_postgres()
    else:
        init_sqlite()


def fetch_reviews():
    if using_postgres():
        with get_postgres_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT name, status, assigned_area, updated_at FROM location_reviews"
                )
                rows = cur.fetchall()
        return [
            {
                "name": row[0],
                "status": row[1],
                "assigned_area": row[2],
                "updated_at": row[3],
            }
            for row in rows
        ]

    with get_sqlite_db() as conn:
        rows = conn.execute(
            "SELECT name, status, assigned_area, updated_at FROM location_reviews"
        ).fetchall()

    return [dict(row) for row in rows]


def get_review(name):
    if using_postgres():
        with get_postgres_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT name, status, assigned_area, updated_at
                    FROM location_reviews
                    WHERE name = %s
                    """,
                    (name,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return {
            "name": row[0],
            "status": row[1],
            "assigned_area": row[2],
            "updated_at": row[3],
        }

    with get_sqlite_db() as conn:
        row = conn.execute(
            """
            SELECT name, status, assigned_area, updated_at
            FROM location_reviews
            WHERE name = ?
            """,
            (name,),
        ).fetchone()
    return dict(row) if row else None


def fetch_review_history(limit=30):
    if using_postgres():
        with get_postgres_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, revision_group, name, previous_status, previous_assigned_area,
                           new_status, new_assigned_area, changed_at, action, restored_from_id
                    FROM review_history
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
        return [
            {
                "id": row[0],
                "revision_group": row[1],
                "name": row[2],
                "previous_status": row[3],
                "previous_assigned_area": row[4],
                "new_status": row[5],
                "new_assigned_area": row[6],
                "changed_at": row[7],
                "action": row[8],
                "restored_from_id": row[9],
            }
            for row in rows
        ]

    with get_sqlite_db() as conn:
        rows = conn.execute(
            """
            SELECT id, revision_group, name, previous_status, previous_assigned_area,
                   new_status, new_assigned_area, changed_at, action, restored_from_id
            FROM review_history
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_history_entry(entry_id):
    if using_postgres():
        with get_postgres_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, revision_group, name, previous_status, previous_assigned_area,
                           new_status, new_assigned_area, changed_at, action, restored_from_id
                    FROM review_history
                    WHERE id = %s
                    """,
                    (entry_id,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return {
            "id": row[0],
            "revision_group": row[1],
            "name": row[2],
            "previous_status": row[3],
            "previous_assigned_area": row[4],
            "new_status": row[5],
            "new_assigned_area": row[6],
            "changed_at": row[7],
            "action": row[8],
            "restored_from_id": row[9],
        }

    with get_sqlite_db() as conn:
        row = conn.execute(
            """
            SELECT id, revision_group, name, previous_status, previous_assigned_area,
                   new_status, new_assigned_area, changed_at, action, restored_from_id
            FROM review_history
            WHERE id = ?
            """,
            (entry_id,),
        ).fetchone()
    return dict(row) if row else None


def insert_history_entry(
    name,
    previous_status,
    previous_assigned_area,
    new_status,
    new_assigned_area,
    changed_at,
    action="update",
    restored_from_id=None,
    revision_group=None,
):
    revision_group = revision_group or str(uuid4())

    if using_postgres():
        with get_postgres_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO review_history (
                      revision_group, name, previous_status, previous_assigned_area,
                      new_status, new_assigned_area, changed_at, action, restored_from_id
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        revision_group,
                        name,
                        previous_status,
                        previous_assigned_area,
                        new_status,
                        new_assigned_area,
                        changed_at,
                        action,
                        restored_from_id,
                    ),
                )
            conn.commit()
        return

    with get_sqlite_db() as conn:
        conn.execute(
            """
            INSERT INTO review_history (
              revision_group, name, previous_status, previous_assigned_area,
              new_status, new_assigned_area, changed_at, action, restored_from_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                revision_group,
                name,
                previous_status,
                previous_assigned_area,
                new_status,
                new_assigned_area,
                changed_at,
                action,
                restored_from_id,
            ),
        )


def upsert_review_row(name, status, assigned_area, updated_at):
    if using_postgres():
        with get_postgres_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO location_reviews (name, status, assigned_area, updated_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT(name) DO UPDATE SET
                      status = EXCLUDED.status,
                      assigned_area = EXCLUDED.assigned_area,
                      updated_at = EXCLUDED.updated_at
                    """,
                    (name, status, assigned_area, updated_at),
                )
            conn.commit()
        return

    with get_sqlite_db() as conn:
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


def delete_review_row(name):
    if using_postgres():
        with get_postgres_db() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM location_reviews WHERE name = %s", (name,))
            conn.commit()
        return

    with get_sqlite_db() as conn:
        conn.execute("DELETE FROM location_reviews WHERE name = ?", (name,))


def fetch_snapshots(limit=20):
    if using_postgres():
        with get_postgres_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, created_at, label
                    FROM review_snapshots
                    ORDER BY id DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = cur.fetchall()
        return [
            {"id": row[0], "created_at": row[1], "label": row[2]}
            for row in rows
        ]

    with get_sqlite_db() as conn:
        rows = conn.execute(
            """
            SELECT id, created_at, label
            FROM review_snapshots
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_snapshot(snapshot_id):
    if using_postgres():
        with get_postgres_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, created_at, label, payload_json
                    FROM review_snapshots
                    WHERE id = %s
                    """,
                    (snapshot_id,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return {"id": row[0], "created_at": row[1], "label": row[2], "payload_json": row[3]}

    with get_sqlite_db() as conn:
        row = conn.execute(
            """
            SELECT id, created_at, label, payload_json
            FROM review_snapshots
            WHERE id = ?
            """,
            (snapshot_id,),
        ).fetchone()
    return dict(row) if row else None


def create_snapshot(label=None):
    created_at = utc_now()
    payload_json = json.dumps(
        {
            "created_at": created_at,
            "areas": AREA_NAMES,
            "locations": merge_locations(),
        },
        ensure_ascii=False,
    )

    if using_postgres():
        with get_postgres_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO review_snapshots (created_at, label, payload_json)
                    VALUES (%s, %s, %s)
                    RETURNING id
                    """,
                    (created_at, label, payload_json),
                )
                snapshot_id = cur.fetchone()[0]
            conn.commit()
        return {"id": snapshot_id, "created_at": created_at, "label": label}

    with get_sqlite_db() as conn:
        cur = conn.execute(
            """
            INSERT INTO review_snapshots (created_at, label, payload_json)
            VALUES (?, ?, ?)
            """,
            (created_at, label, payload_json),
        )
        snapshot_id = cur.lastrowid
    return {"id": snapshot_id, "created_at": created_at, "label": label}


def restore_snapshot(snapshot_id):
    snapshot = get_snapshot(snapshot_id)
    if not snapshot:
        return None

    payload = json.loads(snapshot["payload_json"])
    snapshot_locations = {
        entry["name"]: {
            "status": entry.get("status") or "unreviewed",
            "assigned_area": entry.get("assigned_area"),
        }
        for entry in payload.get("locations", [])
        if entry.get("name") in BASE_LOCATION_BY_NAME
    }

    current_reviews = {row["name"]: row for row in fetch_reviews()}
    revision_group = str(uuid4())
    changed_at = utc_now()

    for name in BASE_LOCATION_BY_NAME:
        current = current_reviews.get(name) or {
            "status": "unreviewed",
            "assigned_area": None,
        }
        desired = snapshot_locations.get(name) or {
            "status": "unreviewed",
            "assigned_area": None,
        }
        desired_assigned_area = desired["assigned_area"] if desired["status"] == "bad" else None

        if current["status"] == desired["status"] and current.get("assigned_area") == desired_assigned_area:
            continue

        if desired["status"] == "unreviewed" and not desired_assigned_area:
            delete_review_row(name)
        else:
            upsert_review_row(name, desired["status"], desired_assigned_area, changed_at)

        insert_history_entry(
            name,
            current["status"],
            current.get("assigned_area"),
            desired["status"],
            desired_assigned_area,
            changed_at,
            action="snapshot_restore",
            restored_from_id=snapshot_id,
            revision_group=revision_group,
        )

    return {"id": snapshot["id"], "created_at": snapshot["created_at"], "label": snapshot["label"]}


def save_review(name, status, assigned_area, updated_at, action="update", restored_from_id=None, revision_group=None):
    previous = get_review(name) or {
        "name": name,
        "status": "unreviewed",
        "assigned_area": None,
        "updated_at": None,
    }
    upsert_review_row(name, status, assigned_area, updated_at)
    insert_history_entry(
        name,
        previous["status"],
        previous["assigned_area"],
        status,
        assigned_area,
        updated_at,
        action=action,
        restored_from_id=restored_from_id,
        revision_group=revision_group,
    )


def merge_locations():
    reviews = {row["name"]: row for row in fetch_reviews()}
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

        if parsed.path == "/api/history":
            return self.end_json(
                200,
                {
                    "history": fetch_review_history(),
                },
            )

        if parsed.path == "/api/snapshots":
            return self.end_json(
                200,
                {
                    "snapshots": fetch_snapshots(),
                },
            )

        if parsed.path == "/api/health":
            return self.end_json(200, {"ok": True})

        return super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/snapshots":
            payload = self.parse_json_body()
            label = None if payload is None else payload.get("label")
            snapshot = create_snapshot(label=label)
            return self.end_json(
                200,
                {
                    "snapshot": snapshot,
                    "snapshots": fetch_snapshots(),
                },
            )

        if parsed.path == "/api/snapshots/restore-previous":
            snapshots = fetch_snapshots(limit=2)
            if len(snapshots) < 2:
                return self.end_json(400, {"error": "אין גרסה קודמת לשחזור"})
            restored_snapshot = restore_snapshot(snapshots[1]["id"])
            return self.end_json(
                200,
                {
                    "restored_snapshot": restored_snapshot,
                    "locations": merge_locations(),
                    "history": fetch_review_history(),
                    "snapshots": fetch_snapshots(),
                },
            )

        prefix = "/api/history/"
        if not parsed.path.startswith(prefix) or not parsed.path.endswith("/restore"):
            return self.end_json(404, {"error": "Not found"})

        history_id = parsed.path[len(prefix):-len("/restore")]
        try:
            history_id = int(history_id)
        except ValueError:
            return self.end_json(400, {"error": "Invalid history id"})

        entry = get_history_entry(history_id)
        if not entry:
            return self.end_json(404, {"error": "History entry not found"})

        name = entry["name"]
        if name not in BASE_LOCATION_BY_NAME:
            return self.end_json(404, {"error": "Unknown location"})

        updated_at = utc_now()
        save_review(
            name,
            entry["previous_status"],
            entry["previous_assigned_area"],
            updated_at,
            action="restore",
            restored_from_id=history_id,
        )

        loc = BASE_LOCATION_BY_NAME[name]
        status = entry["previous_status"]
        assigned_area = entry["previous_assigned_area"]
        return self.end_json(
            200,
            {
                "location": {
                    **loc,
                    "status": status,
                    "assigned_area": assigned_area,
                    "effective_area": assigned_area if status == "bad" and assigned_area else loc["area"],
                    "updated_at": updated_at,
                },
                "history": fetch_review_history(),
            },
        )

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
        save_review(name, status, assigned_area, updated_at)

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
    storage_target = "Postgres" if using_postgres() else str(DB_PATH)
    print(f"Serving on port {PORT} with DB at {storage_target}")
    server.serve_forever()
