"""
app/db.py — SQLite state store for pipeline runs.

Two tables:
    runs                — one row per keyword campaign
    stage_executions    — one row per stage attempt (for retries and history)

Kept intentionally simple. Switch to Postgres later if you need it;
the sqlite3 API is almost identical.
"""

import sqlite3
import json
import os
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

DB_PATH = Path(os.environ.get("SAAS_DB_PATH", "runs.db"))

# Valid run statuses
STATUS_DRAFT                = "draft"
STATUS_STAGE1_RUNNING       = "stage_1_running"
STATUS_STAGE2_RUNNING       = "stage_2_running"
STATUS_AWAITING_FINAL_URL   = "awaiting_final_url"
STATUS_STAGE3_RUNNING       = "stage_3_running"
STATUS_BLOG_READY           = "blog_ready"
STATUS_BANK_RUNNING         = "bank_running"
STATUS_BANK_READY           = "bank_ready"
STATUS_QUORA_RUNNING        = "quora_running"
STATUS_REDDIT_RUNNING       = "reddit_running"
STATUS_SUBSTACK_RUNNING     = "substack_running"
STATUS_COMPLETE             = "complete"
STATUS_FAILED               = "failed"
STATUS_CANCELLED            = "cancelled"

# Sanity check: every status reachable from every other status is enforced
# in orchestrator.py. The DB just stores whatever we tell it.

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword            TEXT NOT NULL,
    countries          TEXT NOT NULL,            -- JSON array
    status             TEXT NOT NULL DEFAULT 'draft',
    created_by         TEXT,                      -- user identifier
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL,
    failed_at_stage    TEXT,
    error_message      TEXT,
    blog_doc_url       TEXT,
    estimated_cost_usd REAL DEFAULT 0.0,
    metadata           TEXT                       -- JSON blob
);

CREATE TABLE IF NOT EXISTS stage_executions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          INTEGER NOT NULL,
    stage_name      TEXT NOT NULL,
    status          TEXT NOT NULL,              -- queued / running / success / failed
    started_at      TEXT,
    finished_at     TEXT,
    duration_secs   REAL,
    log_excerpt     TEXT,
    error_message   TEXT,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS activity_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id       INTEGER,
    timestamp    TEXT NOT NULL,
    level        TEXT NOT NULL,                  -- info / warn / error
    message      TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_created ON runs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_stage_run ON stage_executions(run_id);
CREATE INDEX IF NOT EXISTS idx_log_run ON activity_log(run_id, timestamp DESC);
"""


_lock = threading.Lock()


def _now():
    return datetime.utcnow().isoformat(timespec="seconds")


@contextmanager
def get_conn():
    """Thread-safe connection context manager."""
    with _lock:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def init_db():
    """Create tables on first boot. Idempotent."""
    with get_conn() as c:
        c.executescript(_SCHEMA)


# ─────────────────────────────────────────────────────────────
# Runs
# ─────────────────────────────────────────────────────────────

def create_run(keyword: str, countries: list, created_by: str = "anonymous") -> int:
    """Insert a new run in draft state. Returns run_id."""
    with get_conn() as c:
        cur = c.execute(
            """INSERT INTO runs (keyword, countries, status, created_by,
                                 created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (keyword.strip(), json.dumps(countries), STATUS_DRAFT,
             created_by, _now(), _now()),
        )
        return cur.lastrowid


def get_run(run_id: int) -> dict | None:
    with get_conn() as c:
        row = c.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
    if not row:
        return None
    return _row_to_run(row)


def list_runs(limit: int = 50, status_filter: str | None = None) -> list:
    sql = "SELECT * FROM runs"
    params = []
    if status_filter:
        sql += " WHERE status = ?"
        params.append(status_filter)
    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    with get_conn() as c:
        rows = c.execute(sql, params).fetchall()
    return [_row_to_run(r) for r in rows]


def count_by_status() -> dict:
    with get_conn() as c:
        rows = c.execute(
            "SELECT status, COUNT(*) AS n FROM runs GROUP BY status"
        ).fetchall()
    return {r["status"]: r["n"] for r in rows}


def update_status(run_id: int, new_status: str,
                    error_message: str | None = None,
                    failed_at_stage: str | None = None):
    with get_conn() as c:
        c.execute(
            """UPDATE runs
               SET status = ?, updated_at = ?,
                   error_message = COALESCE(?, error_message),
                   failed_at_stage = COALESCE(?, failed_at_stage)
               WHERE id = ?""",
            (new_status, _now(), error_message, failed_at_stage, run_id),
        )


def update_cost(run_id: int, delta_usd: float):
    with get_conn() as c:
        c.execute(
            "UPDATE runs SET estimated_cost_usd = estimated_cost_usd + ?, "
            "updated_at = ? WHERE id = ?",
            (delta_usd, _now(), run_id),
        )


def set_blog_doc_url(run_id: int, url: str):
    with get_conn() as c:
        c.execute(
            "UPDATE runs SET blog_doc_url = ?, updated_at = ? WHERE id = ?",
            (url, _now(), run_id),
        )


def _row_to_run(row) -> dict:
    d = dict(row)
    d["countries"] = json.loads(d["countries"]) if d["countries"] else []
    d["metadata"] = json.loads(d["metadata"]) if d["metadata"] else {}
    return d


# ─────────────────────────────────────────────────────────────
# Stage executions
# ─────────────────────────────────────────────────────────────

def record_stage_start(run_id: int, stage_name: str) -> int:
    with get_conn() as c:
        cur = c.execute(
            """INSERT INTO stage_executions
               (run_id, stage_name, status, started_at)
               VALUES (?, ?, 'running', ?)""",
            (run_id, stage_name, _now()),
        )
        return cur.lastrowid


def record_stage_finish(exec_id: int, status: str,
                         log_excerpt: str | None = None,
                         error_message: str | None = None):
    with get_conn() as c:
        row = c.execute(
            "SELECT started_at FROM stage_executions WHERE id = ?", (exec_id,)
        ).fetchone()
        duration = None
        if row and row["started_at"]:
            started = datetime.fromisoformat(row["started_at"])
            duration = (datetime.utcnow() - started).total_seconds()
        c.execute(
            """UPDATE stage_executions
               SET status = ?, finished_at = ?, duration_secs = ?,
                   log_excerpt = ?, error_message = ?
               WHERE id = ?""",
            (status, _now(), duration, log_excerpt, error_message, exec_id),
        )


def get_stage_executions(run_id: int) -> list:
    with get_conn() as c:
        rows = c.execute(
            "SELECT * FROM stage_executions WHERE run_id = ? "
            "ORDER BY started_at ASC",
            (run_id,),
        ).fetchall()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────
# Activity log
# ─────────────────────────────────────────────────────────────

def log(run_id: int | None, level: str, message: str):
    """Append a line to the activity log. Thread-safe."""
    with get_conn() as c:
        c.execute(
            "INSERT INTO activity_log (run_id, timestamp, level, message) "
            "VALUES (?, ?, ?, ?)",
            (run_id, _now(), level, message[:2000]),
        )


def get_activity(run_id: int, limit: int = 50) -> list:
    with get_conn() as c:
        rows = c.execute(
            "SELECT * FROM activity_log WHERE run_id = ? "
            "ORDER BY timestamp DESC LIMIT ?",
            (run_id, limit),
        ).fetchall()
    return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────
# Cost tracking helpers
# ─────────────────────────────────────────────────────────────

def todays_cost() -> float:
    today = datetime.utcnow().strftime("%Y-%m-%d")
    with get_conn() as c:
        row = c.execute(
            "SELECT SUM(estimated_cost_usd) AS total FROM runs "
            "WHERE substr(created_at, 1, 10) = ?",
            (today,),
        ).fetchone()
    return float(row["total"] or 0)


if __name__ == "__main__":
    init_db()
    print(f"✅ Database initialised at {DB_PATH.absolute()}")
