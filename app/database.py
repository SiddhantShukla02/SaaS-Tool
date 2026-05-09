import os
import time
from contextlib import contextmanager

import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import RealDictCursor,execute_values

def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var:{name}")
    return value

def connect_with_retry(database_url: str, attempts: int = 3, base_delay: float = 1.0):
    last_error = None

    for attempt in range(1, attempts + 1):
        try:
            return psycopg2.connect(database_url)
        except OperationalError as e:
            last_error = e

            if attempt == attempts:
                break

            time.sleep(base_delay * attempt)

    raise RuntimeError(
        "Could not connect to Neon/Postgres after retrying. "
        "The database may be cold-starting or temporarily unavailable."
    ) from last_error

@contextmanager
def get_db_conn():
    database_url = require_env("NEON_DATABASE_URL")
    conn = connect_with_retry(database_url)

    try:
        yield conn
        if not conn.closed:
            conn.commit()

    except Exception:
        if not conn.closed:
            conn.rollback()
        raise
    finally:
        if not conn.closed:
            conn.close()


def fetch_all(sql: str, params: tuple = ()):
    with get_db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql,params)
            return list(cur.fetchall())


def fetch_one(sql: str, params: tuple=()):
    rows = fetch_all(sql, params)
    return rows[0] if rows else None


def execute(sql:str , params: tuple = None):
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())


def insert_many(sql: str, rows: list[tuple]):
    if not rows:
        return
    
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)

