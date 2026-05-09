import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor,execute_values

def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var:{name}")
    return value


@contextmanager
def get_db_conn():
    database_url = require_env("NEON_DATABASE_URL")
    conn = psycopg2.connect(database_url)

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

