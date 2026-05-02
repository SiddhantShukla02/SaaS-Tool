from app.database import fetch_all, fetch_one, execute


#==========================================
#   STATUS OPTIONS
#==========================================

STATUS_DRAFT = "draft"
STATUS_STAGE1_RUNNING = "stage_1_running"
STATUS_STAGE2_RUNNING = "stage_2_running"
STATUS_AWAITING_FINAL_URL = "awaiting_final_url"
STATUS_STAGE3_RUNNING = "stage_3_running"
STATUS_BLOG_READY = "blog_ready"
STATUS_BANK_RUNNING = "bank_running"
STATUS_BANK_READY = "bank_ready"
STATUS_QUORA_RUNNING = "quora_running"
STATUS_REDDIT_RUNNING = "reddit_running"
STATUS_SUBSTACK_RUNNING = "substack_running"
STATUS_COMPLETE = "complete"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"

def get_run(run_id: int) -> dict | None:
    return fetch_one(
        """
        SELECT *
        FROM runs
        WHERE id = %s
        """,
        (run_id,),
    )


def list_runs(limit: int = 50, status_filter: str | None = None) -> list[dict]:
    if status_filter:
        return fetch_all(
            """
            SELECT *
            FROM runs
            WHERE status = %s
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (status_filter, limit),
        )

    return fetch_all(
        """
        SELECT *
        FROM runs
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (limit,),
    )


def count_by_status() -> dict:
    rows = fetch_all(
        """
        SELECT status, COUNT(*) AS n
        FROM runs
        GROUP BY status
        """
    )
    return {row["status"]: row["n"] for row in rows}


def update_status(
    run_id: int,
    new_status: str,
    error_message: str | None = None,
    failed_at_stage: str | None = None,
) -> None:
    execute(
        """
        UPDATE runs
        SET status = %s,
            updated_at = NOW(),
            error_message = COALESCE(%s, error_message),
            failed_at_stage = COALESCE(%s, failed_at_stage)
        WHERE id = %s
        """,
        (new_status, error_message, failed_at_stage, run_id),
    )


def log(run_id: int | None, level: str, message: str) -> None:
    execute(
        """
        INSERT INTO activity_log (run_id, level, message)
        VALUES (%s, %s, %s)
        """,
        (run_id, level, message[:2000]),
    )

def record_stage_start(run_id: int, stage_name: str) -> int:
    row = fetch_one(
        """
        INSERT INTO stage_executions
            (run_id, stage_name, status, started_at)
        VALUES (%s, %s, 'running', NOW())
        RETURNING id
        """,
        (run_id, stage_name),
    )

    return row["id"]


def record_stage_finish(
    exec_id: int,
    status: str,
    log_excerpt: str | None = None,
    error_message: str | None = None,
) -> None:
    execute(
        """
        UPDATE stage_executions
        SET status = %s,
            finished_at = NOW(),
            duration_secs = EXTRACT(EPOCH FROM (NOW() - started_at)),
            log_excerpt = %s,
            error_message = %s
        WHERE id = %s
        """,
        (status, log_excerpt, error_message, exec_id),
    )


def get_stage_executions(run_id: int) -> list[dict]:
    return fetch_all(
        """
        SELECT *
        FROM stage_executions
        WHERE run_id = %s
        ORDER BY started_at ASC
        """,
        (run_id,),
    )

def is_cancelled(run_id: int) -> bool:
    run = get_run(run_id)
    return bool(run and run["status"] == STATUS_CANCELLED)

def todays_cost() -> float:
    return 0.0

def get_activity(run_id: int, limit: int = 50) -> list[dict]:
    return fetch_all(
        """
        SELECT
            id,
            run_id,
            created_at AS timestamp,
            level,
            message
        FROM activity_log
        WHERE run_id = %s
        ORDER BY created_at DESC
        LIMIT %s
        """,
        (run_id, limit),
    )