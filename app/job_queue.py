"""
app/queue.py — Job queue abstraction.

Supports two backends, chosen by env var SAAS_QUEUE_BACKEND:
  - 'rq'      — Redis Queue. Production. Requires REDIS_URL env var.
  - 'thread'  — In-process threading. Dev only. Single-machine, ephemeral.

For Streamlit Cloud / Railway's cheapest tier, 'thread' works fine if you
don't need jobs to survive a restart.

Public surface:
    enqueue(run_id, stage_name, job_params) → enqueues a job
"""

import os
import sys
import threading
import time
from pathlib import Path
from typing import Optional

BACKEND = os.environ.get("SAAS_QUEUE_BACKEND", "thread").lower()

ROOT = Path(__file__).resolve().parent.parent

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ─────────────────────────────────────────────────────────────
# Job function — what a worker actually runs
# ─────────────────────────────────────────────────────────────

def execute_job(run_id: int, stage_name: str, job_params: dict):
    """
    The function both backends call.
    Pulls in heavy deps only inside the function so the UI process doesn't
    load gspread/google-genai unnecessarily.
    """

    try:
        # print(f"[debug] cwd={os.getcwd()}", flush=True)
        # print(f"[debug] ROOT={ROOT}", flush=True)
        # print(f"[debug] stages exists={(ROOT / 'stages').exists()}", flush=True)
        # print(f"[debug] sys.path[0:3]={sys.path[:3]}", flush=True)

        from pathlib import Path
        import sys

        ROOT = Path(__file__).resolve().parent.parent
        STAGES_PATH = ROOT / "stages"

        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))

        if str(STAGES_PATH) not in sys.path:
            sys.path.insert(0, str(STAGES_PATH))

        # direct import now
        from runner import run_stage

    except Exception as e:
        print(f"[debug] import failed: {e}", flush=True)
        raise


    from stages.runner import run_stage
    from app import orchestrator
    from app.repositories import state_repo as db

    def progress_cb(line: str):
        print(f"[run {run_id}] {line}", flush=True)
        db.log(run_id, "info", line[:1000])
    db.log(run_id, "info", f"──────── {stage_name} ────────")
    db.log(run_id, "info", f"[worker] starting {stage_name}")
    print(f"[run {run_id}] [worker] starting {stage_name}", flush=True)
    exec_id = db.record_stage_start(run_id, stage_name)

    try:
        # Pass platform parameter via env var for stage_5_drafts
        if stage_name == "stage_5_drafts" and "platform" in job_params:
            os.environ["SAAS_PLATFORM"] = job_params["platform"]

        result = run_stage(stage_name, run_id, progress_cb=progress_cb)

        if result["status"] == "cancelled":
            db.log(run_id, "warn", f"[worker] stopped {stage_name} because run was cancelled")
            print(f"[run {run_id}] [worker] stopped {stage_name}: cancelled", flush=True)

            db.record_stage_finish(
                exec_id,
                "cancelled",
                log_excerpt=result.get("output", "")[-2000:],
                error_message=None,
            )
            return
        


        if result["status"] == "failed":
            db.log(run_id, "error", result.get("error", "Stage failed")[:1000])
            print(f"[run {run_id}] [worker] stage failed {stage_name}: {result.get('error')}", flush=True)
        db.record_stage_finish(
            exec_id, result["status"],
            log_excerpt=result.get("output", "")[-2000:],
            error_message=result.get("error"),
        )
        orchestrator.on_stage_finished(
            run_id, stage_name, result["status"], result.get("error"),
        )
        level = "error" if result["status"] == "failed" else "info"

        db.log(run_id, level, f"[worker] finished {stage_name}: {result['status']}")
        print(f"[run {run_id}] [worker] finished {stage_name}: {result['status']}", flush=True)
    except Exception as e:
        import traceback
        err = f"{type(e).__name__}: {e}\n{traceback.format_exc()[-1500:]}"
        db.log(run_id, "error", err[:1000])
        print(f"[run {run_id}] [worker] failed {stage_name}: {err}", flush=True)
        db.record_stage_finish(exec_id, "failed", error_message=err)
        orchestrator.on_stage_finished(run_id, stage_name, "failed", err)


# ─────────────────────────────────────────────────────────────
# Backend: RQ (production)
# ─────────────────────────────────────────────────────────────

def _enqueue_rq(run_id: int, stage_name: str, job_params: dict):
    import redis
    from rq import Queue

    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    conn = redis.from_url(redis_url)
    q = Queue("saas-blog", connection=conn)
    q.enqueue(
        execute_job, run_id, stage_name, job_params,
        job_timeout=3600,       # 1 hour max per job
        result_ttl=86400,       # keep result metadata 24h
    )


# ─────────────────────────────────────────────────────────────
# Backend: threading (dev)
# ─────────────────────────────────────────────────────────────

_thread_pool_running = []


def _enqueue_thread(run_id: int, stage_name: str, job_params: dict):
    t = threading.Thread(
        target=execute_job,
        args=(run_id, stage_name, job_params),
        daemon=True,
        name=f"saas-job-{run_id}-{stage_name}",
    )
    _thread_pool_running.append(t)
    t.start()


# ─────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────

def enqueue(run_id: int, stage_name: str, job_params: Optional[dict] = None):
    params = job_params or {}
    if BACKEND == "rq":
        _enqueue_rq(run_id, stage_name, params)
    elif BACKEND == "thread":
        _enqueue_thread(run_id, stage_name, params)
    else:
        raise RuntimeError(f"Unknown SAAS_QUEUE_BACKEND: {BACKEND}")


def run_worker_loop():
    """
    Entrypoint for the background worker process (RQ backend).
    For 'thread' backend this is a no-op — jobs run in the UI process.
    """
    if BACKEND != "rq":
        print(f"ℹ️  worker not needed for backend '{BACKEND}'. Exiting.")
        return

    import redis
    from rq import Worker, Queue

    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    conn = redis.from_url(redis_url)
    print(f"🔧 RQ worker starting (queue: saas-blog, redis: {redis_url})")
    worker = Worker([Queue("saas-blog", connection=conn)], connection=conn)
    worker.work()


if __name__ == "__main__":
    run_worker_loop()
