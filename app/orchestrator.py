"""
app/orchestrator.py — Pipeline state machine and job dispatch.

The orchestrator is the only place that knows what comes after what.
UI calls it ("start run 42", "mark final_url ready"). Orchestrator writes
to DB and enqueues jobs on the worker queue.

State transitions enforced here:
    draft → stage_1_running → stage_2_running → awaiting_final_url
         → stage_3_running → blog_ready
         → (optional) bank_running → bank_ready
         → (optional) quora/reddit/substack_running → complete
    Any state → failed (on exception) or cancelled (on user action)
"""

import time
from datetime import datetime
from typing import Optional
from app.repositories.run_repo import create_run, insert_run_keywords
from app.repositories import state_repo as db
from app.job_queue import enqueue


# ─────────────────────────────────────────────────────────────
# User-facing actions
# ─────────────────────────────────────────────────────────────

def start_run(keyword: str, countries: list, created_by: str) -> int:
    run_id = create_run(keyword, created_by)

    keyword_rows = [
        {
            "keyword": keyword,
            "country_code": country_code,
        }
        for country_code in countries
    ]

    insert_run_keywords(run_id, keyword_rows)

    db.log(run_id, "info", f"Run created by {created_by}")
    db.log(run_id, "info", f"Stored {len(keyword_rows)} keyword/country pairs in DB")

    _queue_stage(run_id, "stage_1_serp_paa", db.STATUS_STAGE1_RUNNING)

    return run_id


def mark_final_url_ready(run_id: int, user: str = "anonymous"):
    """
    User clicked 'Final_URL ready, run Stage 3' in the UI.
    Only allowed from AWAITING_FINAL_URL state.
    """
    run = db.get_run(run_id)
    if run is None:
        raise ValueError(f"Run {run_id} not found")
    if run["status"] != db.STATUS_AWAITING_FINAL_URL:
        raise ValueError(
            f"Cannot mark Final_URL ready — run is in state '{run['status']}', "
            f"expected '{db.STATUS_AWAITING_FINAL_URL}'"
        )
    db.log(run_id, "info", f"Final_URL marked ready by {user}")
    _queue_stage(run_id, "stage_2_context", db.STATUS_STAGE2_RUNNING)


def start_question_bank(run_id: int):
    run = db.get_run(run_id)
    if run is None:
        raise ValueError(f"Run {run_id} not found")
    if run["status"] not in (db.STATUS_BLOG_READY, db.STATUS_COMPLETE,
                               db.STATUS_BANK_READY):
        raise ValueError(
            f"Question Bank can only start from blog_ready state. "
            f"Current state: {run['status']}"
        )
    _queue_stage(run_id, "stage_4_bank", db.STATUS_BANK_RUNNING)


def start_platform_drafts(run_id: int, platform: str):
    """platform = 'quora' | 'reddit' | 'substack' | 'all'"""
    if platform not in ("quora", "reddit", "substack", "all"):
        raise ValueError(f"Unknown platform: {platform}")
    run = db.get_run(run_id)
    if run is None:
        raise ValueError(f"Run {run_id} not found")
    if run["status"] not in (db.STATUS_BANK_READY, db.STATUS_COMPLETE):
        raise ValueError(
            f"Drafts require bank_ready or complete state. "
            f"Current: {run['status']}. Run Build Question Bank first."
        )
    status_map = {
        "quora":    db.STATUS_QUORA_RUNNING,
        "reddit":   db.STATUS_REDDIT_RUNNING,
        "substack": db.STATUS_SUBSTACK_RUNNING,
        "all":      db.STATUS_QUORA_RUNNING,   # marker
    }
    _queue_stage(
        run_id, "stage_5_drafts", status_map[platform],
        job_params={"platform": platform},
    )


def cancel_run(run_id: int, user: str = "anonymous"):
    db.update_status(run_id, db.STATUS_CANCELLED)
    db.log(run_id, "warn", f"Run cancelled by {user}")


def retry_failed_stage(run_id: int):
    """
    Retry the last failed stage. Looks up which stage it failed at,
    re-queues that stage.
    """
    run = db.get_run(run_id)
    if run is None:
        raise ValueError(f"Run {run_id} not found")
    if run["status"] != db.STATUS_FAILED:
        raise ValueError(
            f"Can only retry failed runs. Current state: {run['status']}"
        )
    stage = run.get("failed_at_stage")
    if not stage:
        raise ValueError("No failed_at_stage recorded — cannot retry")

    # Map stage back to status
    status_for_stage = {
        "stage_1_serp_paa":  db.STATUS_STAGE1_RUNNING,
        "stage_2_context":   db.STATUS_STAGE2_RUNNING,
        "stage_3_blog":      db.STATUS_STAGE3_RUNNING,
        "stage_4_bank":      db.STATUS_BANK_RUNNING,
        "stage_5_drafts":    db.STATUS_QUORA_RUNNING,
    }
    new_status = status_for_stage.get(stage)
    if new_status is None:
        raise ValueError(f"Cannot retry stage: {stage}")

    db.log(run_id, "info", f"Retrying {stage}")
    db.update_status(run_id, new_status, error_message="", failed_at_stage="")
    _queue_stage(run_id, stage, new_status)


# ─────────────────────────────────────────────────────────────
# Worker-side callback — called from jobs.py when a stage finishes
# ─────────────────────────────────────────────────────────────

def on_stage_finished(run_id: int, stage_name: str,
                       status: str, error: Optional[str] = None):
    """
    Called by the background worker when a stage completes or fails.
    Decides what to queue next based on the stage that just finished.
    """
    if status == "failed":
        db.update_status(run_id, db.STATUS_FAILED,
                          error_message=error, failed_at_stage=stage_name)
        db.log(run_id, "error", f"{stage_name} failed: {(error or '')[:200]}")
        return

    db.log(run_id, "info", f"{stage_name} complete")

    # Auto-advance chains
    if stage_name == "stage_1_serp_paa":
        db.update_status(run_id, db.STATUS_AWAITING_FINAL_URL)
        db.log(run_id, "info", "Stage 1 complete — waiting for URL selection")

    elif stage_name == "stage_2_context":
        db.log(run_id, "info", "Stage 2 complete — starting blog writing")
        _queue_stage(run_id, "stage_3_blog", db.STATUS_STAGE3_RUNNING)

    elif stage_name == "stage_3_blog":
        db.update_status(run_id, db.STATUS_BLOG_READY)
        db.log(run_id, "info", "Blog ready")

    elif stage_name == "stage_4_bank":
        db.update_status(run_id, db.STATUS_BANK_READY)
        db.log(run_id, "info", "Question Bank ready")

    elif stage_name == "stage_5_drafts":
        db.update_status(run_id, db.STATUS_COMPLETE)
        db.log(run_id, "info", "Run complete")


# ─────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────

def _queue_stage(run_id: int, stage_name: str, new_status: str,
                   job_params: Optional[dict] = None):
    """Update DB status and enqueue the actual job."""
    db.update_status(run_id, new_status)
    db.log(run_id, "info", f"{stage_name} queued")
    enqueue(run_id, stage_name, job_params or {})


# ─────────────────────────────────────────────────────────────
# Read helpers for the UI
# ─────────────────────────────────────────────────────────────

def get_dashboard_metrics() -> dict:
    """Returns summary counts for the dashboard header."""
    counts = db.count_by_status()
    active_statuses = [
        db.STATUS_STAGE1_RUNNING, db.STATUS_STAGE2_RUNNING,
        db.STATUS_STAGE3_RUNNING, db.STATUS_BANK_RUNNING,
        db.STATUS_QUORA_RUNNING, db.STATUS_REDDIT_RUNNING,
        db.STATUS_SUBSTACK_RUNNING,
    ]
    return {
        "active":      sum(counts.get(s, 0) for s in active_statuses),
        "awaiting":    counts.get(db.STATUS_AWAITING_FINAL_URL, 0),
        "complete":    counts.get(db.STATUS_COMPLETE, 0) + counts.get(db.STATUS_BLOG_READY, 0),
        "failed":      counts.get(db.STATUS_FAILED, 0),
        "cost_today":  round(db.todays_cost(), 2),
    }


def progress_for_run(run_id: int) -> dict:
    """
    Compute stage-level progress for a single run:
      {"stage_1": "done", "stage_2": "active", "stage_3": "pending", ...}
    """
    run = db.get_run(run_id)
    if run is None:
        return {}
    status = run["status"]
    STATE_TO_PROGRESS = {
        db.STATUS_DRAFT:              ("pending", "pending", "pending", "pending"),
        db.STATUS_STAGE1_RUNNING:     ("active",  "pending", "pending", "pending"),
        db.STATUS_STAGE2_RUNNING:     ("done",    "active",  "pending", "pending"),
        db.STATUS_AWAITING_FINAL_URL: ("done",    "done",    "pending", "pending"),
        db.STATUS_STAGE3_RUNNING:     ("done",    "done",    "active",  "pending"),
        db.STATUS_BLOG_READY:         ("done",    "done",    "done",    "pending"),
        db.STATUS_BANK_RUNNING:       ("done",    "done",    "done",    "active"),
        db.STATUS_BANK_READY:         ("done",    "done",    "done",    "done"),
        db.STATUS_QUORA_RUNNING:      ("done",    "done",    "done",    "done"),
        db.STATUS_REDDIT_RUNNING:     ("done",    "done",    "done",    "done"),
        db.STATUS_SUBSTACK_RUNNING:   ("done",    "done",    "done",    "done"),
        db.STATUS_COMPLETE:           ("done",    "done",    "done",    "done"),
        db.STATUS_CANCELLED:          ("pending", "pending", "pending", "pending"),
    }
    if status == db.STATUS_FAILED:
        stage = run.get("failed_at_stage", "")
        marker = {
            "stage_1_serp_paa":  ("failed",  "pending", "pending", "pending"),
            "stage_2_context":   ("done",    "failed",  "pending", "pending"),
            "stage_3_blog":      ("done",    "done",    "failed",  "pending"),
            "stage_4_bank":      ("done",    "done",    "done",    "failed"),
        }.get(stage, ("pending",) * 4)
    else:
        marker = STATE_TO_PROGRESS.get(status, ("pending",) * 4)
    return {
        "stage_1": marker[0],
        "stage_2": marker[1],
        "stage_3": marker[2],
        "stage_4": marker[3],
    }
