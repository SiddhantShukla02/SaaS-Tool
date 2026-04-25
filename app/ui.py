"""
app/ui.py — Streamlit UI for the blog pipeline SaaS.

Single-file UI with two views:
  1. Dashboard      — list of runs, New Run button, metrics
  2. Run detail     — per-run progress, gate control, action buttons

Launch:
    streamlit run app/ui.py

Env vars:
    SAAS_PASSWORD          — shared login password
    SAAS_DB_PATH           — path to SQLite DB (default: runs.db)
    SAAS_QUEUE_BACKEND     — 'thread' (dev) or 'rq' (prod)
    REDIS_URL              — if backend is 'rq'
    SAAS_ALLOW_NO_AUTH=1   — bypass login for local dev
"""

import os
import streamlit as st

# Set page config FIRST — before any other st.* call
st.set_page_config(
    page_title="SaaS for Blog — Divinheal",
    page_icon="📝",
    layout="wide",
    initial_sidebar_state="collapsed",
)

from app import db, orchestrator, auth          # noqa: E402

# Initialise DB on first load
db.init_db()

# Require login
auth.require_auth()

# ─────────────────────────────────────────────────────────────
# Styling — keep it minimal, let Streamlit's defaults do the heavy lifting
# ─────────────────────────────────────────────────────────────

st.markdown("""
<style>
.status-pill { display: inline-block; padding: 3px 10px; border-radius: 10px;
               font-size: 11px; font-weight: 500; }
.pill-running { background: #E6F1FB; color: #0C447C; }
.pill-gate    { background: #FAEEDA; color: #854F0B; }
.pill-done    { background: #E1F5EE; color: #085041; }
.pill-failed  { background: #FCEBEB; color: #791F1F; }
.pill-draft   { background: #F1EFE8; color: #444441; }
.progress-bar { display: flex; gap: 4px; margin: 6px 0; }
.progress-step { flex: 1; height: 5px; border-radius: 2px; background: #D3D1C7; }
.step-done   { background: #1D9E75; }
.step-active { background: #378ADD; }
.step-failed { background: #E24B4A; }
.run-card { padding: 12px; border: 0.5px solid rgba(0,0,0,0.1); border-radius: 8px;
            margin-bottom: 10px; }
.gate-box { background: #FAEEDA; padding: 12px; border-radius: 8px;
            border-left: 4px solid #EF9F27; color: #633806; margin: 10px 0; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# Status label helpers
# ─────────────────────────────────────────────────────────────

STATUS_LABELS = {
    db.STATUS_DRAFT:              ("Draft", "pill-draft"),
    db.STATUS_STAGE1_RUNNING:     ("Stage 1 running", "pill-running"),
    db.STATUS_STAGE2_RUNNING:     ("Stage 2 running", "pill-running"),
    db.STATUS_AWAITING_FINAL_URL: ("Awaiting Final_URL", "pill-gate"),
    db.STATUS_STAGE3_RUNNING:     ("Stage 3 running (blog)", "pill-running"),
    db.STATUS_BLOG_READY:         ("Blog ready", "pill-done"),
    db.STATUS_BANK_RUNNING:       ("Building question bank", "pill-running"),
    db.STATUS_BANK_READY:         ("Question bank ready", "pill-done"),
    db.STATUS_QUORA_RUNNING:      ("Generating Quora", "pill-running"),
    db.STATUS_REDDIT_RUNNING:     ("Generating Reddit", "pill-running"),
    db.STATUS_SUBSTACK_RUNNING:   ("Generating Substack", "pill-running"),
    db.STATUS_COMPLETE:           ("Complete", "pill-done"),
    db.STATUS_FAILED:             ("Failed", "pill-failed"),
    db.STATUS_CANCELLED:          ("Cancelled", "pill-draft"),
}


def status_pill(status: str) -> str:
    label, cls = STATUS_LABELS.get(status, (status, "pill-draft"))
    return f'<span class="status-pill {cls}">{label}</span>'


def progress_bar_html(progress: dict) -> str:
    def cls(s):
        return {"done": "step-done", "active": "step-active",
                "failed": "step-failed"}.get(s, "")
    return (
        '<div class="progress-bar">'
        f'<div class="progress-step {cls(progress.get("stage_1", "pending"))}"></div>'
        f'<div class="progress-step {cls(progress.get("stage_2", "pending"))}"></div>'
        f'<div class="progress-step {cls(progress.get("stage_3", "pending"))}"></div>'
        f'<div class="progress-step {cls(progress.get("stage_4", "pending"))}"></div>'
        '</div>'
    )


# ─────────────────────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────────────────────

header_left, header_right = st.columns([6, 1])
with header_left:
    st.markdown("## SaaS for Blog")
    st.caption(f"Divinheal content pipeline · signed in as **{auth.current_user()}**")
with header_right:
    st.write("")
    if st.button("Log out", use_container_width=True):
        auth.logout()

# Query param routing: ?run=42 opens detail view
run_view = st.query_params.get("run")


# ═════════════════════════════════════════════════════════════
# Run detail view
# ═════════════════════════════════════════════════════════════

def render_run_detail(run_id: int):
    run = db.get_run(run_id)
    if run is None:
        st.error(f"Run {run_id} not found")
        if st.button("← Back to dashboard"):
            st.query_params.clear()
            st.rerun()
        return

    if st.button("← Back to dashboard"):
        st.query_params.clear()
        st.rerun()

    # Title + status
    col_l, col_r = st.columns([5, 1])
    with col_l:
        st.markdown(f"### {run['keyword']}")
        st.caption(
            f"Run #{run['id']} · created by {run['created_by']} · "
            f"{run['created_at'].replace('T', ' ')}"
        )
    with col_r:
        st.markdown(status_pill(run["status"]), unsafe_allow_html=True)

    # Info tiles
    m1, m2, m3 = st.columns(3)
    with m1:
        st.metric("Target countries", ", ".join(run["countries"]) or "—")
    with m2:
        st.metric("Estimated cost", f"${run['estimated_cost_usd']:.2f}")
    with m3:
        blog_url = run.get("blog_doc_url")
        if blog_url:
            st.markdown(f"**Blog:** [Open doc ↗]({blog_url})")
        else:
            st.markdown("**Blog:** *(not ready yet)*")

    # Progress
    st.markdown(
        progress_bar_html(orchestrator.progress_for_run(run_id)),
        unsafe_allow_html=True,
    )

    # Stage executions table
    st.markdown("#### Pipeline progress")
    execs = db.get_stage_executions(run_id)
    STAGE_DISPLAY = {
        "stage_1_serp_paa": "Stage 1 — SERP + PAA (Cell 3)",
        "stage_2_context":  "Stage 2 — Context (Cells 5,7,16,18,20,21)",
        "stage_3_blog":     "Stage 3 — Blog (Cells 9,12,14,25,27,29,33)",
        "stage_4_bank":     "Stage 4 — Question Bank (Cell A)",
        "stage_5_drafts":   "Stage 5 — Platform Drafts (Cell B)",
    }
    if not execs:
        st.info("No stages have run yet.")
    else:
        for e in execs:
            icon = {"success": "✅", "failed": "❌", "running": "🔵"}.get(
                e["status"], "⏳",
            )
            label = STAGE_DISPLAY.get(e["stage_name"], e["stage_name"])
            dur = f"{e['duration_secs']:.0f}s" if e["duration_secs"] else "—"
            st.write(f"{icon} {label} · {dur} · `{e['status']}`")
            if e.get("error_message"):
                with st.expander("Error details"):
                    st.code(e["error_message"])

    # ── Action panel — depends on run status ──
    st.markdown("#### Actions")

    if run["status"] == db.STATUS_AWAITING_FINAL_URL:
        st.markdown(
            '<div class="gate-box"><strong>Action needed:</strong> '
            'open the Google Sheet, go to the Final_URL tab, paste the 5–8 '
            'competitor URLs you want to use, and click the green button '
            'when done. Stage 3 will then run automatically to blog completion.'
            '</div>',
            unsafe_allow_html=True,
        )
        col_a, col_b = st.columns([1, 1])
        with col_a:
            if st.button("✓ Final_URL ready, run Stage 3", type="primary",
                          use_container_width=True):
                try:
                    orchestrator.mark_final_url_ready(run_id, auth.current_user())
                    st.success("Stage 3 queued!")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))
        with col_b:
            if st.button("Cancel run", use_container_width=True):
                orchestrator.cancel_run(run_id, auth.current_user())
                st.rerun()

    elif run["status"] == db.STATUS_FAILED:
        st.error(f"Failed at: {run.get('failed_at_stage', 'unknown')}")
        if run.get("error_message"):
            with st.expander("Error message"):
                st.code(run["error_message"])
        if st.button("🔁 Retry failed stage", type="primary"):
            try:
                orchestrator.retry_failed_stage(run_id)
                st.rerun()
            except Exception as e:
                st.error(str(e))

    elif run["status"] in (db.STATUS_BLOG_READY, db.STATUS_BANK_READY,
                             db.STATUS_COMPLETE):
        col_bank, col_q, col_r, col_s = st.columns(4)
        with col_bank:
            if run["status"] == db.STATUS_BLOG_READY:
                if st.button("📦 Build Question Bank", use_container_width=True):
                    orchestrator.start_question_bank(run_id)
                    st.rerun()
            else:
                st.write("✓ Question bank built")
        bank_ready = run["status"] in (db.STATUS_BANK_READY, db.STATUS_COMPLETE)
        with col_q:
            if st.button("✍️ Generate Quora drafts", use_container_width=True,
                          disabled=not bank_ready):
                orchestrator.start_platform_drafts(run_id, "quora")
                st.rerun()
        with col_r:
            if st.button("🟠 Generate Reddit drafts", use_container_width=True,
                          disabled=not bank_ready):
                orchestrator.start_platform_drafts(run_id, "reddit")
                st.rerun()
        with col_s:
            if st.button("🔵 Generate Substack", use_container_width=True,
                          disabled=not bank_ready):
                orchestrator.start_platform_drafts(run_id, "substack")
                st.rerun()

    elif run["status"] in (db.STATUS_STAGE1_RUNNING, db.STATUS_STAGE2_RUNNING,
                             db.STATUS_STAGE3_RUNNING, db.STATUS_BANK_RUNNING,
                             db.STATUS_QUORA_RUNNING, db.STATUS_REDDIT_RUNNING,
                             db.STATUS_SUBSTACK_RUNNING):
        st.info("🔄 Currently running. This page auto-refreshes every 5s.")
        # Auto-refresh while running
        import time as _time
        _time.sleep(2)
        st.rerun()

    # ── Currently running stage ──
    running_exec = next((e for e in execs if e["status"] == "running"), None)

    if running_exec:
        stage_label = STAGE_DISPLAY.get(
            running_exec["stage_name"],
            running_exec["stage_name"]
        )

        st.markdown(f"### 🔄 Currently running: **{stage_label}**")
    # ── Activity log ──
    st.markdown("---")
    st.markdown("#### Activity")
    activity = db.get_activity(run_id, limit=150)
    if not activity:
        st.caption("No activity yet.")
    else:
        for a in reversed(activity):
            prefix = {"error": "🔴", "warn": "🟡", "info": "·"}.get(a["level"], "·")
            ts = a["timestamp"].split("T")[1][:8] if "T" in a["timestamp"] else a["timestamp"]

            msg = a["message"]

            formatted = f"[{ts}] {msg}"

            if "▶ started" in msg:
                st.info(formatted)
            elif "✅ finished" in msg:
                st.success(formatted)
            elif "❌" in msg or "failed" in msg.lower() or a["level"] == "error":
                st.error(formatted)
            else:
                st.markdown(
                    f"<code>[{ts}]</code> {prefix} {msg}",
                    unsafe_allow_html=True,
                )


# ═════════════════════════════════════════════════════════════
# Dashboard view
# ═════════════════════════════════════════════════════════════

def render_dashboard():
    metrics = orchestrator.get_dashboard_metrics()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Active runs", metrics["active"])
    m2.metric("Awaiting gate", metrics["awaiting"])
    m3.metric("Completed", metrics["complete"])
    m4.metric("Cost today", f"${metrics['cost_today']:.2f}")

    st.divider()

    # New run form
    with st.expander("➕ Start new run", expanded=not bool(db.list_runs(limit=1))):
        st.markdown(
            "**Step 1 (manual):** In your Google Sheet (`Keyword_n8n`), open the "
            "`keyword` tab and add the keyword + target country codes. "
            "**Step 2:** Fill in below and click Start."
        )
        with st.form("new_run"):
            keyword = st.text_input(
                "Primary keyword",
                placeholder="e.g. cornea transplant cost india",
            )
            countries_raw = st.text_input(
                "Country codes (comma-separated, 2-letter)",
                placeholder="e.g. ae, ng, gb, au",
            )
            submit = st.form_submit_button("Start run", type="primary")

        if submit:
            if not keyword.strip():
                st.error("Enter a keyword")
            else:
                ccs = [c.strip().lower() for c in countries_raw.split(",") if c.strip()]
                if not ccs:
                    st.error("Enter at least one country code")
                else:
                    run_id = orchestrator.start_run(
                        keyword.strip(), ccs, auth.current_user(),
                    )
                    st.success(f"Run #{run_id} queued — Stage 1 starting")
                    st.query_params["run"] = str(run_id)
                    st.rerun()

    # Runs list
    st.markdown("#### Runs")
    runs = db.list_runs(limit=30)
    if not runs:
        st.info("No runs yet. Click 'Start new run' above.")
        return

    for run in runs:
        with st.container():
            cols = st.columns([4, 1.5, 1])
            with cols[0]:
                st.markdown(f"**{run['keyword']}**")
                st.caption(
                    f"Run #{run['id']} · {', '.join(run['countries'])} · "
                    f"{run['created_at'].replace('T', ' ')[:16]}"
                )
                st.markdown(
                    progress_bar_html(orchestrator.progress_for_run(run['id'])),
                    unsafe_allow_html=True,
                )
            with cols[1]:
                st.markdown(status_pill(run["status"]), unsafe_allow_html=True)
                st.caption(f"${run['estimated_cost_usd']:.2f}")
            with cols[2]:
                if st.button("Open", key=f"open-{run['id']}", use_container_width=True):
                    st.query_params["run"] = str(run["id"])
                    st.rerun()
            st.markdown("---")


# ─────────────────────────────────────────────────────────────
# Route
# ─────────────────────────────────────────────────────────────

if run_view:
    try:
        render_run_detail(int(run_view))
    except ValueError:
        st.error(f"Invalid run id: {run_view}")
        if st.button("← Back to dashboard"):
            st.query_params.clear()
            st.rerun()
else:
    render_dashboard()
