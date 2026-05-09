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
from streamlit_autorefresh import st_autorefresh

# Set page config FIRST — before any other st.* call
st.set_page_config(
    page_title="SaaS for Blog — Divinheal",
    page_icon="📝",
    layout="wide",
    initial_sidebar_state="collapsed",
)

from app import orchestrator, auth          # noqa: E402
from app.repositories import state_repo as db
from app.repositories.run_repo import get_run_country_codes
from app.repositories.search_repo import get_serp_urls_for_run, save_selected_urls

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
    db.STATUS_AWAITING_FINAL_URL: ("Awaiting URL selection", "pill-gate"),
    db.STATUS_STAGE3_RUNNING:     ("Stage 3 running (Blog writing)", "pill-running"),
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
        st.markdown(f"### {run['primary_keyword']}")
        st.caption(
            f"Run #{run['id']} · created by {run['created_by']} · "
            f"{str(run['created_at']).replace('T', ' ')}"
        )
    with col_r:
        st.markdown(status_pill(run["status"]), unsafe_allow_html=True)

    # Info tiles
    m1, m2, m3 = st.columns(3)
    with m1:
        country_codes = get_run_country_codes(run["id"])
        st.metric("Target countries", ", ".join(country_codes) or "—")
    with m2:
        st.metric("Run ID", f"#{run['id']}")
    with m3:
        from app.database import fetch_one

        blog_row = fetch_one(
            """
            SELECT r2_key
            FROM generated_outputs
            WHERE run_id = %s AND output_type = %s
            ORDER BY id DESC
            LIMIT 1
            """,
            (run["id"], "blog"),
        )

        if blog_row:
            st.markdown(f"**Blog:** `{blog_row['r2_key']}`")
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
        "stage_1_serp_paa": "Stage 1 — Search discovery (Cells 1,3,5,7)",
        "stage_2_context":  "Stage 2 — Context build (Competitor + Forums)",
        "stage_3_blog":     "Stage 3 — Blog writing (Cells 23,25,27,29,31,33)",
        "stage_4_bank":     "Stage 4 — Question Bank",
        "stage_5_drafts":   "Stage 5 — Platform Drafts",
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
            'Select or paste the competitor URLs you want to use. '
            'These will be saved to the database before context extraction runs.'
            '</div>',
            unsafe_allow_html=True,
        )

        serp_rows = get_serp_urls_for_run(run_id)

        st.markdown("##### SERP URLs found")

        selected_from_serp = []

        for row in serp_rows:
            label = f"{row['rank']}. {row['url']}"
            checked = st.checkbox(label, key=f"serp-url-{row['id']}")
            if checked:
                selected_from_serp.append(row["url"])

        manual_urls_raw = st.text_area(
            "Additional/manual URLs, one per line",
            placeholder="https://example.com/page-1\nhttps://example.com/page-2",
        )
        col_a, col_b = st.columns([1, 1])
        with col_a:
            if st.button("✓ URLs ready, run Stage 2 - Context build", type="primary",
                        use_container_width=True):
                try:
                    manual_urls = [
                        url.strip()
                        for url in manual_urls_raw.splitlines()
                        if url.strip()
                    ]

                    final_urls = list(dict.fromkeys(selected_from_serp + manual_urls))

                    if not final_urls:
                        st.error("Please select or enter at least one URL.")
                    else:
                        save_selected_urls(run_id, final_urls)
                        orchestrator.mark_final_url_ready(run_id, auth.current_user())
                        st.success("URLs saved. Stage 2 - Context build is queued!")
                        st.rerun()

                except Exception as e:
                    st.error(str(e))
        with col_b:
            if st.button("Cancel run", use_container_width=True):
                orchestrator.cancel_run(run_id, auth.current_user())
                st.rerun()

    elif run["status"] == db.STATUS_FAILED:
        failed_stage = run.get("failed_at_stage", "unknown")

        if failed_stage == "stage_5_drafts":
            st.warning(
                "Platform draft generation failed. "
                "Your blog and question bank are still available."
            )
        else:
            st.error(f"Failed at: {failed_stage}")

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
        st.info("🔄 Currently running. This page auto-refreshes every 2s.")

        if st.button("🛑 Stop run", type="secondary"):
            orchestrator.cancel_run(run_id, auth.current_user())
            st.warning("Stop requested. The current cell may finish, but no further cells/stages will start.")
            st.rerun()
            
        # Auto-refresh while running without blocking Streamlit rendering.
        st_autorefresh(
            interval=2000,
            key=f"run-{run_id}-autorefresh",
        )

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
        for a in (activity):
            prefix = {"error": "🔴", "warn": "🟡", "info": "·"}.get(a["level"], "·")
            from datetime import datetime
            from zoneinfo import ZoneInfo

            timestamp_text = str(a["timestamp"])

            try:
                dt = datetime.fromisoformat(timestamp_text.replace("Z", "+00:00"))
                dt_ist = dt.astimezone(ZoneInfo("Asia/Kolkata"))
                ts = dt_ist.strftime("%H:%M:%S")
            except Exception:
                ts = timestamp_text[11:19]  # fallback

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
            "**Step 1:** Add one keyword-country pair per row.  \n"
            "**Step 2:** Click Start to begin Stage 1 - Search discovery."
        )
        st.caption(
            "Keyword text is preserved exactly except leading/trailing spaces. "
            "Country codes are normalized to lowercase."
        )

        default_keyword_rows = [
            {"keyword": "", "country_code": ""},
            {"keyword": "", "country_code": ""},
            {"keyword": "", "country_code": ""},
        ]

        with st.form("new_run"):
            keyword_table = st.data_editor(
                default_keyword_rows,
                column_config={
                    "keyword": st.column_config.TextColumn(
                        "Keyword",
                        help="Exact search keyword/topic to run",
                        required=False,
                    ),
                    "country_code": st.column_config.TextColumn(
                        "Country code",
                        help="2-letter country code, e.g. uk, ae, ng",
                        max_chars=2,
                        required=False,
                    ),
                },
                num_rows="dynamic",
                use_container_width=True,
                hide_index=True,
                key="keyword_country_editor",
            )

            submit = st.form_submit_button("Start run", type="primary")

        if submit:
            keyword_rows = []

            for row in keyword_table:
                keyword = str(row.get("keyword", "")).strip()
                country_code = str(row.get("country_code", "")).strip().lower()

                if not keyword and not country_code:
                    continue

                keyword_rows.append({
                    "keyword": keyword,
                    "country_code": country_code,
                })

            if not keyword_rows:
                st.error("Add at least one keyword-country pair.")
            else:
                try:
                    run_id = orchestrator.start_run(
                        keyword_rows,
                        auth.current_user(),
                    )
                    st.success(f"Run #{run_id} queued — Stage 1 starting")
                    st.query_params["run"] = str(run_id)
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

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
                st.markdown(f"**{run['primary_keyword']}**")
                country_codes = get_run_country_codes(run["id"])

                st.caption(
                    f"Run #{run['id']} · {', '.join(country_codes) or '—'} · "
                    f"{str(run['created_at']).replace('T', ' ')[:16]}"
                )
                st.markdown(
                    progress_bar_html(orchestrator.progress_for_run(run['id'])),
                    unsafe_allow_html=True,
                )
            with cols[1]:
                st.markdown(status_pill(run["status"]), unsafe_allow_html=True)
                st.caption("")
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