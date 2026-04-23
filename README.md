# SaaS for Blog — Divinheal content pipeline

A production wrapper around your existing Jupyter pipeline. Same Gemini
calls, same Google Sheet, same output quality — now with a web UI,
auto-advancing stages, a proper manual gate, and multi-user access.

## Table of contents

1. [What this is](#what-this-is)
2. [Architecture in one diagram](#architecture-in-one-diagram)
3. [Folder layout](#folder-layout)
4. [How a run flows](#how-a-run-flows)
5. [Local dev setup (5 min)](#local-dev-setup-5-min)
6. [Deploy to Railway (15 min)](#deploy-to-railway-15-min)
7. [Self-host with Docker (10 min)](#self-host-with-docker-10-min)
8. [Operational notes](#operational-notes)
9. [When things go wrong](#when-things-go-wrong)
10. [Cost and limits](#cost-and-limits)

---

## What this is

A Streamlit web app that wraps your existing pipeline notebook. It does four
jobs your notebook doesn't:

| Job | How it's done |
|---|---|
| Let multiple people trigger runs from a browser | Streamlit UI, shared-password login |
| Run 20-minute pipelines without UI timeout | Background worker + job queue |
| Remember what happened across restarts | SQLite DB with `runs` and `stage_executions` tables |
| Handle the manual Final_URL curation step | Dedicated "awaiting gate" status with a "ready" button |

**Your existing code is not modified.** Cells 1, 3, 5, 7, 9, 12, 14, 16, 18,
20, 21, 23, 25, 27, 29, 31, 33, 35, 37 from
`Complete_Playbook_v16_2_REPURPOSE.ipynb` were extracted verbatim into
`stages/cells/*.py`. The runner executes them via Python's built-in `exec()`
in the same namespace they'd have in Jupyter. Update your notebook →
re-extract the cells → redeploy. Zero adapter code to maintain.

---

## Architecture in one diagram

```
┌─ Browser ──┐    ┌─ Streamlit UI ──────┐    ┌─ SQLite DB ─┐
│   user     │◀──▶│ dashboard + detail  │◀──▶│   runs      │
└────────────┘    └──────────┬──────────┘    │ stage_execs │
                             │                │ activity_log │
                             ▼                └─────────────┘
                  ┌─ Orchestrator ─────┐             ▲
                  │ state machine +    │             │
                  │ transitions        │─────────────┤
                  └──────────┬─────────┘             │
                             │                        │
                             ▼                        │
                  ┌─ RQ job queue ─┐    ┌─ Worker ───┐│
                  │    (Redis)     │───▶│ executes   ├┘
                  └────────────────┘    │ stage cells│
                                         └──────┬─────┘
                                                │
                                                ▼
                                     ┌─ Your cells (unchanged) ─┐
                                     │ stages/cells/cell_NN.py  │
                                     └──────────┬───────────────┘
                                                │
                                                ▼
                                  ┌─ Google Sheets + Gemini ─┐
                                  │  (same as today)          │
                                  └───────────────────────────┘
```

---

## Folder layout

```
saas-blog/
├── app/
│   ├── db.py                 ← SQLite schema and CRUD
│   ├── orchestrator.py       ← state machine: what runs after what
│   ├── queue.py              ← RQ / thread job queue abstraction
│   ├── auth.py               ← shared-password login for Streamlit
│   └── ui.py                 ← Streamlit pages (dashboard + run detail)
│
├── stages/
│   ├── runner.py             ← executes cells via exec()
│   └── cells/                ← your pipeline cells, extracted verbatim
│       ├── cell_01_env_config.py
│       ├── cell_03_serp_paa.py
│       ├── cell_05_autocomplete.py
│       ├── ... (19 total cells)
│       └── stage5_wrapper.py ← routes to Quora/Reddit/Substack only
│
├── tests/
│   └── smoke_test.py         ← pre-deploy sanity check
│
├── config.py                 ← YOUR existing config (copy into repo root)
├── config_repurpose.py       ← YOUR existing repurpose config (copy in)
├── creds_data.json           ← YOUR Google service account (DO NOT COMMIT)
├── .env                      ← secrets (DO NOT COMMIT)
│
├── requirements.txt
├── Procfile                  ← for Railway
├── railway.json              ← Railway config
├── Dockerfile                ← for self-hosting
├── docker-compose.yml        ← local dev with Redis
├── .env.template             ← copy to .env and fill in
├── .gitignore
└── README.md                 ← this file
```

### Files you must add yourself

After cloning / downloading this repo, add three files from your existing
pipeline folder:

1. `config.py` — your main config with brand, hospitals, citations
2. `config_repurpose.py` — your repurposing config
3. `creds_data.json` — your Google service account credentials

All three sit at the repo root (same level as `app/`).

---

## How a run flows

### Stage 1 — SERP + PAA
User clicks "Start new run" → enters keyword + country codes → UI calls
`orchestrator.start_run()` → DB row created with status `stage_1_running` →
job enqueued → worker picks up and runs:

- `cell_01_env_config.py` (loads your config)
- `cell_03_serp_paa.py` (SerpAPI for PAA + Serp_Url)

On success, orchestrator **auto-advances** to Stage 2.

### Stage 2 — Context collection
Worker runs:
- `cell_05_autocomplete.py`
- `cell_07_related.py`
- `cell_16_reddit.py`
- `cell_18_brave_forum.py`
- `cell_20_forum_combine.py`
- `cell_21_forum_classify.py`

On success, status becomes `awaiting_final_url`. UI shows a prominent gate
card with "Final_URL ready" button and a direct link to the Sheet.

### Manual gate
User opens the Google Sheet, curates the `Final_URL` tab (5–8 competitor
URLs), returns to the UI, clicks the green button. Orchestrator validates
state is `awaiting_final_url` and queues Stage 3.

### Stage 3 — Blog creation
Worker runs:
- `cell_09_scraper.py`
- `cell_12_meta_title.py`
- `cell_14_keyword_extractor.py`
- `cell_23_shared_utils.py`
- `cell_25_h1_meta.py`
- `cell_27_outline.py`
- `cell_29_empathy_faq.py`
- `cell_31_writer_helpers.py`
- `cell_33_blog_writer.py`

Status becomes `blog_ready`. Four buttons appear in UI: **Build Question
Bank**, **Generate Quora**, **Generate Reddit**, **Generate Substack**.
Quora/Reddit/Substack are disabled until Question Bank is built.

### Stage 4 — Question Bank (optional)
User clicks "Build Question Bank" → Stage 4 queued → worker runs Cell A.
Status → `bank_ready`. Now Quora/Reddit/Substack buttons enable.

### Stage 5 — Platform drafts (optional, independently triggerable)
Each platform button starts a separate run of the Stage 5 wrapper with
`SAAS_PLATFORM` env var set. Wrapper invokes only the requested generator
(Quora / Reddit / Substack / all). Status → `complete`.

### Anywhere → failed
If any stage raises, status becomes `failed`, error is stored, UI shows
"Retry failed stage" button.

---

## Local dev setup (5 min)

The fastest way to verify everything works on your laptop before deploying.

```bash
# 1. Create virtualenv
python3 -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate

# 2. Install deps
pip install -r requirements.txt

# 3. Copy your existing config + creds
cp /path/to/your/config.py .
cp /path/to/your/config_repurpose.py .
cp /path/to/your/creds_data.json .

# 4. Create .env from template
cp .env.template .env
# Open .env in a text editor — fill in:
#   SAAS_PASSWORD, SERP_API_KEY, GEMINI_API_KEY, FIRECRAWL_API_KEY, BRAVE_API_KEY
# Leave SAAS_QUEUE_BACKEND=thread (no Redis needed for local dev)

# 5. Smoke test (doesn't hit network)
python -m tests.smoke_test

# 6. Run the UI
streamlit run app/ui.py
# → open http://localhost:8501
# → log in with the password you set in .env
# → click "Start new run"
```

With `SAAS_QUEUE_BACKEND=thread`, jobs run in the same process as the UI.
Good for local dev. Jobs don't survive restart.

---

## Deploy to Railway (15 min)

Railway is the lowest-friction host for this stack. Git push → deployed URL.

### Step 1 — Prepare your Git repo

```bash
cd saas-blog
git init
git add .
git commit -m "Initial production pipeline"
# Push to GitHub (create a private repo)
git remote add origin git@github.com:your-org/saas-blog.git
git push -u origin main
```

**Important:** verify `.gitignore` excludes `.env`, `creds_data.json`,
and `runs.db` before pushing. The `.gitignore` in this repo handles it.

### Step 2 — Create Railway project

1. Go to [railway.app](https://railway.app) → Sign in with GitHub
2. "New Project" → "Deploy from GitHub repo" → select your repo
3. Railway auto-detects Python and runs `streamlit run app/ui.py` per `Procfile`

### Step 3 — Add Redis

Railway dashboard → your project → "+ New" → "Database" → "Add Redis"
Railway auto-injects `REDIS_URL` env var into your services.

### Step 4 — Add the worker service

Your web service already runs from the repo. Now add a separate worker:

Railway dashboard → "+ New" → "Empty Service" → "Deploy from GitHub" →
same repo. Then in the service settings:
- **Custom start command:** `python -m app.queue`
- All env vars (below) copied from the web service

### Step 5 — Set env vars (both services)

Railway dashboard → each service → "Variables" → add these:

```
SAAS_PASSWORD=<pick_a_strong_password>
SAAS_QUEUE_BACKEND=rq
SAAS_DB_PATH=/data/runs.db

SERP_API_KEY=<your_rotated_key>
GEMINI_API_KEY=<your_rotated_key>
FIRECRAWL_API_KEY=<your_rotated_key>
BRAVE_API_KEY=<your_rotated_key>
```

`REDIS_URL` is injected automatically by the Redis plugin. Don't set it
manually.

### Step 6 — Add a volume for the SQLite DB

Railway dashboard → web service → "Volumes" → attach a volume at `/data`.
This makes `runs.db` survive deploys. Without this, every deploy wipes
your run history.

Do the same for the worker service (mount the same volume).

### Step 7 — Upload creds_data.json + config files

Two options:

**Option A (easier, less secure):** commit `creds_data.json`, `config.py`,
`config_repurpose.py` to a **private** repo. They ship with the deploy.

**Option B (recommended for real SaaS):** store as base64-encoded env vars,
decode at startup. Write a tiny `bootstrap.py` that reads
`CREDS_DATA_B64` env var and writes `creds_data.json` to disk before
Streamlit starts.

For your team's internal tool on a private repo, Option A is fine.

### Step 8 — Open the URL

Railway gives you a `<project>-<service>.up.railway.app` URL. Open it,
log in, start a run. Watch the worker logs in the Railway dashboard.

### Step 9 — Custom domain (optional)

Railway dashboard → service → "Settings" → "Domains" → add your domain
(e.g. `pipeline.divinheal.com`). Point DNS as Railway instructs. HTTPS
auto-provisioned.

### Step 10 — Verify end-to-end

From the deployed URL:
1. Log in
2. Make sure your Google Sheet `keyword` tab has a test row
3. Start a run with that keyword
4. Watch status transitions in near-real-time
5. When `awaiting_final_url` hits, curate in Sheet, click ready
6. Verify blog appears in `Blog_Output` tab

---

## Self-host with Docker (10 min)

If you prefer your own server:

```bash
# Copy your config + creds into the repo root
cp /path/to/config.py /path/to/config_repurpose.py /path/to/creds_data.json .

# Create .env
cp .env.template .env
# Edit .env — set SAAS_PASSWORD and the 4 API keys
# Also set SAAS_QUEUE_BACKEND=rq and REDIS_URL=redis://redis:6379

# Build + run
docker-compose up --build -d

# → http://localhost:8501
# Logs:
docker-compose logs -f worker
```

The compose file runs three services: `web` (Streamlit), `worker` (RQ
worker), `redis`. SQLite persists in a named volume `saas_data`.

---

## Operational notes

### Changing the pipeline
Edit your notebook as usual → re-export the changed cell(s):

```python
import json
nb = json.load(open('Complete_Playbook_v16_2_REPURPOSE.ipynb'))
open('stages/cells/cell_33_blog_writer.py', 'w').write(
    ''.join(nb['cells'][33]['source'])
)
```

Commit, push, redeploy. Or script this into a `scripts/sync_from_notebook.py`.

### Adding a team member
They go to the URL, enter the shared `SAAS_PASSWORD`, enter their name
at login → their name is recorded in `created_by` on their runs.

### Rotating a key
Railway dashboard → variables → update → redeploy. Old runs in flight will
fail with a clear error; retry them after redeploy.

### Log retention
Activity log grows ~5KB per run. After 1,000 runs = ~5MB. Prune with:

```sql
DELETE FROM activity_log WHERE timestamp < date('now', '-90 days');
VACUUM;
```

Run from `sqlite3 /data/runs.db` shell.

### Backing up
```bash
# From Railway CLI
railway run sqlite3 /data/runs.db ".backup /tmp/backup.db"
railway run cat /tmp/backup.db > backup-$(date +%F).db
```

Once a week is plenty.

---

## When things go wrong

### "Run stuck in stage_X_running"
Worker crashed or disconnected. Check worker logs. Simplest recovery:

```python
# From a Python shell on the server
from app import db
db.update_status(42, db.STATUS_FAILED, error_message="stuck; manual reset")
```

Then retry from UI.

### "Worker not picking up jobs"
1. Check Redis is reachable: `railway run redis-cli -u $REDIS_URL ping`
   → should return `PONG`
2. Check worker logs for startup errors
3. Check `SAAS_QUEUE_BACKEND=rq` is set on both services

### "Login screen won't accept password"
- `SAAS_PASSWORD` env var may not be set; check Railway dashboard variables
- Must match exactly — including trailing whitespace accidentally added
  when pasting

### "Gemini returns 429"
You've hit your rate limit. Gemini Free is 15 RPM, 1M tokens/day.
For production, use a paid key with higher limits. Worker will retry
with exponential backoff three times, then fail the stage.

### "Final_URL tab has data but clicking the button does nothing"
The UI only enables the button when the run's DB status is
`awaiting_final_url`. If the status is `stage_2_running` still, Stage 2
is actually still running — wait for it to finish. Check worker logs.

---

## Cost and limits

### Hosting
- Railway starter: $5/mo flat — covers web + worker + Redis for a team
  of 2–5. ~500 runs/month comfortably.
- DigitalOcean droplet: $6/mo — more control, more setup.
- Fly.io: $0–10/mo for similar spec.

### Pipeline
Per keyword-run (blog only, no Stage 5):
- SerpAPI: 1 call × $0.005 = $0.005
- Gemini 2.5 Flash: ~100K tokens × $0.0075 per 1K = $0.75
- Firecrawl: 5–8 URLs × $0.002 = $0.015
- Brave: ~10 calls free-tier

Total: **~$0.80 per blog** (unchanged from your notebook baseline).

With Stage 5 repurposing: add ~$0.10 per full run (all three platforms).

### Rate limits to watch
- **Gemini Free:** 15 RPM, 1M tokens/day. One run burns ~150K tokens;
  you can do ~6 runs/day on free tier.
- **Gemini Paid:** 1000 RPM, no daily cap. Billed per token.
- **Google Sheets API:** 60 read/write requests per minute per user.
  Pipeline respects this via the `_write_with_retry` in your cells.

---

## What's NOT in this build (deferred to Phase 2)

1. **Per-user accounts.** Shared password is fine for your team. When you
   want per-user audit trails, drop in `streamlit-authenticator` or
   Google OAuth (~30 lines).
2. **Slack notifications.** Easy to add: POST to webhook in
   `orchestrator.on_stage_finished`.
3. **Run cancellation while in-flight.** Current cancel marks the DB row
   cancelled but doesn't kill the running worker job. RQ supports this
   via `job.cancel()` — add later.
4. **Substack API auto-publish.** Drafts land in the sheet; manual post
   today. Substack's API is straightforward to wire in.
5. **Concurrent multi-user run isolation.** If two people start runs
   for the same keyword simultaneously, the Google Sheet will get
   stomped. Add a `keyword + user` uniqueness check in `orchestrator.start_run`
   when it becomes a real problem.

None of these block getting value from the pipeline as-is.
