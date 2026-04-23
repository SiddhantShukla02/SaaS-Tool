# ─── STAGE 5 WRAPPER — Platform-selective draft generator ────────────
#
# Runs only the Quora / Reddit / Substack generator requested via the
# SAAS_PLATFORM env var. Uses the functions defined in Cell B
# (cell_37_platform_drafts.py) without modifying that cell.
#
# Reads:  SAAS_PLATFORM env var — "quora" | "reddit" | "substack" | "all"
# Invokes: the matching generator function from Cell B module-level.
#
# This wrapper is how the UI's three separate buttons route to Cell B without
# editing Cell B to accept a platform argument.
# ─────────────────────────────────────────────────────────────────────

import os
import sys
import importlib.util
import time
from datetime import datetime
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

# Import Cell B module manually (it's a sibling file, not a package)
_CELL_B_PATH = Path(__file__).parent / "cell_37_platform_drafts.py"
_spec = importlib.util.spec_from_file_location("cell_b", str(_CELL_B_PATH))
_cell_b = importlib.util.module_from_spec(_spec)

# Cell B has its own `if __name__ == "__main__"` guard; we want its functions
# and constants loaded but NOT to execute the __main__ block. Since Cell B
# doesn't use __main__ guard and runs on import, we need a cleaner path:
# extract just the generator functions by executing Cell B up to the
# MAIN EXECUTION line only.
#
# Simpler: run Cell B's helper definitions by executing everything BEFORE
# the "# MAIN EXECUTION" marker. But that's fragile.
#
# Cleanest: just import the generators via exec of the file's source in a
# namespace, then pick out the three generator functions and the helpers
# they need — then run only what the user requested.

_SOURCE = _CELL_B_PATH.read_text(encoding="utf-8")
_MAIN_MARKER = "# MAIN EXECUTION"
_split_pos = _SOURCE.find(_MAIN_MARKER)
if _split_pos == -1:
    raise RuntimeError(
        "Could not find '# MAIN EXECUTION' marker in cell_37_platform_drafts.py — "
        "wrapper expects it to separate definitions from execution."
    )

# Execute only the definitions portion in a namespace
_ns: dict = {"__name__": "__cell_b_defs__", "__file__": str(_CELL_B_PATH)}
exec(compile(_SOURCE[:_split_pos], str(_CELL_B_PATH), "exec"), _ns)

# Now _ns has all the functions, including load_question_bank,
# load_forum_voice, load_existing_blog, get_or_create_tab, generate_quora_drafts,
# generate_reddit_drafts, generate_substack_drafts, _trunc, _write_with_retry,
# _fmt_header, and the gc / sp / gemini_client globals.

# ── Run only requested platform ─────────────────────────────────────
platform = os.environ.get("SAAS_PLATFORM", "all").lower()
print(f"\n═══ Stage 5 — platform = '{platform}' ═══\n")

gc_obj = _ns["gc"]
sp = gc_obj.open(_ns["SPREADSHEET_NAME"])

bank = _ns["load_question_bank"](sp)
if not bank:
    print("❌ Question_Bank empty. Run Stage 4 first.")
else:
    print(f"✅ Loaded {len(bank)} questions from Question_Bank")

    forum_voice = _ns["load_forum_voice"](sp)
    blog_ref    = _ns["load_existing_blog"](sp)
    specialty   = bank[0].get("Specialty", "general") if bank else "general"
    REPURPOSE_TABS = _ns["REPURPOSE_TABS"]

    def _persist_drafts(drafts, tab_key, label):
        if not drafts:
            print(f"   ({label}: 0 drafts generated)")
            return
        HEADERS = list(drafts[0].keys())
        rows = [HEADERS] + [[_ns["_trunc"](d[h]) for h in HEADERS] for d in drafts]
        ws = _ns["get_or_create_tab"](
            sp, REPURPOSE_TABS[tab_key],
            rows=max(len(rows) + 20, 100), cols=len(HEADERS),
        )
        _ns["_write_with_retry"](ws, rows)
        _ns["_fmt_header"](ws, len(HEADERS))
        print(f"✅ {len(drafts)} {label} drafts → '{REPURPOSE_TABS[tab_key]}'")

    if platform in ("quora", "all"):
        print("\n── Generating Quora drafts ──")
        drafts = _ns["generate_quora_drafts"](bank, forum_voice, blog_ref, limit=30)
        _persist_drafts(drafts, "quora_drafts", "Quora")

    if platform in ("reddit", "all"):
        print("\n── Generating Reddit drafts ──")
        drafts = _ns["generate_reddit_drafts"](bank, forum_voice, specialty, limit=15)
        _persist_drafts(drafts, "reddit_drafts", "Reddit")

    if platform in ("substack", "all"):
        print("\n── Generating Substack essays ──")
        drafts = _ns["generate_substack_drafts"](bank, blog_ref, max_essays=6)
        _persist_drafts(drafts, "substack_drafts", "Substack")

    print(f"\n✅ Stage 5 complete (platform: {platform})")
