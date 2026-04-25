"""
stages/runner.py — Executes extracted pipeline cells as functions.

Each "stage" is one or more of your original Jupyter cells, now saved as .py
files in stages/cells/. The runner executes them via exec() in a clean
namespace that includes your config.

This is the "don't change your code" guarantee in action — the original cell
source is copied verbatim; only the execution context changes.
"""

import os
import sys
import time
import traceback
import contextlib
import io
from pathlib import Path
from typing import Callable

# Make the project root importable so cells can `from config import ...`
ROOT = Path(__file__).parent.parent.resolve()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CELLS_DIR = Path(__file__).parent / "cells"


def _run_cell(cell_file: str, progress_cb: Callable[[str], None] | None = None) -> str:
    """
    Execute a saved cell .py file. Returns captured stdout.

    Uses a clean globals() dict seeded only with __name__ and __file__ so
    the cell behaves as if it were freshly executed in a Jupyter kernel.

    Raises whatever the cell raises. Caller catches and records failure.
    """
    path = CELLS_DIR / cell_file
    if not path.exists():
        raise FileNotFoundError(f"Cell file not found: {path}")

    source = path.read_text(encoding="utf-8")
    code = compile(source, str(path), "exec")

    namespace = {
        "__name__": "__main__",
        "__file__": str(path),
    }

    # Capture stdout for the activity log
    captured = io.StringIO()

    if progress_cb:
        progress_cb(f"▶ started {cell_file}")

    with contextlib.redirect_stdout(captured):
        exec(code, namespace)

    output = captured.getvalue()

    if progress_cb:
        lines = [line.strip() for line in output.splitlines() if line.strip()]

        for line in lines[-25:]:
            progress_cb(f"[{cell_file}] {line}")

        progress_cb(f"✅ finished {cell_file}")

    return output


# ─────────────────────────────────────────────────────────────────
# Stage definitions — each stage is a list of cells to run in order
# ─────────────────────────────────────────────────────────────────

STAGE_CELLS = {
    "stage_1_serp_paa": [
        "cell_01_env_config.py",
        "cell_03_serp_paa.py",
    ],
    "stage_2_context": [
        "cell_01_env_config.py",
        "cell_05_autocomplete.py",
        "cell_07_related.py",
        "cell_16_reddit.py",
        "cell_18_brave_forum.py",
        "cell_20_forum_combine.py",
        "cell_21_forum_classify.py",
    ],
    "stage_3_blog": [
        "cell_01_env_config.py",
        "cell_09_scraper.py",
        "cell_12_meta_title.py",
        "cell_14_keyword_extractor.py",
        "cell_23_shared_utils.py",
        "cell_25_h1_meta.py",
        "cell_27_outline.py",
        "cell_29_empathy_faq.py",
        "cell_31_writer_helpers.py",
        "cell_33_blog_writer.py",
    ],
    "stage_4_bank": [
        "cell_01_env_config.py",
        "cell_35_question_bank.py",
    ],
    "stage_5_drafts": [
        "cell_01_env_config.py",
        "stage5_wrapper.py",
    ],
}


def run_stage(stage_name: str, run_id: int,
                progress_cb: Callable[[str], None] | None = None) -> dict:
    """
    Run a full stage (sequence of cells) for a given run.
    Returns {'status': 'success'/'failed', 'output': str, 'error': str|None}.

    The run_id is made available to cells via the SAAS_RUN_ID env var,
    which cells can optionally read (current cells don't need it since the
    Google Sheet is the shared state).
    """
    if stage_name not in STAGE_CELLS:
        return {
            "status": "failed",
            "output": "",
            "error": f"Unknown stage: {stage_name}",
        }

    os.environ["SAAS_RUN_ID"] = str(run_id)
    outputs = []
    started = time.time()

    try:
        for cell_file in STAGE_CELLS[stage_name]:
            if progress_cb:
                progress_cb(f"→ running {cell_file}")
            output = _run_cell(cell_file, progress_cb)
            outputs.append(f"─── {cell_file} ───\n{output}")

        return {
            "status":    "success",
            "output":    "\n\n".join(outputs)[-4000:],  # last 4K chars for log
            "error":     None,
            "duration":  time.time() - started,
        }

    except Exception as e:
        tb = traceback.format_exc()
        if progress_cb:
            progress_cb(f"❌ failed {cell_file}: {type(e).__name__}: {e}")
            
        return {
            "status":    "failed",
            "output":    "\n\n".join(outputs)[-2000:],
            "error":     f"{type(e).__name__}: {e}\n\n{tb[-1500:]}",
            "duration":  time.time() - started,
        }


if __name__ == "__main__":
    # CLI: python -m stages.runner stage_1_serp_paa 42
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("stage")
    parser.add_argument("run_id", type=int)
    args = parser.parse_args()

    result = run_stage(args.stage, args.run_id, progress_cb=print)
    print(f"\n{'=' * 60}")
    print(f"Status: {result['status']}")
    print(f"Duration: {result.get('duration', 0):.1f}s")
    if result["error"]:
        print(f"Error:\n{result['error']}")
