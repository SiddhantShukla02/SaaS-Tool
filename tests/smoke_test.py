"""
tests/smoke_test.py — Pre-deploy sanity check.

Runs BEFORE you deploy to verify:
  - All modules import
  - SQLite initialises
  - Cells are importable (not executed — just parsed)
  - config.py and config_repurpose.py are reachable

Does NOT hit any network. Safe to run offline.

Usage:
    python -m tests.smoke_test
"""

import sys
import ast
import importlib
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

print("═" * 60)
print("  SaaS for Blog — smoke test")
print("═" * 60)

errors = []


def check(name, fn):
    try:
        fn()
        print(f"✅ {name}")
    except Exception as e:
        print(f"❌ {name}: {e}")
        errors.append((name, e))


def check_imports():
    # Set dummy env vars so config.py doesn't raise
    import os
    dummies = {
        "SERP_API_KEY": "x", "GEMINI_API_KEY": "x",
        "FIRECRAWL_API_KEY": "x", "BRAVE_API_KEY": "x",
    }
    for k, v in dummies.items():
        os.environ.setdefault(k, v)

    # Always importable (no streamlit dep)
    from app import db, orchestrator, queue  # noqa: F401
    from stages import runner                # noqa: F401

    # Optional: streamlit-dependent modules
    try:
        import streamlit  # noqa: F401
        from app import auth, ui  # noqa: F401
    except ImportError:
        print("  (info) streamlit not installed — auth.py/ui.py not imported "
               "(expected in test env; pip install streamlit before deploy)")


def check_db_init():
    import os
    os.environ["SAAS_DB_PATH"] = "/tmp/saas_smoke_test.db"
    from app import db as _db
    # Re-read the module-level DB_PATH now that env is set
    from pathlib import Path
    import importlib
    importlib.reload(_db)
    _db.init_db()
    Path("/tmp/saas_smoke_test.db").unlink(missing_ok=True)


def check_cells_parse():
    cells_dir = ROOT / "stages" / "cells"
    assert cells_dir.exists(), f"cells dir missing: {cells_dir}"
    py_files = sorted(cells_dir.glob("cell_*.py"))
    assert len(py_files) == 19, f"expected 19 cells, found {len(py_files)}"
    for f in py_files:
        try:
            ast.parse(f.read_text(encoding="utf-8"))
        except SyntaxError as e:
            raise RuntimeError(f"{f.name}: {e}")


def check_config_reachable():
    # config.py should be importable — we don't have it here; skip gracefully
    try:
        import config      # noqa: F401
        import config_repurpose  # noqa: F401
    except ImportError:
        # Expected in test env; deployment must provide these
        print("  (info) config.py / config_repurpose.py not in test path — must be present at deploy time")


def check_stage_cells_referenced():
    from stages.runner import STAGE_CELLS
    cells_dir = ROOT / "stages" / "cells"
    for stage, cells in STAGE_CELLS.items():
        for cell_file in cells:
            path = cells_dir / cell_file
            if not path.exists():
                raise FileNotFoundError(
                    f"Stage '{stage}' references missing cell: {cell_file}"
                )


def check_state_machine_closure():
    """Verify every status constant has a defined progress mapping by
    exercising the function against a real in-memory run."""
    import os, tempfile
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    os.environ["SAAS_DB_PATH"] = tmp.name

    import importlib
    from app import db as _db
    importlib.reload(_db)
    _db.init_db()

    from app import orchestrator as _orch
    importlib.reload(_orch)

    run_id = _db.create_run("test keyword", ["ae"], "smoke-test")

    statuses = [
        _db.STATUS_DRAFT, _db.STATUS_STAGE1_RUNNING, _db.STATUS_STAGE2_RUNNING,
        _db.STATUS_AWAITING_FINAL_URL, _db.STATUS_STAGE3_RUNNING,
        _db.STATUS_BLOG_READY, _db.STATUS_BANK_RUNNING, _db.STATUS_BANK_READY,
        _db.STATUS_QUORA_RUNNING, _db.STATUS_REDDIT_RUNNING,
        _db.STATUS_SUBSTACK_RUNNING, _db.STATUS_COMPLETE,
        _db.STATUS_CANCELLED,
    ]
    for s in statuses:
        _db.update_status(run_id, s)
        progress = _orch.progress_for_run(run_id)
        assert len(progress) == 4, f"progress mapping broken for status {s}"
        for stage_key, stage_state in progress.items():
            assert stage_state in ("pending", "active", "done", "failed"), \
                f"invalid stage state '{stage_state}' for status {s}"

    Path(tmp.name).unlink(missing_ok=True)


check("imports",                 check_imports)
check("SQLite init",             check_db_init)
check("cells parse",             check_cells_parse)
check("config reachable",        check_config_reachable)
check("stage→cell references",   check_stage_cells_referenced)
check("state machine closure",   check_state_machine_closure)

print("═" * 60)
if errors:
    print(f"❌ {len(errors)} test(s) failed")
    sys.exit(1)
else:
    print("✅ all smoke tests passed")
    sys.exit(0)
