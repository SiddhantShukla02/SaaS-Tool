# ─────────────────────────────────────────────────────────────
# STAGE 5 WRAPPER
# ─────────────────────────────────────────────────────────────
# PURPOSE:
#   Executes the Stage 5 platform draft generator.
#
# INPUT:
#   - RUN_ID env var
#   - cell_37_platform_drafts.py
#
# PROCESS:
#   - Dynamically loads and executes cell_37
#
# OUTPUT:
#   - Delegated to cell_37
#
# NOTES:
#   - Temporary wrapper
#   - Future refactor: integrate Stage 5 directly into STAGE_CELLS
# ─────────────────────────────────────────────────────────────

import importlib.util
from pathlib import Path

print("\n═══ Stage 5 — Platform Draft Generator ═══\n")

# Load cell_37 dynamically
CELL_B_PATH = Path(__file__).parent / "cell_37_platform_drafts.py"

spec = importlib.util.spec_from_file_location("cell_b", str(CELL_B_PATH))
module = importlib.util.module_from_spec(spec)

# Pass through environment (RUN_ID already set)
spec.loader.exec_module(module)

print("\n✅ Stage 5 complete\n")