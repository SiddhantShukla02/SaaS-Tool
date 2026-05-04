# ─── STAGE 5 WRAPPER (POST-MIGRATION) ───
# Simply executes Cell B (platform drafts generator)
# which now handles R2 + DB internally.

import os
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