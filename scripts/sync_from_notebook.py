"""
scripts/sync_from_notebook.py — Re-extract pipeline cells from the source notebook.

Run this whenever you edit the Jupyter notebook and want the changes reflected
in the production SaaS pipeline.

Usage:
    python scripts/sync_from_notebook.py path/to/Complete_Playbook_v16_2_REPURPOSE.ipynb
"""

import json
import sys
from pathlib import Path

CELL_MAP = {
    1:  "cell_01_env_config.py",
    3:  "cell_03_serp_paa.py",
    5:  "cell_05_autocomplete.py",
    7:  "cell_07_related.py",
    9:  "cell_09_scraper.py",
    12: "cell_12_meta_title.py",
    14: "cell_14_keyword_extractor.py",
    16: "cell_16_reddit.py",
    18: "cell_18_brave_forum.py",
    20: "cell_20_forum_combine.py",
    21: "cell_21_forum_classify.py",
    23: "cell_23_shared_utils.py",
    25: "cell_25_h1_meta.py",
    27: "cell_27_outline.py",
    29: "cell_29_empathy_faq.py",
    31: "cell_31_writer_helpers.py",
    33: "cell_33_blog_writer.py",
    35: "cell_35_question_bank.py",
    37: "cell_37_platform_drafts.py",
}


def main():
    if len(sys.argv) != 2:
        print("Usage: python scripts/sync_from_notebook.py <path-to-notebook.ipynb>")
        sys.exit(1)

    nb_path = Path(sys.argv[1])
    if not nb_path.exists():
        print(f"❌ Notebook not found: {nb_path}")
        sys.exit(1)

    nb = json.loads(nb_path.read_text(encoding="utf-8"))
    cells = nb.get("cells", [])

    repo_root = Path(__file__).parent.parent
    out_dir = repo_root / "stages" / "cells"
    out_dir.mkdir(parents=True, exist_ok=True)

    changes = 0
    for idx, filename in CELL_MAP.items():
        if idx >= len(cells):
            print(f"  ⚠️  skipping {filename} — cell index {idx} out of range")
            continue
        cell = cells[idx]
        if cell["cell_type"] != "code":
            print(f"  ⚠️  skipping cell {idx} — not a code cell")
            continue

        new_source = "".join(cell["source"])
        dest = out_dir / filename

        if dest.exists() and dest.read_text(encoding="utf-8") == new_source:
            print(f"  ·  {filename} unchanged")
            continue

        dest.write_text(new_source, encoding="utf-8")
        print(f"  ✓  {filename} updated ({len(new_source)} chars)")
        changes += 1

    print(f"\n{changes} cell(s) updated. "
          f"{'Commit + redeploy.' if changes else 'Nothing to deploy.'}")


if __name__ == "__main__":
    main()
