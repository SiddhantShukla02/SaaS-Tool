# ─── NOTE: This cell now imports keys from config.py ────────
# If you haven't set up config.py yet, see README_REVISION.md
try:
    from config import (SERP_API_KEY, GEMINI_API_KEY,
                         FIRECRAWL_API_KEY, BRAVE_API_KEY,
                         SPREADSHEET_NAME,
                         GEMINI_MODEL, COUNTRY_MAP, SAFETY_OFF, SCOPES)
except ImportError:
    print('⚠️ config.py not found — falling back to globals from Cell 1')
# ────────────────────────────────────────────────────────────

# ─── CELL A: Forum Combiner + Deduplicator ──────────────────────────
# Reads  : Keyword_n8n > Reddit_Insights tab
#         : Keyword_n8n > Reddit_Insights_MD tab
#         : Keyword_n8n > Google_Forum_Insights tab
# Writes : Keyword_n8n > Forum_Master_Raw tab
#
# Schema : Source | Detected_Country | Insight_Text | Emotion_Tags |
#          Upvotes | URL | Raw_Title
#
# Deduplication : Jaccard token-overlap >= DEDUP_THRESHOLD collapses to richer row
# Country detection : longest-match keyword scan against 40+ target market signals
# ─────────────────────────────────────────────────────────────────────

import re
import time
import math
from collections import Counter

# ── Config ────────────────────────────────────────────────────────────

MAX_CELL        = 49000
DEDUP_THRESHOLD = 0.80   # Jaccard token overlap ratio for deduplication


# ── Country signal map → ISO alpha-2 (longest match first) ───────────
COUNTRY_SIGNALS = [
    ("south africa", "ZA"), ("saudi arabia", "SA"), ("sri lanka", "LK"),
    ("nigeria", "NG"),  ("nigerian", "NG"), ("lagos", "NG"),
    ("abuja", "NG"),    ("naira", "NG"),
    ("dubai", "AE"),    ("abu dhabi", "AE"), ("sharjah", "AE"),
    ("emirati", "AE"),  ("dirham", "AE"),   (" uae ", "AE"),
    ("riyadh", "SA"),   ("jeddah", "SA"),   (" ksa ", "SA"),
    ("ethiopia", "ET"), ("ethiopian", "ET"), ("addis", "ET"),
    ("kenya", "KE"),    ("kenyan", "KE"),    ("nairobi", "KE"),
    ("johannesburg", "ZA"), ("cape town", "ZA"), (" rand ", "ZA"),
    ("ghana", "GH"),    ("ghanaian", "GH"),  ("accra", "GH"),
    ("britain", "GB"),  ("london", "GB"),    (" nhs ", "GB"),
    (" uk ", "GB"),
    ("america", "US"),  ("insurance", "US"), (" usa ", "US"),
    ("pakistan", "PK"), ("karachi", "PK"),   ("lahore", "PK"),
    ("bangladesh", "BD"), ("dhaka", "BD"),
    ("muscat", "OM"),   (" oman", "OM"),
    ("doha", "QA"),     (" qatar", "QA"),
    ("kuwait", "KW"),   ("bahrain", "BH"),
    ("baghdad", "IQ"),  (" iraq", "IQ"),
    ("colombo", "LK"),
    ("kathmandu", "NP"), ("nepal", "NP"),
    ("singapore", "SG"),
    ("sydney", "AU"),   ("medicare", "AU"),  ("australia", "AU"),
    ("toronto", "CA"),  ("canada", "CA"),
]

# ── Emotion keyword map (same taxonomy as Reddit collector) ───────────
EMOTION_KEYWORDS = {
    "fear":             ["scared", "terrified", "afraid", "worried",
                         "nervous", "anxious", "fear", "panic", "dread"],
    "financial_stress": ["afford", "expensive", "cost", "price", "insurance",
                         "bankrupt", "debt", "saving", "budget", "loan", "money"],
    "trust_deficit":    ["scam", "trust", "legit", "reliable", "fake",
                         "fraud", "shady", "suspicious", "cheat", "rip off"],
    "urgency":          ["urgent", "emergency", "dying", "critical",
                         "immediately", "asap", "desperate", "time running"],
    "hope":             ["hope", "miracle", "success", "recovered", "saved",
                         "worked", "grateful", "blessed", "amazing"],
    "overwhelm":        ["confused", "overwhelmed", "lost", "complicated",
                         "where to start", "don't know", "don\u2019t know"],
    "quality_concern":  ["quality", "hygiene", "clean", "standard", "accredited",
                         "safe", "infection", "complication", "risk", "death"],
    "logistics_worry":  ["visa", "travel", "language", "alone", "companion",
                         "accommodation", "flight", "translator"],
}


# ── Sheet helpers ─────────────────────────────────────────────────────
def _trunc(v):
    s = str(v) if v is not None else ""
    return s[:MAX_CELL] + "\n[TRUNCATED]" if len(s) > MAX_CELL else s


# ── Utility functions ─────────────────────────────────────────────────
def detect_country(text: str) -> str:
    """Scan text for country signals. Returns ISO-2 code or 'Global'."""
    lower = " " + text.lower() + " "
    for signal, code in COUNTRY_SIGNALS:
        if signal in lower:
            return code
    return "Global"

def infer_emotion_tags(text: str) -> str:
    """Keyword-based emotion tagging for rows that lack Reddit emotion flags."""
    lower = text.lower()
    found = []
    for emotion, keywords in EMOTION_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            found.append(emotion)
    return ", ".join(found) if found else ""

def token_overlap(text_a: str, text_b: str) -> float:
    """Jaccard similarity on word tokens — used for deduplication."""
    if not text_a or not text_b:
        return 0.0
    def tokenise(t):
        return set(re.sub(r"[^\w\s]", "", t.lower()).split())
    ta, tb = tokenise(text_a), tokenise(text_b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)

def row_richness(row: dict) -> int:
    """Score a row's richness to decide which duplicate to keep (higher = better)."""
    emotion_count = len([e for e in row.get("Emotion_Tags", "").split(",") if e.strip()])
    upvotes       = int(row.get("Upvotes", 0) or 0)
    has_url       = 5 if row.get("URL", "") else 0
    src_bonus     = {"Quora": 8, "Review": 9, "Reddit": 6, "Reddit-Brave": 5, "Forum": 5}
    return (emotion_count * 10) + int(math.log10(upvotes + 1) * 5) + has_url + src_bonus.get(row.get("Source", ""), 5)

def deduplicate(rows: list) -> list:
    """
    Collapse near-duplicate rows (Jaccard >= DEDUP_THRESHOLD).
    Keeps the richer row (more emotion tags / higher upvotes / has URL).
    O(n^2) — acceptable for typical forum dataset sizes (< 2000 rows).
    """
    kept = []
    for candidate in rows:
        is_dup = False
        for i, existing in enumerate(kept):
            overlap = token_overlap(
                candidate.get("Insight_Text", ""),
                existing.get("Insight_Text", "")
            )
            if overlap >= DEDUP_THRESHOLD:
                if row_richness(candidate) > row_richness(existing):
                    kept[i] = candidate
                is_dup = True
                break
        if not is_dup:
            kept.append(candidate)
    return kept

# ── LOAD FROM DB  ──────────────────────────────────────────────────────────────

from app.database import fetch_all
import os

run_id = int(os.environ.get("RUN_ID"))

def load_reddit_insights():
    rows = fetch_all(
        "SELECT * FROM reddit_insights WHERE run_id = %s",
        (run_id,),
    )
    return [
        {
            "Source": "Reddit",
            "Detected_Country": detect_country(r["title_or_body"]),
            "Insight_Text": r["title_or_body"],
            "Emotion_Tags": r["emotions"],
            "Upvotes": int(r.get("score") or 0),
            "URL": r["url"],
            "Raw_Title": "",
        }
        for r in rows
    ]


def load_reddit_md():
    rows = fetch_all(
        "SELECT * FROM reddit_insights_md WHERE run_id = %s",
        (run_id,),
    )

    from app.storage import r2_get_text

    result = []
    for r in rows:
        md = r2_get_text(r["r2_key"])
        result.append({
            "Source": "Reddit",
            "Detected_Country": detect_country(md),
            "Insight_Text": md,
            "Emotion_Tags": infer_emotion_tags(md),
            "Upvotes": 0,
            "URL": "",
            "Raw_Title": r["keyword"],
        })
    return result


def load_brave_insights():
    rows = fetch_all(
        "SELECT * FROM forum_search_results WHERE run_id = %s",
        (run_id,),
    )

    result = []
    for r in rows:
        insight = f"{r['title']}. {r['snippet']}".strip(". ")

        result.append({
            "Source": r["source_type"],
            "Detected_Country": detect_country(insight),
            "Insight_Text": insight,
            "Emotion_Tags": infer_emotion_tags(insight),
            "Upvotes": 0,
            "URL": r["url"],
            "Raw_Title": r["title"],
        })
    return result

# ── MAIN ──────────────────────────────────────────────────────────────
def main():
    print("\n" + "="*60)
    print("🔗 CELL A — FORUM COMBINER + DEDUPLICATOR")
    print("="*60)

    print("\n📥 Loading all three forum sources...")
    reddit_rows = load_reddit_insights()
    reddit_md   = load_reddit_md()
    brave_rows  = load_brave_insights()

    all_rows = reddit_rows + reddit_md + brave_rows
    print(f"\n📊 Total before dedup : {len(all_rows)}")
    print(f"   Reddit_Insights   : {len(reddit_rows)}")
    print(f"   Reddit_MD         : {len(reddit_md)}")
    print(f"   Brave/Forum       : {len(brave_rows)}")

    if not all_rows:
        print("❌ No rows loaded. Ensure Cells 15 and 17 have run.")
        return

    deduped = deduplicate(all_rows)
    removed = len(all_rows) - len(deduped)
    print(f"\n✂️  After dedup: {len(deduped)} rows ({removed} duplicates removed)")

    from app.repositories.search_repo import insert_forum_master_row

    print("\n💾 Saving forum master rows to DB...")

    for row in deduped:
        insert_forum_master_row(
            run_id=run_id,
            source=_trunc(row["Source"]),
            detected_country=_trunc(row["Detected_Country"]),
            insight_text=_trunc(row["Insight_Text"]),
            emotion_tags=_trunc(row["Emotion_Tags"]),
            upvotes=int(row["Upvotes"] or 0),
            url=_trunc(row["URL"]),
            raw_title=_trunc(row["Raw_Title"]),
        )

    print(f"\n{'='*60}")
    print("✅ CELL A COMPLETE — Forum_Master_Raw written")
    print(f"{'='*60}")
    print(f"  Rows   : {len(deduped)}")
    src_counts = Counter(r["Source"] for r in deduped)
    for src, cnt in src_counts.most_common():
        print(f"  {src:<20s}: {cnt}")
    country_counts = Counter(r["Detected_Country"] for r in deduped)
    print(f"\n  Top countries:")
    for country, cnt in country_counts.most_common(8):
        print(f"  {country:<12s}: {cnt}")
    print("\n→ Run Cell B next to classify all rows into 9 insight categories")

main()
