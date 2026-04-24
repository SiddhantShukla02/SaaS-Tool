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
import gspread
from app.utils.helper import get_sheet_client

# ── Config ────────────────────────────────────────────────────────────
SHEET_NAME     = SPREADSHEET_NAME
REDDIT_TAB     = "Reddit_Insights"
REDDIT_MD_TAB  = "Reddit_Insights_MD"
BRAVE_TAB      = "Google_Forum_Insights"
OUTPUT_TAB     = "Forum_Master_Raw"

MAX_CELL        = 49000
DEDUP_THRESHOLD = 0.80   # Jaccard token overlap ratio for deduplication

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

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

# ── Auth ──────────────────────────────────────────────────────────────
gc = get_sheet_client(SCOPES)

# ── Sheet helpers ─────────────────────────────────────────────────────
def _trunc(v):
    s = str(v) if v is not None else ""
    return s[:MAX_CELL] + "\n[TRUNCATED]" if len(s) > MAX_CELL else s

def _write_with_retry(ws, data, retries=3):
    for attempt in range(retries):
        try:
            ws.update(data, value_input_option="RAW")
            return True
        except Exception as e:
            wait = 4 * (attempt + 1)
            if attempt < retries - 1:
                print(f"    ⚠️  Write attempt {attempt+1}/{retries} failed: {e} — retry in {wait}s")
                time.sleep(wait)
            else:
                print(f"    ❌ Write failed after {retries} attempts: {e}")
                return False

def _fmt_header(ws, n_cols):
    col_letter = chr(64 + min(n_cols, 26))
    ws.format(f"A1:{col_letter}1", {
        "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        "backgroundColor": {"red": 0.13, "green": 0.37, "blue": 0.60},
    })

def get_or_create_tab(spreadsheet, tab_name, rows=5000, cols=10):
    try:
        ws = spreadsheet.worksheet(tab_name)
        ws.clear()
        print(f"  📋 Tab '{tab_name}' cleared.")
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=rows, cols=cols)
        print(f"  📋 Tab '{tab_name}' created.")
    return ws

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

# ── Source loaders ────────────────────────────────────────────────────
def load_reddit_insights(sp) -> list:
    """Load Reddit_Insights tab into normalised rows."""
    rows = []
    try:
        ws      = sp.worksheet(REDDIT_TAB)
        records = ws.get_all_records()
        print(f"  📥 {REDDIT_TAB}: {len(records)} raw rows")
    except gspread.WorksheetNotFound:
        print(f"  ⚠️  Tab '{REDDIT_TAB}' not found — skipping")
        return []
    for r in records:
        raw_text = (
            str(r.get("Title_or_Body", "") or "")
            or str(r.get("Post_Text",    "") or "")
            or str(r.get("Content",      "") or "")
            or str(r.get("Body",         "") or "")
            or str(r.get("Text",         "") or "")
        ).strip()
        if not raw_text or raw_text.lower() in ("nan", "none", ""):
            continue
        emotion_tags = str(r.get("Emotions", r.get("Emotion_Tags", ""))).strip()
        if not emotion_tags or emotion_tags.lower() in ("nan", "none", ""):
            emotion_tags = infer_emotion_tags(raw_text)
        upvotes = r.get("Score", r.get("Upvotes", r.get("Ups", 0)))
        try:
            upvotes = int(float(str(upvotes).replace(",", "")))
        except (ValueError, TypeError):
            upvotes = 0
        url = str(r.get("URL", r.get("Post_URL", r.get("Link", r.get("Permalink", "")))).strip())
        ddddp=len(raw_text)
        rows.append({
            "Source":           "Reddit",
            "Detected_Country": detect_country(raw_text),
            "Insight_Text":     raw_text[:ddddp].strip(),
            "Emotion_Tags":     emotion_tags,
            "Upvotes":          upvotes,
            "URL":              url,
            "Raw_Title":        str(r.get("Title", r.get("Post_Title", ""))).strip(),
        })
    print(f"      → {len(rows)} rows normalised")
    return rows

def load_reddit_md(sp) -> list:
    """Load Reddit_Insights_MD — per-keyword aggregated markdown blocks."""
    rows = []
    try:
        ws      = sp.worksheet(REDDIT_MD_TAB)
        records = ws.get_all_records()
        print(f"  📥 {REDDIT_MD_TAB}: {len(records)} raw rows")
    except gspread.WorksheetNotFound:
        print(f"  ⚠️  Tab '{REDDIT_MD_TAB}' not found — skipping")
        return []
    for r in records:
        md = str(r.get("Prompt_Ready_Markdown", "")).strip()
        dd=len(md)
        print("length of Prompt_Ready_Markdown cell ",dd)
        if not md or md.lower() in ("nan", "none", ""):
            continue
        rows.append({
            "Source":           "Reddit",
            "Detected_Country": detect_country(md),
            "Insight_Text":     md[:dd].strip(),
            "Emotion_Tags":     infer_emotion_tags(md),
            "Upvotes":          0,
            "URL":              "",
            "Raw_Title":        str(r.get("Keyword", "")).strip(),
        })
    print(f"      → {len(rows)} rows normalised")
    return rows

def load_brave_insights(sp) -> list:
    """Load Google_Forum_Insights (Brave) tab into normalised rows."""
    rows = []
    try:
        ws      = sp.worksheet(BRAVE_TAB)
        records = ws.get_all_records()
        print(f"  📥 {BRAVE_TAB}: {len(records)} raw rows")
    except gspread.WorksheetNotFound:
        print(f"  ⚠️  Tab '{BRAVE_TAB}' not found — skipping")
        return []
    for r in records:
        title   = str(r.get("Title",       "") or "").strip()
        snippet = str(r.get("Snippet",     r.get("Description", "")) or "").strip()
        url     = str(r.get("URL",         r.get("Link", "")) or "").strip()
        country = str(r.get("Country_Code","") or "").strip().upper()
        url_l   = url.lower()
        src_raw = str(r.get("Source_Type", "") or "").lower()
        if "quora.com"    in url_l:                           source_type = "Quora"
        elif "reddit.com" in url_l:                           source_type = "Reddit-Brave"
        elif "trustpilot" in url_l:                           source_type = "Review"
        elif "google.com" in url_l and "maps" in url_l:      source_type = "Review"
        elif src_raw:                                          source_type = src_raw.title()
        else:                                                  source_type = "Forum"
        insight_text = f"{title}. {snippet}".strip(". ").strip() if snippet else title
        if not insight_text:
            continue    
        lllpp=len(insight_text)
        rows.append({
            "Source":           source_type,
            "Detected_Country": country if country else detect_country(insight_text),
            "Insight_Text":     insight_text[:lllpp].strip(),
            "Emotion_Tags":     infer_emotion_tags(insight_text),
            "Upvotes":          0,
            "URL":              url,
            "Raw_Title":        title,
        })
    print(f"      → {len(rows)} rows normalised")
    return rows

# ── MAIN ──────────────────────────────────────────────────────────────
def main():
    print("\n" + "="*60)
    print("🔗 CELL A — FORUM COMBINER + DEDUPLICATOR")
    print("="*60)
    sp = gc.open(SHEET_NAME)

    print("\n📥 Loading all three forum sources...")
    reddit_rows = load_reddit_insights(sp)
    reddit_md   = load_reddit_md(sp)
    brave_rows  = load_brave_insights(sp)

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

    headers = ["Source", "Detected_Country", "Insight_Text", "Emotion_Tags", "Upvotes", "URL", "Raw_Title"]
    output_data = [headers]
    for row in deduped:
        output_data.append([
            _trunc(row["Source"]),
            _trunc(row["Detected_Country"]),
            _trunc(row["Insight_Text"]),
            _trunc(row["Emotion_Tags"]),
            str(row["Upvotes"]),
            _trunc(row["URL"]),
            _trunc(row["Raw_Title"]),
        ])

    ws_out = get_or_create_tab(sp, OUTPUT_TAB, rows=len(deduped)+20, cols=len(headers))
    if _write_with_retry(ws_out, output_data):
        _fmt_header(ws_out, len(headers))

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
