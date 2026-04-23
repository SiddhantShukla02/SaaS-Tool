# ─── NOTE: This cell now imports keys from config.py ────────
# If you haven't set up config.py yet, see README_REVISION.md
try:
    from config import (SERP_API_KEY, GEMINI_API_KEY,
                         FIRECRAWL_API_KEY, BRAVE_API_KEY,
                         CREDS_FILE, SPREADSHEET_NAME,
                         GEMINI_MODEL, COUNTRY_MAP, SAFETY_OFF, SCOPES)
except ImportError:
    print('⚠️ config.py not found — falling back to globals from Cell 1')
# ────────────────────────────────────────────────────────────

# ─── CELL B: Gemini Insight Extractor + Master Insights Builder ─────
# Reads  : Keyword_n8n > Forum_Master_Raw
# Writes : Keyword_n8n > Forum_Master_Insights  (11 cols, 9-category classification)
#         : Keyword_n8n > Forum_Master_MD        (prompt-ready markdown per category)
#
# 9 insight categories:
#   Emotional_Hook | Patient_Question | Objection       | Trust_Signal
#   Patient_Voice  | Country_Pain_Point | Negative_Signal | Content_Gap
#   Journey_Stage_Signal
#
# Priority_Score 1-10 = source authority + emotion intensity + upvotes + journey stage
#
# Forum_Master_MD has one row per category + an ALL_CATEGORIES row which is a
# drop-in replacement for Reddit_Insights_MD in Cells 21, 23, 25, and 31.
# ─────────────────────────────────────────────────────────────────────

import json
import math
import re
import time
from collections import Counter, defaultdict
import gspread
from google.oauth2.service_account import Credentials
from google import genai
from google.genai import types

# ── Config ────────────────────────────────────────────────────────────
SHEET_NAME    = SPREADSHEET_NAME
SOURCE_TAB    = "Forum_Master_Raw"
OUTPUT_TAB    = "Forum_Master_Insights"
MD_OUTPUT_TAB = "Forum_Master_MD"

CREDS_FILE     = "creds_data.json"

GEMINI_MODEL   = "gemini-2.5-flash"

MAX_CELL   = 49000
BATCH_SIZE = 20     # rows per Gemini call

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

INSIGHT_CATEGORIES = [
    "Emotional_Hook", "Patient_Question", "Objection", "Trust_Signal",
    "Patient_Voice", "Country_Pain_Point", "Negative_Signal",
    "Content_Gap", "Journey_Stage_Signal",
]
INSIGHT_CATEGORY_SET = set(INSIGHT_CATEGORIES)
JOURNEY_STAGES = {"TOFU", "MOFU", "BOFU"}

SOURCE_AUTHORITY  = {"Review": 9, "Quora": 8, "Reddit": 7, "Reddit-Brave": 6, "Forum": 6}
EMOTION_INTENSITY = {
    "urgency": 10, "fear": 9, "trust_deficit": 9, "financial_stress": 8,
    "quality_concern": 8, "overwhelm": 7, "logistics_worry": 6, "hope": 5,
}
HIGH_VALUE_CATEGORIES = {"Content_Gap", "Objection", "Country_Pain_Point"}

# ── Auth ──────────────────────────────────────────────────────────────
creds         = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
gc            = gspread.authorize(creds)
gemini_client = genai.Client(api_key=GEMINI_API_KEY)

SAFETY_OFF = [
    types.SafetySetting(category="HARM_CATEGORY_HARASSMENT",        threshold="OFF"),
    types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH",       threshold="OFF"),
    types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="OFF"),
    types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="OFF"),
    types.SafetySetting(category="HARM_CATEGORY_CIVIC_INTEGRITY",   threshold="OFF"),
]

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
                print(f"    ❌ Write failed: {e}")
                return False

def _fmt_header(ws, n_cols):
    col_letter = chr(64 + min(n_cols, 26))
    ws.format(f"A1:{col_letter}1", {
        "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        "backgroundColor": {"red": 0.13, "green": 0.37, "blue": 0.60},
    })

def get_or_create_tab(spreadsheet, tab_name, rows=5000, cols=15):
    try:
        ws = spreadsheet.worksheet(tab_name)
        ws.clear()
        print(f"  📋 Tab '{tab_name}' cleared.")
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=rows, cols=cols)
        print(f"  📋 Tab '{tab_name}' created.")
    return ws

# ── Gemini call ───────────────────────────────────────────────────────
def call_gemini(prompt: str, max_tokens: int = 8000) -> str:
    for attempt in range(3):
        try:
            resp = gemini_client.models.generate_content(
                model=GEMINI_MODEL, contents=prompt,
                config=types.GenerateContentConfig(
                    max_output_tokens=max_tokens, temperature=0.2,
                    safety_settings=SAFETY_OFF,
                ),
            )
            return resp.text.strip()
        except Exception as e:
            wait = 8 * (attempt + 1)
            if attempt < 2:
                print(f"    ⚠️  Gemini attempt {attempt+1}/3: {e} — retry in {wait}s")
                time.sleep(wait)
            else:
                print(f"    ❌ Gemini failed: {e}")
                return ""
    return ""

# ── JSON cleanup ──────────────────────────────────────────────────────
def parse_gemini_json(raw: str) -> list:
    raw = re.sub(r"^```(?:json)?\s*", "", raw.strip(), flags=re.MULTILINE)
    raw = re.sub(r"^```\s*$",         "", raw.strip(), flags=re.MULTILINE)
    raw = raw.strip()
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        match = re.search(r"\[.*\]", raw, re.DOTALL)
        if match:
            try:
                obj = json.loads(match.group())
            except json.JSONDecodeError:
                return []
        else:
            return []
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for v in obj.values():
            if isinstance(v, list):
                return v
    return []

# ── Priority score ────────────────────────────────────────────────────
def compute_priority_score(row: dict, insight_type: str, journey_stage: str) -> int:
    source   = row.get("Source", "Forum")
    emotions = row.get("Emotion_Tags", "")

    # ✅ FIX: Safely parse upvotes — handles None, "", "3.0", negatives
    try:
        upvotes = max(0, int(float(str(row.get("Upvotes", 0) or 0).strip())))
    except (ValueError, TypeError):
        upvotes = 0

    authority = SOURCE_AUTHORITY.get(source, 5)
    emotion_max = 0
    for emo in emotions.split(","):
        emo = emo.strip()
        if emo in EMOTION_INTENSITY:
            emotion_max = max(emotion_max, EMOTION_INTENSITY[emo])

    # ✅ FIX: max(0, upvotes) ensures log10 never receives 0 or negative
    upvote_bonus = min(5, int(math.log10(max(0, upvotes) + 1) * 2.5))
    stage_bonus  = {"BOFU": 3, "MOFU": 2, "TOFU": 1}.get(journey_stage, 1)
    cat_bonus    = 2 if insight_type in HIGH_VALUE_CATEGORIES else 0
    raw = authority + (emotion_max * 0.3) + upvote_bonus + stage_bonus + cat_bonus
    return min(10, max(1, round(raw / 2.6)))

# ── Batch classification ──────────────────────────────────────────────
def build_classification_prompt(batch: list) -> str:
    rows_text = ""
    for i, row in enumerate(batch):
        rows_text += f"""
ROW {i+1}:
  Source       : {row.get('Source', '')}
  Country      : {row.get('Detected_Country', '')}
  Emotion_Tags : {row.get('Emotion_Tags', '')}
  Text         : {row.get('Insight_Text', '')}
"""
    return f"""You are classifying patient community data for a medical tourism SEO pipeline.
Context: Divinheal — connecting patients from Africa, Middle East, GCC to hospitals in India.

CLASSIFY EACH ROW into EXACTLY ONE of these 9 categories:

1. Emotional_Hook — strong emotion for opening blog sections with empathy
2. Patient_Question — direct question patients are actively asking
3. Objection — specific barrier or hesitation blocking a patient from proceeding
4. Trust_Signal — what specifically convinced a patient to proceed
5. Patient_Voice — striking phrase or expression capturing HOW patients speak
6. Country_Pain_Point — insight SPECIFIC to one country (visa, currency, language)
7. Negative_Signal — warning, complaint, bad experience, or risk factor
8. Content_Gap — question/concern with NO satisfying answer online (high SEO opportunity)
9. Journey_Stage_Signal — reveals WHERE patient is: TOFU/MOFU/BOFU

ALSO assign one of: TOFU | MOFU | BOFU for ALL rows.
AND write a clean_insight: ONE crisp sentence, max 80 characters, written as a pull-quote.

RULES:
- Return ONLY a JSON array
- No preamble, no markdown fences, no text outside JSON

OUTPUT FORMAT:
[
  {{
    "row_index": 1,
    "insight_type": "<one of the 9 categories>",
    "journey_stage": "<TOFU|MOFU|BOFU>",
    "clean_insight": "<max 80 chars, pull-quote style>"
  }}
]

ROWS TO CLASSIFY:
{rows_text}

Return the JSON array now:"""

def _fallback_classifications(batch: list) -> list:
    results = []
    for i, row in enumerate(batch):
        text = str(row.get("Insight_Text", "") or "").lower()
        if "?" in text or text.startswith(("how", "what", "when", "where", "which", "who", "is ", "can ")):
            cat, stage = "Patient_Question", "MOFU"
        elif any(w in text for w in ["scam", "fraud", "bad", "terrible", "wrong", "problem"]):
            cat, stage = "Negative_Signal", "MOFU"
        elif any(w in text for w in ["jci", "nabh", "accredited", "certified", "trusted", "recommend"]):
            cat, stage = "Trust_Signal", "MOFU"
        elif any(w in text for w in ["scared", "afraid", "worried", "anxious", "hope", "amazing"]):
            cat, stage = "Emotional_Hook", "TOFU"
        elif any(w in text for w in ["book", "visa", "document", "payment", "transfer", "how to"]):
            cat, stage = "Journey_Stage_Signal", "BOFU"
        else:
            cat, stage = "Emotional_Hook", "TOFU"
        results.append({
            "row_index": i + 1, "insight_type": cat,
            "journey_stage": stage,
            "clean_insight": str(row.get("Insight_Text", "") or "")[:120].strip(),
        })
    return results

def classify_batch(batch: list) -> list:
    if not batch:
        return []
    raw = call_gemini(build_classification_prompt(batch), max_tokens=BATCH_SIZE * 130)
    if not raw:
        return _fallback_classifications(batch)
    results = parse_gemini_json(raw)
    if len(results) < len(batch):
        fallbacks = _fallback_classifications(batch[len(results):])
        for i, fb in enumerate(fallbacks):
            fb["row_index"] = len(results) + i + 1
        results.extend(fallbacks)
    return results

# ── Build Forum_Master_MD ─────────────────────────────────────────────
def build_master_md(classified_rows: list) -> dict:
    grouped = defaultdict(list)
    for row in classified_rows:
        grouped[row["Insight_Type"]].append(row)
    md_blocks = {}
    ordered_parts = []
    for cat in INSIGHT_CATEGORIES:
        rows_in_cat = grouped.get(cat, [])
        if not rows_in_cat:
            continue
        rows_sorted = sorted(rows_in_cat, key=lambda r: int(r.get("Priority_Score", 1)), reverse=True)
        lines = [f"## {cat} ({len(rows_sorted)} insights)"]
        for r in rows_sorted:
            country = r.get("Detected_Country", "Global")
            source  = r.get("Source", "")
            stage   = r.get("Journey_Stage", "")
            score   = r.get("Priority_Score", "")
            insight = r.get("Clean_Insight", "") or r.get("Insight_Text", "")[:120]
            emotion = r.get("Emotion_Tags", "")
            line = f"- [{source} | {country} | {stage} | P{score}] {insight}"
            if emotion:
                line += f"  _(emotions: {emotion})_"
            lines.append(line)
        block = "\n".join(lines)
        md_blocks[cat] = block
        ordered_parts.append(block)
    md_blocks["ALL_CATEGORIES"] = "\n\n---\n\n".join(ordered_parts)
    return md_blocks

# ── MAIN ──────────────────────────────────────────────────────────────
def main():
    print("\n" + "="*60)
    print("🤖 CELL B — GEMINI INSIGHT EXTRACTOR")
    print("="*60)
    sp = gc.open(SHEET_NAME)

    try:
        ws_src  = sp.worksheet(SOURCE_TAB)
        records = ws_src.get_all_records()
    except gspread.WorksheetNotFound:
        print(f"❌ Tab '{SOURCE_TAB}' not found. Run Cell A first.")
        return
    if not records:
        print(f"❌ Tab '{SOURCE_TAB}' is empty. Run Cell A first.")
        return
    print(f"📥 Loaded {len(records)} rows from '{SOURCE_TAB}'")

    classified_rows = []
    total_batches   = math.ceil(len(records) / BATCH_SIZE)
    print(f"\n🤖 Classifying in {total_batches} batches (batch size: {BATCH_SIZE})...")

    for batch_num in range(total_batches):
        start = batch_num * BATCH_SIZE
        end   = min(start + BATCH_SIZE, len(records))
        batch = records[start:end]
        print(f"  Batch {batch_num+1}/{total_batches} — rows {start+1}–{end}...", end=" ")
        classifications = classify_batch(batch)
        for i, row in enumerate(batch):
            cls = classifications[i] if i < len(classifications) else {}
            insight_type  = cls.get("insight_type",  "Emotional_Hook")
            journey_stage = cls.get("journey_stage",  "TOFU")
            clean_insight = cls.get("clean_insight",  str(row.get("Insight_Text", "") or "")[:120])
            if insight_type  not in INSIGHT_CATEGORY_SET: insight_type  = "Emotional_Hook"
            if journey_stage not in JOURNEY_STAGES:        journey_stage = "TOFU"
            priority = compute_priority_score(row, insight_type, journey_stage)
            classified_rows.append({
                "Source":           row.get("Source",           ""),
                "Detected_Country": row.get("Detected_Country", ""),
                "Insight_Type":     insight_type,
                "Journey_Stage":    journey_stage,
                "Clean_Insight":    clean_insight,
                "Emotion_Tags":     row.get("Emotion_Tags",     ""),
                "Priority_Score":   priority,
                "Insight_Text":     row.get("Insight_Text",     ""),
                "Upvotes":          row.get("Upvotes",          0),
                "URL":              row.get("URL",              ""),
                "Raw_Title":        row.get("Raw_Title",        ""),
            })
        batch_types = Counter(
            classifications[i].get("insight_type", "?")
            for i in range(len(batch))
            if i < len(classifications)
        )
        print(f"done. [{', '.join(f'{v} {k}' for k, v in batch_types.most_common(3))}]")
        time.sleep(1.5)

    print(f"\n✅ Classification complete: {len(classified_rows)} rows")

    # Write Forum_Master_Insights
    headers_insights = [
        "Source", "Detected_Country", "Insight_Type", "Journey_Stage",
        "Clean_Insight", "Emotion_Tags", "Priority_Score",
        "Insight_Text", "Upvotes", "URL", "Raw_Title",
    ]
    ws_insights = get_or_create_tab(sp, OUTPUT_TAB, rows=len(classified_rows)+20, cols=len(headers_insights))
    insights_data = [headers_insights]
    for row in classified_rows:
        insights_data.append([
            _trunc(row["Source"]),         _trunc(row["Detected_Country"]),
            _trunc(row["Insight_Type"]),   _trunc(row["Journey_Stage"]),
            _trunc(row["Clean_Insight"]),  _trunc(row["Emotion_Tags"]),
            str(row["Priority_Score"]),    _trunc(row["Insight_Text"]),
            str(row["Upvotes"]),           _trunc(row["URL"]),
            _trunc(row["Raw_Title"]),
        ])
    if _write_with_retry(ws_insights, insights_data):
        _fmt_header(ws_insights, len(headers_insights))
        print(f"  📊 '{OUTPUT_TAB}' written: {len(classified_rows)} rows × {len(headers_insights)} cols")

    # Write Forum_Master_MD
    md_blocks  = build_master_md(classified_rows)
    headers_md = ["Insight_Type", "Row_Count", "Prompt_Ready_Markdown"]
    ws_md      = get_or_create_tab(sp, MD_OUTPUT_TAB, rows=len(INSIGHT_CATEGORIES)+5, cols=3)
    md_data    = [headers_md]
    for cat in INSIGHT_CATEGORIES:
        if cat in md_blocks:
            block     = md_blocks[cat]
            row_count = block.count("\n- ")
            md_data.append([cat, str(row_count), _trunc(block)])
    full_md = md_blocks.get("ALL_CATEGORIES", "")
    md_data.append(["ALL_CATEGORIES", str(len(classified_rows)), _trunc(full_md)])
    if _write_with_retry(ws_md, md_data):
        _fmt_header(ws_md, 3)
        print(f"  📝 '{MD_OUTPUT_TAB}' written: {len(md_data)-1} category blocks")

    # Summary
    print("\n" + "="*60)
    print("✅ CELL B COMPLETE")
    print("="*60)
    type_counts  = Counter(r["Insight_Type"]  for r in classified_rows)
    stage_counts = Counter(r["Journey_Stage"] for r in classified_rows)
    print("\n📊 Insights by category:")
    for cat in INSIGHT_CATEGORIES:
        count = type_counts.get(cat, 0)
        bar   = "█" * min(count, 30)
        print(f"  {cat:<30s} {count:>4d}  {bar}")
    print("\n🔢 Journey stage distribution:")
    for stage in ["TOFU", "MOFU", "BOFU"]:
        count = stage_counts.get(stage, 0)
        pct   = round(count / len(classified_rows) * 100) if classified_rows else 0
        print(f"  {stage:<6s} {count:>4d} ({pct}%)")
    high_p = [r for r in classified_rows if int(r["Priority_Score"]) >= 7]
    print(f"\n⭐ High-priority (score >= 7): {len(high_p)}")
    print("\n" + "─"*60)
    print("DOWNSTREAM USAGE:")
    print("  Cell 21 (H1/Meta)  → Forum_Master_MD: Emotional_Hook + Patient_Voice rows")
    print("  Cell 23 (Outline)  → Forum_Master_Insights: Content_Gap + Objection + Patient_Question")
    print("  Cell 25 (Empathy)  → Forum_Master_MD: ALL_CATEGORIES row (full drop-in)")
    print("  Cell 31 (Writer)   → Forum_Master_MD: Patient_Voice + Trust_Signal + Negative_Signal")

main()