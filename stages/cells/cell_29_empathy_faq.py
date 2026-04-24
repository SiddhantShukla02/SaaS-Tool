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


from stages.cells.cell_23_shared_utils import *

# ─── CELL: Empathy Hooks + Forum-Enriched FAQ Answers ────────────────
# SINGLE-ARTICLE PARADIGM:
#   Reads the ONE outline row and ONE Reddit insights set.
#   Generates empathy hooks + FAQ answers for the single article.
#
# Reads  : Keyword_n8n > Blog_Outline tab        (single row with outline + H1)
#         : Keyword_n8n > Forum_Master_MD tab     (ALL_CATEGORIES row — replaces Reddit_Insights_MD)
#         : Keyword_n8n > keyword tab             (fallback)
# Writes : Keyword_n8n > Empathy_FAQ_Output tab   (ONE row)
# ─────────────────────────────────────────────────────────────────────

import json, time, re
import gspread
from google import genai
from app.utils.helper import get_sheet_client
from google.genai import types

# ── Config ────────────────────────────────────────────────────────────
SHEET_NAME        = SPREADSHEET_NAME
OUTLINE_TAB       = "Blog_Outline"
FORUM_MD_TAB      = "Forum_Master_MD"   # ← UPDATED: replaces Reddit_Insights_MD
KEYWORD_TAB       = "keyword"
OUTPUT_TAB        = "Empathy_FAQ_Output"


GEMINI_MODEL      = "gemini-2.5-flash"
MAX_CELL          = 49000

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

COUNTRY_MAP = {
    "ng": "Nigeria", "ae": "UAE", "gb": "UK", "us": "USA",
    "et": "Ethiopia", "ke": "Kenya", "za": "South Africa",
    "sa": "Saudi Arabia", "pk": "Pakistan", "bd": "Bangladesh",
    "au": "Australia", "ca": "Canada", "sg": "Singapore",
    "in": "India",
}

# ── Auth ──────────────────────────────────────────────────────────────
gc = get_sheet_client(SCOPES)
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
                time.sleep(wait)
            else:
                print(f"  ❌ Sheet write failed: {e}")
                return False

def _fmt_header(ws, n_cols):
    col_letter = chr(64 + min(n_cols, 26))
    ws.format(f"A1:{col_letter}1", {
        "textFormat": {"bold": True, "foregroundColor": {"red":1,"green":1,"blue":1}},
        "backgroundColor": {"red": 0.13, "green": 0.37, "blue": 0.60}
    })

def get_or_create_tab(spreadsheet, tab_name, rows=200, cols=10):
    try:
        ws = spreadsheet.worksheet(tab_name)
        ws.clear()
        print(f"  📋 Tab '{tab_name}' cleared.")
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=rows, cols=cols)
        print(f"  📋 Tab '{tab_name}' created.")
    return ws

# FIX #1: resp.text instead of parts[0].text (Gemini 2.5 Flash thinking bug)
# FIX #2: max_tokens 6000 → 16000 (thinking consumes ~5500 tokens)
def call_gemini(prompt, max_tokens=16000):   # ← FIX #2: was 6000
    for attempt in range(3):
        try:
            resp = gemini_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    max_output_tokens=max_tokens,
                    safety_settings=SAFETY_OFF,
                ),
            )
            if resp.candidates:
                return resp.text.strip()     # ← FIX #1: was parts[0].text
        except Exception as e:
            print(f"  ⚠️ Gemini attempt {attempt+1}/3: {e}")
            if attempt < 2:
                time.sleep(6 * (attempt + 1))
    return ""


# ═══════════════════════════════════════════════════════════════
# DATA LOADERS
# ═══════════════════════════════════════════════════════════════

def load_article_outline(sp):
    """Load the single-row article outline from Blog_Outline tab."""
    try:
        ws = sp.worksheet(OUTLINE_TAB)
        records = ws.get_all_records()
        if records:
            r = records[0]
            return {
                "primary_keyword": str(r.get("Primary_Keyword", "")).strip(),
                "secondary_keywords": str(r.get("Secondary_Keywords", "")).strip(),
                "target_countries": str(r.get("Target_Countries", "")).strip(),
                "chosen_h1": str(r.get("Chosen_H1", "")).strip(),
                "outline": str(r.get("Complete_Outline", "")).strip(),
            }
    except Exception as e:
        print(f"  ⚠️ Blog_Outline tab not found: {e}")
    return None


# FIX #3: Prefer ALL_CATEGORIES row (pre-merged by Cell 19)
def load_all_forum_data(sp):
    """Load forum insights — prefers ALL_CATEGORIES row, falls back to merge."""
    try:
        ws      = sp.worksheet(FORUM_MD_TAB)
        records = ws.get_all_records()
    except Exception:
        # Fallback: try legacy Reddit_Insights_MD
        try:
            ws      = sp.worksheet("Reddit_Insights_MD")
            records = ws.get_all_records()
            all_md  = [str(r.get("Prompt_Ready_Markdown","")).strip() for r in records]
            return "\n\n---\n\n".join(m for m in all_md if m and m != "nan")
        except Exception:
            return ""

    # Prefer ALL_CATEGORIES row (all 9 categories pre-merged)
    for r in records:
        if str(r.get("Insight_Type","")).strip() == "ALL_CATEGORIES":
            md = str(r.get("Prompt_Ready_Markdown","")).strip()
            if md and md != "nan":
                print(f"  📣 Forum_Master_MD (ALL_CATEGORIES): {len(md):,} chars")
                return md

    # Fallback: merge individual category rows
    all_md = []
    for r in records:
        md = str(r.get("Prompt_Ready_Markdown","")).strip()
        if md and md != "nan":
            all_md.append(md)
    merged = "\n\n---\n\n".join(all_md)
    print(f"  📣 Forum_Master_MD (merged {len(all_md)} categories): {len(merged):,} chars")
    return merged


def extract_from_outline(outline_text, pattern, group=1):
    """Extract items from outline text using a regex pattern."""
    matches = re.findall(pattern, outline_text, re.MULTILINE)
    return matches


# ═══════════════════════════════════════════════════════════════
# PROMPT BUILDERS
# ═══════════════════════════════════════════════════════════════

def build_empathy_hooks_prompt(article_data, forum_data):
    primary = article_data["primary_keyword"]
    countries = article_data["target_countries"]
    chosen_h1 = article_data["chosen_h1"]
    outline = article_data["outline"]

    # Extract H2 sections that need hooks
    h2_sections = re.findall(r'^H2:\s*(.+?)(?:\s*—|$)', outline, re.MULTILINE)
    # Generate hooks for ALL outline sections (not just 5 fixed types)
    hook_sections = []
    for h2 in h2_sections:
        if h2.lower().strip() and "faq" not in h2.lower() and "frequently" not in h2.lower():
            hook_sections.append(h2.strip())

    sections_block = "\n".join(f"  {i}. {s}" for i, s in enumerate(hook_sections, 1))
    # Build dynamic hook labels for the output format
    dynamic_hook_labels = "\n\n".join(f"{s} Hook:\n[2-3 sentences]" for s in hook_sections)
    # FIX A: Cap forum data at 2000 chars (was sending ALL 34,625 chars)
    # Full dump buried the writing instructions — Gemini focused on forum data, not hooks
    forum_snippet = forum_data[:10000] if forum_data else "(No forum data available — use general patient empathy patterns for medical tourism)"

    return f"""You are a senior medical tourism consultant at Divinheal who has helped
thousand of patients from {countries} plan their treatment in India.

TASK: Write empathy hooks (2-3 sentence openers) for each blog section listed below.
These hooks go at the START of each section to connect emotionally before factual content.

ARTICLE: {chosen_h1}
KEYWORD: {primary}
COUNTRIES: {countries}

SECTIONS NEEDING HOOKS:
{sections_block}

PATIENT CONCERNS (from Reddit/forums):
{forum_snippet}

HOOK FORMULA (use for every hook):
Sentence 1: Name the specific worry. Mirror patient language from the forum data above.
Sentence 2: Validate — "You're not alone" or "This is the first question every patient asks."
Sentence 3: Pivot to what this section answers. Give a reason to keep reading.

RULES:
- Each hook: EXACTLY 2-3 sentences.
- Use "you" — speak directly to the patient.
- BANNED phrases: "In this section", "Let's explore", "Read on to find out", "We understand that",
  "Many patients, like you", "paving the way", "Are you asking yourself", "You're not alone",
  "This guide is here to provide", "your confident new self"
- Each hook MUST start with a DIFFERENT first word. No two hooks can begin the same way.
- Use contractions: "you've", "it's", "that's", "won't".
- Use the patient's actual words from the forum data.
- Hooks must work for patients from all target countries ({countries}).

OUTPUT — plain text, one hook per section:

Introduction Hook:
[2-3 sentences]

{dynamic_hook_labels}

CTA/Conclusion Hook:
[2-3 sentences]"""


def extract_medical_data_from_competitors(sp):
    """Dynamically extract cost figures and hospital names from competitor scraped content."""
    try:
        ws = sp.worksheet("Url_data_ext")
        records = ws.get_all_records()
    except Exception:
        return "(No competitor data available — use your medical knowledge for approximate figures.)"

    # Combine all competitor text
    all_text = ""
    for r in records:
        text = str(r.get("Texts_Only", "")).strip()
        faqs = str(r.get("FAQs", "")).strip()
        others = str(r.get("Others", "")).strip()
        all_text += f" {text[:len(text)]} {faqs} {others}"

    if len(all_text) < 100:
        return "(Limited competitor data — use general medical tourism pricing knowledge.)"

    # Use Gemini to extract structured data
    extraction_prompt = f"""Extract medical cost data from competitor content below.
Return ONLY a bullet-point list.

EXTRACT THESE DATA POINTS:
- Cost per treatment cycle in India: ₹ amount ($ USD equivalent)
- Cost for full course in India: ₹ amount ($ USD equivalent)
- Same treatment cost in UK: £ amount
- Same treatment cost in UAE: AED or $ amount
- Percentage savings India vs UK/UAE
- Hospital names mentioned (with city)
- Accreditations: NABH, JCI, NABL, etc.
- Treatment duration/timeline
- Success rates or survival rates
- City-specific pricing (Delhi, Mumbai, Chennai, Bangalore, etc.)

Rules:
- Always show ₹ first, then $ equivalent.
- If data not found, write "Not found in competitor data".
- Note which competitor URL each data point comes from if possible.

COMPETITOR CONTENT:
{all_text[:len(all_text)]}"""

    result = call_gemini(extraction_prompt, max_tokens=2000)
    if result:
        return result
    return "(Could not extract medical data — use general knowledge.)"


def load_competitor_faq_answers(sp):
    """
    Load competitor FAQ Q&A pairs from Url_data_ext → FAQs column.
    Returns formatted string of competitor questions + answers with real data.
    These are passed to P8 as REFERENCE ANSWERS alongside medical_data_block.
    """
    try:
        ws = sp.worksheet("Url_data_ext")
        records = ws.get_all_records()
    except Exception:
        return ""
    
    qa_pairs = []
    seen_questions = set()
    
    for r in records:
        faq_raw = str(r.get("FAQs", "")).strip()
        url = str(r.get("URL", ""))[:60]
        if not faq_raw or faq_raw == "nan":
            continue
        
        # Parse Q/A pairs
        pairs = re.findall(
            r'Q\d+\s*:\s*(.+?)\s*\nA\d+\s*:\s*(.+?)(?=\nQ\d+\s*:|$)',
            faq_raw, re.DOTALL
        )
        for q, a in pairs:
            q = q.strip()
            a = a.strip()
            if q and a and len(q) > 10 and q.lower() not in seen_questions:
                qa_pairs.append({"q": q, "a": a[:200], "source": url})
                seen_questions.add(q.lower())
    
    if not qa_pairs:
        return ""
    
    # Format for prompt injection
    lines = ["COMPETITOR FAQ ANSWERS (use these real figures in YOUR answers):"]
    for i, pair in enumerate(qa_pairs[:10], 1):
        lines.append(f"  Q: {pair['q']}")
        lines.append(f"  A: {pair['a']}")
        lines.append(f"  Source: {pair['source']}")
        lines.append("")
    
    return "\n".join(lines)


def build_faq_answers_prompt(article_data, forum_data):
    primary = article_data["primary_keyword"]
    countries = article_data["target_countries"]
    chosen_h1 = article_data["chosen_h1"]
    outline = article_data["outline"]

    # Dynamically extract medical data from competitor content
    medical_data_block = extract_medical_data_from_competitors(sp)
    
    # Load competitor FAQ answers — real Q&A pairs with actual ₹ figures
    comp_faq_answers = load_competitor_faq_answers(sp)

    # Use the faq_questions already extracted in main execution
    faq_block = "\n".join(f"  {i}. {q}" for i, q in enumerate(faq_questions[:len(faq_questions)], 1))
    return f"""You are a senior medical tourism consultant at Divinheal.

TASK: Write FAQ answers for patients from {countries} considering treatment in India.

MEDICAL DATA (use these real figures in your answers):
{medical_data_block}

{comp_faq_answers}

ARTICLE: {chosen_h1}
KEYWORD: {primary}
COUNTRIES: {countries}

FAQ QUESTIONS:
{faq_block}

ANSWER RULES:
1. Each answer: 50-80 words. HARD LIMIT: 80 words. If your answer exceeds 80 words, CUT the last sentence.
2. FIRST SENTENCE: direct factual answer with a number or data point.
   This sentence must work as a Google featured snippet on its own.
   Start with the answer, NOT the context (e.g., "₹45,000–₹2,50,000" not "The cost depends on...").
3. Sentences 2-3: add context, comparison, or reassurance.
4. Show costs in ₹ first, then USD/£/AED equivalent.
5. Reference Divinheal naturally in 3-4 answers (not all).
6. Add "Individual outcomes vary — consult your specialist." on 2 clinical answers.
7. Say "patients traveling to India" — never assume the reader's country.
8. NO filler openers: "Great question", "That's a common concern", "Absolutely", "Yes,".
9. For clinical questions, cite a specific accreditation or guideline (e.g., "per NABH standards" or "according to WHO guidelines").

OUTPUT — one answer per question:

Q1: [Question]
A: [50-80 words, fact-first]

Q2: [Question]
A: [50-80 words]"""


# ═══════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════

print("\n" + "="*65)
print("💬 EMPATHY HOOKS + FORUM-ENRICHED FAQ ANSWERS")
print("   Mode: SINGLE ARTICLE")
print("="*65)

sp = gc.open(SHEET_NAME)

article_data = load_article_outline(sp)

if not article_data:
    print("❌ No outline found. Run the Blog Outline cell first.")
else:
    print(f"  📌 H1: {article_data['chosen_h1'][:70]}")
    print(f"  🌍 Countries: {article_data['target_countries']}")

    # Load ALL forum data (merged across all keyword variations)
    forum_data = load_all_forum_data(sp)
    print(f"  📣 Forum data: {'✅ ' + str(len(forum_data)) + ' chars' if forum_data else '⚠️ Not available'}")

    # Extract medical data from competitors (used in FAQ, also available for hooks)
    print(f"\n  📊 Extracting medical data from competitor content...")
    medical_data = extract_medical_data_from_competitors(sp)
    print(f"  {'✅ Extracted' if 'Not available' not in medical_data[:50] else '⚠️ Limited data'}")

    # Generate empathy hooks
    print(f"\n  🤖 Generating empathy hooks...")
    hooks_result = call_gemini(build_empathy_hooks_prompt(article_data, forum_data), max_tokens=3000)
    time.sleep(4)

# Generate FAQ answers
    faq_result = ""
    outline = article_data["outline"]

    # Extract FAQ questions from outline (try multiple formats + PAA fallback)
    faq_questions = []
    # Try Q1: pattern
    for m in re.finditer(r'Q\d+\s*:\s*(.+?)(?:\s*—|\s*$)', outline, re.MULTILINE):
        q = m.group(1).strip()
        if q and len(q) > 10:
            faq_questions.append(q)
    # Try bullet-question pattern
    if not faq_questions:
        faq_questions = [m.group(1).strip() for m in re.finditer(r'[-•]\s*(.+\?)', outline) if len(m.group(1).strip()) > 10]
    # Try ### heading questions
    if not faq_questions:
        faq_questions = [m.group(1).strip() for m in re.finditer(r'###\s*(.+\?)', outline) if len(m.group(1).strip()) > 10]
    # Fallback: load PAA questions directly
    if not faq_questions:
        try:
            paa_ws = sp.worksheet("PAA")
            paa_recs = paa_ws.get_all_records()
            seen = set()
            for pr in paa_recs:
                q = str(pr.get("Question", "")).strip()
                if q and q.lower() not in seen:
                    faq_questions.append(q)
                    seen.add(q.lower())
        except Exception:
            pass
    print(f"  📋 FAQ questions found: {len(faq_questions)}")

    faq_count = len(faq_questions)
    if faq_count > 0:
        print(f"  🤖 Generating FAQ answers ({faq_count} questions)...")
        faq_result = call_gemini(build_faq_answers_prompt(article_data, forum_data), max_tokens=5000)
        time.sleep(4)
    else:
        print(f"  ⚠️ No FAQ questions found in outline")

    # Extract emotion map
    emotion_map = ""
    em_match = re.search(r'## Extracted Emotion Map\s*\n(.*?)$', hooks_result or "", re.DOTALL)
    if em_match:
        emotion_map = em_match.group(1).strip()
    hooks_only = hooks_result[:em_match.start()].strip() if em_match and hooks_result else (hooks_result or "")

    # Write ONE row
    out_ws = get_or_create_tab(sp, OUTPUT_TAB, rows=10, cols=7)
    HEADERS = [
        "Primary_Keyword", "Target_Countries", "Chosen_H1",
        "Empathy_Hooks", "FAQ_Answers", "Emotion_Map",
    ]
    rows_out = [
        HEADERS,
        [
            article_data["primary_keyword"],
            article_data["target_countries"],
            article_data["chosen_h1"],
            _trunc(hooks_only),
            _trunc(faq_result),
            _trunc(emotion_map),
        ],
    ]

    _write_with_retry(out_ws, rows_out)
    _fmt_header(out_ws, len(HEADERS))

    
    hook_count = hooks_only.count("### ") if hooks_only else 0
    faq_a_count = faq_result.count("**Q") if faq_result else 0
    print(f"\n  ✅ Hooks: {hook_count} sections | FAQ answers: {faq_a_count}")
    print(f"\n{'='*65}")
    print(f"✅ EMPATHY + FAQ COMPLETE — single article")
    print(f"   Output tab: '{OUTPUT_TAB}' in '{SHEET_NAME}'")
    print(f"{'='*65}")
