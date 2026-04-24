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













# ─── CELL: H2/H3 Strategic Outline + Content Gap Analysis ────────────
# SINGLE-ARTICLE PARADIGM:
#   Merges ALL PAA, Autocomplete, Related Searches across all keywords
#   into ONE unified blog outline for the single article.
#
# Reads  : Keyword_n8n > H1_Meta_Output tab      (primary keyword, countries, chosen H1)
#         : Keyword_n8n > keyword tab             (fallback for keyword list)
#         : Keyword_n8n > Url_data_ext tab        (all competitor H2/H3 structures)
#         : Keyword_n8n > PAA tab                 (ALL PAA questions across all keywords)
#         : Keyword_n8n > Other_Autocomplete tab  (ALL autocomplete across all keywords)
#         : Keyword_n8n > Related_search tab      (ALL related searches)
#         : Keyword_n8n > Forum_Master_Insights   (NEW: Content_Gap + Objection + Patient_Question)
#         : Keyword_n8n > Forum_Master_MD         (NEW: patient voice + emotion vocabulary)
# Writes : Keyword_n8n > Blog_Outline tab         (ONE row — the single article outline)
# ─────────────────────────────────────────────────────────────────────

import json, time, re
import gspread
from google import genai
from google.genai import types
from app.utils.helper import get_sheet_client

# ── Config ────────────────────────────────────────────────────────────
SHEET_NAME       = SPREADSHEET_NAME
H1_META_TAB      = "H1_Meta_Output"
KEYWORD_TAB      = "keyword"
URL_DATA_TAB     = "Url_data_ext"
PAA_TAB          = "PAA"
AUTOCOMPLETE_TAB = "Other_Autocomplete"
RELATED_TAB           = "Related_search"
FORUM_INSIGHTS_TAB    = "Forum_Master_Insights"
FORUM_MD_TAB          = "Forum_Master_MD"
OUTPUT_TAB            = "Blog_Outline"


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
    "om": "Oman", "qa": "Qatar", "bh": "Bahrain", "kw": "Kuwait",
    "in": "India", "iq": "Iraq", "lk": "Sri Lanka", "np": "Nepal",
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


def parse_outline_sections(raw_text):
    """
    Parse blog outline from Gemini output.
    Handles multiple formats Gemini uses:
      "H2: heading"
      "## H2: heading"
      "### H2: heading"
      "**H2: heading**"
      "## heading" (without "H2:" prefix)
    Returns dict with gap_analysis, outline, linking, word_count
    """
    text = clean_gemini_output(raw_text)

    # First, try to split by ## section headers
    # Look for Content Gap Analysis section
    gap = ""
    outline = ""
    linking = ""
    word_count = ""

    # Flexible section boundary detection
    gap_patterns = [
        r'(?:#{1,3}\s*)?Content\s*Gap\s*Analysis\s*\n(.*?)(?=(?:#{1,3}\s*)?(?:Complete\s*Blog\s*Outline|Blog\s*Outline|H1\s*:)|$)',
        r'(?:Must-Have|Gap\s*Opportunities)(.*?)(?=H1\s*:|H2\s*:|#{1,3}\s*H2)',
    ]
    for pat in gap_patterns:
        m = re.search(pat, text, re.DOTALL | re.IGNORECASE)
        if m and len(m.group(1).strip()) > 50:
            gap = m.group(1).strip()
            break

    # Find outline section — everything from first H2 line to linking/word count
    outline_patterns = [
        r'(H1\s*:.*?)(?=(?:#{1,3}\s*)?Internal\s*Linking|(?:#{1,3}\s*)?Recommended\s*Word|$)',
        r'((?:#{1,3}\s*)?H2\s*:.*?)(?=(?:#{1,3}\s*)?Internal\s*Linking|(?:#{1,3}\s*)?Recommended\s*Word|$)',
    ]
    for pat in outline_patterns:
        m = re.search(pat, text, re.DOTALL | re.IGNORECASE)
        if m and len(m.group(1).strip()) > 100:
            outline = m.group(1).strip()
            break

    # If still no outline, take everything after gap analysis
    if not outline and gap:
        gap_end = text.find(gap) + len(gap)
        remainder = text[gap_end:].strip()
        if len(remainder) > 100:
            outline = remainder

    # If STILL no outline, use the full text (minus obvious non-outline)
    if not outline:
        outline = text

    # Linking
    m = re.search(r'(?:#{1,3}\s*)?Internal\s*Linking.*?\n(.*?)(?=(?:#{1,3}\s*)?Recommended\s*Word|$)', text, re.DOTALL | re.IGNORECASE)
    if m:
        linking = m.group(1).strip()

    # Word count
    m = re.search(r'(?:#{1,3}\s*)?Recommended\s*Word\s*Count.*?\n(.*?)$', text, re.DOTALL | re.IGNORECASE)
    if m:
        word_count = m.group(1).strip()

    return {
        "gap_analysis": gap,
        "outline": outline,
        "linking": linking,
        "word_count": word_count,
    }


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

# ── FIX 1 + FIX 2: increased max_tokens default + use resp.text ───────
# FIX 1: max_tokens raised from 6000 → 16000
#   gemini-2.5-flash thinking mode consumes tokens from the same
#   max_output_tokens budget. With 6000, ~5500 tokens go to internal
#   thinking, leaving ~500 for actual output — outline cuts off at H2 #5.
#   16000 gives enough headroom for thinking + a full outline.
#
# FIX 2: resp.text instead of resp.candidates[0].content.parts[0].text
#   gemini-2.5-flash can return thinking as parts[0] (thought=True).
#   Accessing parts[0].text directly returns the thinking block, not the
#   actual outline. resp.text skips thinking parts and returns only the
#   real response, correctly concatenated.
def call_gemini(prompt, max_tokens=16000):   # ← FIX 1: was 6000
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
                return resp.text.strip()     # ← FIX 2: was resp.candidates[0].content.parts[0].text
        except Exception as e:
            print(f"  ⚠️ Gemini attempt {attempt+1}/3: {e}")
            if attempt < 2:
                time.sleep(6 * (attempt + 1))
    return ""


# ═══════════════════════════════════════════════════════════════
# DATA LOADERS — all load EVERYTHING (no per-keyword filtering)
# ═══════════════════════════════════════════════════════════════


# ── FIX #6: extract_best_h1 was called but never defined in this cell ─
# It existed only in Cell 21 (shared_utils). If Cell 21 wasn't run first,
# this cell crashed with NameError. Now defined inline as safety net.
def extract_best_h1(h1_options_text, recommended_text, fallback_keyword):
    """Extract the best H1 from parsed sections, with multiple fallbacks."""
    if recommended_text:
        match = re.search(r'H1\s*:\s*(.+?)(?:\n|$)', recommended_text)
        if match:
            h1 = match.group(1).strip().strip('"').strip("'").strip("*")
            if len(h1) > 10:
                return h1
    if h1_options_text:
        match = re.search(r'^\s*1[\.\)]\s*(.+?)(?:\s*—|\s*$)', h1_options_text, re.MULTILINE)
        if match:
            h1 = match.group(1).strip().strip('"').strip("'").strip("*")
            if len(h1) > 10:
                return h1
    return fallback_keyword

def load_article_brief(sp):
    """Load the single-article brief from H1_Meta_Output (produced by previous cell)."""
    try:
        ws = sp.worksheet(H1_META_TAB)
        records = ws.get_all_records()
        if records:
            r = records[0]
            primary = str(r.get("Primary_Keyword", "")).strip()
            secondary = str(r.get("Secondary_Keywords", "")).strip()
            countries = str(r.get("Target_Countries", "")).strip()

            chosen_h1 = extract_best_h1(
                str(r.get("H1_Options", "")),
                str(r.get("Recommended_Combination", "")),
                primary
            )

            return {
                "primary_keyword": primary,
                "secondary_keywords": [k.strip() for k in secondary.split(",") if k.strip()],
                "all_keywords": [primary] + [k.strip() for k in secondary.split(",") if k.strip()],
                "target_countries": countries,
                "chosen_h1": chosen_h1,
            }
    except Exception as e:
        print(f"  ⚠️ Could not load H1_Meta_Output: {e}")

    print(f"  ⚠️ Falling back to keyword tab...")
    ws = sp.worksheet(KEYWORD_TAB)
    records = ws.get_all_records()
    kws = list(dict.fromkeys(str(r.get("Keyword", "")).strip() for r in records if str(r.get("Keyword", "")).strip()))
    ccs = list(set(str(r.get("Country_Code", "")).strip().lower() for r in records if str(r.get("Country_Code", "")).strip()))
    countries = ", ".join(COUNTRY_MAP.get(c, c.upper()) for c in sorted(ccs))
    primary = sorted(kws, key=lambda k: len(k.split()), reverse=True)[0] if kws else ""
    return {
        "primary_keyword": primary,
        "secondary_keywords": [k for k in kws if k != primary],
        "all_keywords": kws,
        "target_countries": countries,
        "chosen_h1": primary,
    }


def load_all_competitor_h2s(sp):
    """Load ALL competitor H2/H3 structures."""
    try:
        ws = sp.worksheet(URL_DATA_TAB)
        records = ws.get_all_records()
    except Exception:
        return []
    competitors = []
    for r in records:
        url = str(r.get("URL", "")).strip()
        h2_raw = str(r.get("H2_Data", "")).strip()
        h3_raw = str(r.get("H3_Data", "")).strip()
        if h2_raw and h2_raw != "nan":
            h2s = [re.sub(r'^\d+\.\s*', '', l).strip() for l in h2_raw.split("\n") if l.strip() and len(l.strip()) > 3]
            h3s = [re.sub(r'^\d+\.\s*', '', l).strip() for l in h3_raw.split("\n") if l.strip() and len(l.strip()) > 3]
            competitors.append({"url": url, "h2s": h2s, "h3s": h3s})
    return competitors


def load_competitor_faqs(sp):
    """Load FAQ questions from competitor pages (Url_data_ext → FAQs column)."""
    try:
        ws = sp.worksheet(URL_DATA_TAB)
        records = ws.get_all_records()
    except Exception:
        return []
    seen = set()
    questions = []
    for r in records:
        faq_raw = str(r.get("FAQs", "")).strip()
        if not faq_raw or faq_raw == "nan":
            continue
        for m in re.finditer(r'Q\d+\s*:\s*(.+?)(?:\nA\d+\s*:|$)', faq_raw, re.DOTALL):
            q = m.group(1).strip()
            if q and len(q) > 10 and q.lower() not in seen:
                questions.append(q)
                seen.add(q.lower())
        for m in re.finditer(r'"question"\s*:\s*"([^"]+)"', faq_raw, re.IGNORECASE):
            q = m.group(1).strip()
            if q and len(q) > 10 and q.lower() not in seen:
                questions.append(q)
                seen.add(q.lower())
    return questions


def load_all_deduplicated(sp, tab_name, col_name):
    """Load all values from a tab column, deduplicated."""
    try:
        ws = sp.worksheet(tab_name)
        records = ws.get_all_records()
    except Exception:
        return []
    seen = set()
    items = []
    for r in records:
        val = ""
        for key in [col_name, col_name.lower(), col_name.replace("_", " ")]:
            val = str(r.get(key, "")).strip()
            if val and val != "nan":
                break
        if val and val != "nan" and val.lower() not in seen:
            items.append(val)
            seen.add(val.lower())
    return items


def load_autocomplete_suggestions(sp, tab_name):
    """Load pipe-separated autocomplete suggestions from Other_Autocomplete tab."""
    try:
        ws = sp.worksheet(tab_name)
        records = ws.get_all_records()
    except Exception:
        return []
    seen = set()
    items = []
    for r in records:
        raw = str(r.get("Suggestions", r.get("suggestions", r.get("Suggestion", "")))).strip()
        if not raw or raw == "nan":
            continue
        for suggestion in raw.split("|"):
            s = suggestion.strip()
            if s and len(s) > 3 and s.lower() not in seen:
                items.append(s)
                seen.add(s.lower())
    return items


def load_forum_insights_for_outline(sp):
    """
    Load Content_Gap, Objection, Patient_Question rows from Forum_Master_Insights.
    These are injected into the outline prompt as high-priority H2 candidates.
    """
    try:
        ws      = sp.worksheet(FORUM_INSIGHTS_TAB)
        records = ws.get_all_records()
    except Exception:
        print(f"  ⚠️  Forum_Master_Insights not found — run Cells A+B first")
        return ""

    outline_cats = {"Content_Gap", "Objection", "Patient_Question"}
    grouped = {"Content_Gap": [], "Objection": [], "Patient_Question": []}
    for r in records:
        cat = str(r.get("Insight_Type", "")).strip()
        if cat in outline_cats:
            score   = int(r.get("Priority_Score", 1) or 1)
            insight = str(r.get("Clean_Insight", r.get("Insight_Text", ""))[:120]).strip()
            country = str(r.get("Detected_Country", "Global")).strip()
            if insight:
                grouped[cat].append((score, insight, country))

    for cat in grouped:
        grouped[cat].sort(key=lambda x: x[0], reverse=True)

    if not any(grouped.values()):
        return ""

    lines = ["\nFORUM MASTER INSIGHTS (for outline — run Cells A+B to generate):"]

    if grouped["Content_Gap"]:
        lines.append("\nCONTENT GAPS (topics NO competitor covers — highest SEO priority):")
        for score, insight, country in grouped["Content_Gap"][:10]:
            lines.append(f"  [P{score}|{country}] {insight}")

    if grouped["Objection"]:
        lines.append("\nPATIENT OBJECTIONS (each needs a dedicated H2 section):")
        for score, insight, country in grouped["Objection"][:8]:
            lines.append(f"  [P{score}|{country}] {insight}")

    if grouped["Patient_Question"]:
        lines.append("\nPATIENT QUESTIONS (validate H2/H3 headings + FAQ candidates):")
        for score, insight, country in grouped["Patient_Question"][:12]:
            lines.append(f"  [P{score}|{country}] {insight}")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# PROMPT BUILDER
# ═══════════════════════════════════════════════════════════════

def build_outline_prompt(brief, competitor_h2s, paa_all, autocomplete_all, related_all, comp_faqs=None, forum_insights=""):
    """Build ONE outline prompt for the single article, using ALL merged data."""

    primary   = brief["primary_keyword"]
    secondary = brief["secondary_keywords"]
    countries = brief["target_countries"]
    chosen_h1 = brief["chosen_h1"]

    COMP_CAP = len(competitor_h2s)
    comp_block = ""
    print(f"  Competitors: {len(competitor_h2s)} (capped at {COMP_CAP})")
    if competitor_h2s:
        comp_block = "COMPETITOR H2 STRUCTURES:\n"
        for i, comp in enumerate(competitor_h2s[:COMP_CAP], 1):
            comp_block += f"\nURL {i}: {comp['url'][:70]}\n"
            for h2 in comp["h2s"][:25]:
                comp_block += f"  - H2: {h2}\n"
            for h3 in comp["h3s"][:25]:
                comp_block += f"    - H3: {h3}\n"
        if len(competitor_h2s) > COMP_CAP:
            comp_block += f"\n(Plus {len(competitor_h2s) - COMP_CAP} more competitor URLs — themes covered above)\n"

    # FIX #11: Cap data to prevent prompt bloat (was sending ALL 200+ items)
    PAA_CAP = 25
    paa_block = ""
    print(f"  PAA questions: {len(paa_all)} (capped at {PAA_CAP})")
    if paa_all:
        paa_block = "PAA QUESTIONS (merged across all keyword variations):\n"
        for i, q in enumerate(paa_all[:PAA_CAP], 1):
            paa_block += f"  {i}. {q}\n"
        if len(paa_all) > PAA_CAP:
            paa_block += f"  (Plus {len(paa_all) - PAA_CAP} more PAA questions on similar themes)\n"

    AC_CAP = 60
    ac_block = ""
    print(f"  Autocomplete: {len(autocomplete_all)} (capped at {AC_CAP})")
    if autocomplete_all:
        ac_block = "GOOGLE AUTOCOMPLETE SUGGESTIONS (merged):\n"
        for i, s in enumerate(autocomplete_all[:AC_CAP], 1):
            ac_block += f"  {i}. {s}\n"
        if len(autocomplete_all) > AC_CAP:
            ac_block += f"  (Plus {len(autocomplete_all) - AC_CAP} more suggestions)\n"

    RS_CAP = 40
    rs_block = ""
    print(f"  Related searches: {len(related_all)} (capped at {RS_CAP})")
    if related_all:
        rs_block = "GOOGLE RELATED SEARCHES (merged):\n"
        for i, s in enumerate(related_all[:RS_CAP], 1):
            rs_block += f"  {i}. {s}\n"
        if len(related_all) > RS_CAP:
            rs_block += f"  (Plus {len(related_all) - RS_CAP} more related searches)\n"

    comp_faq_block = ""
    if comp_faqs:
        comp_faq_block = "COMPETITOR FAQ QUESTIONS (from top-ranking pages — consider for YOUR FAQ section):\n"
        for i, q in enumerate(comp_faqs[:80], 1):
            comp_faq_block += f"  {i}. {q}\n"

    forum_insights_block = forum_insights if forum_insights else "(Run Cells A+B to generate Forum_Master_Insights — content gaps and objections will appear here)"

    return f"""Create a blog outline for Divinheal (Indian medical tourism company).

ARTICLE BRIEF:
H1: {chosen_h1}
Primary keyword: {primary}
Secondary keywords: {", ".join(secondary) if secondary else "(none)"}
Target countries: {countries}

FORUM INTELLIGENCE (high-priority — address all content gaps as H2 sections):
{forum_insights_block}

SERP DATA:
{paa_block}
{ac_block}
{rs_block}
{comp_faq_block}
{comp_block}

OUTLINE RULES (follow in order):
1. Follow Patient Decision Journey: Understanding → Cost → Safety/Success → Hospital Selection → Country Comparison → Logistics → Recovery → Divinheal Value → FAQ → Conclusion.
2. Multi-country: Include India vs {countries} cost comparison H2. Include country-specific logistics H3s. ONE unified outline, not per-country.
3. Every keyword variation ({", ".join(brief["all_keywords"])}) must be covered by at least one H2/H3.
4. Tag each H2 with SOURCE: [PAA] / [AC] / [RS] / [COMP] / [COMP_FAQ] / [GAP].
5. Tag each H2 with CONTENT NEEDS: [TABLE] / [CTA] / [STORY] / [IMAGE] / [SCHEMA] / [MANUAL DATA].
6. Include at least 3 H2s tagged [GAP] — topics NO competitor covers.
7. 10-15 H2s total (including FAQ and Conclusion). Annotate each with ~word count.
8. FAQ: 10-13 questions. Prioritize commercial intent (cost, comparison, process).
   DEDUP: PAA, autocomplete, related searches, and competitor FAQs may contain overlapping questions.
   If two questions ask the same thing in different words, keep the version with stronger commercial intent.
   Example: keep "How much does [treatment] cost in India?" over "What is the cost of [treatment]?"
9. Gap Analysis: BRIEF (under 300 words). Outline is the priority.
10. Do NOT include "Internal Linking" or "Word Count" as blog H2s — those go after the outline as metadata.
11. Heading depth: Use H2 and H3 ONLY. Do NOT create H4s under any circumstance.If a sub-point feels like it needs an H4, either fold it into the H3 body text
    or elevate it to its own H3 if it warrants standalone coverage.
    
WORD TARGETS:
- Major sections (Cost, Safety, Hospital): ~400-500 words
- Minor sections (Recovery, Logistics): ~200-300 words
- FAQ: ~350 words | Conclusion: ~130 words | Total: 3000-4500 words

OUTPUT FORMAT (use plain text, not markdown ## or **):

Content Gap Analysis:
- Must-Have (in 3+ competitors): [list]
- Unique to 1 competitor: [list]
- Gap Opportunities (in none): [list]

Blog Outline:
HEADING DEPTH CONSTRAINT: Maximum 2 levels — H2 and H3 only. No H4s anywhere in the outline.

H1: {chosen_h1}

H2: [heading] — Sources: [tags] — Tags: [tags] — ~[X] words
  H3: [sub-heading]
  H3: [sub-heading]

H2: Frequently Asked Questions — Tags: [SCHEMA] — ~350 words
  Q1: [question] — Source: [PAA/AC/RS/COMP_FAQ]
  Q2: [question] — Source: [PAA/AC/RS/COMP_FAQ]

H2: Conclusion — Tags: [CTA] — ~130 words

Internal Linking Opportunities:
- [3-5 Divinheal pages to link to]

Recommended Word Count: ~[X] words"""


# ═══════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════

print("\n" + "="*65)
print("🗂️  H2/H3 STRATEGIC OUTLINE + CONTENT GAP ANALYSIS")
print("   Mode: SINGLE ARTICLE — all keywords/data merged")
print("="*65)

sp = gc.open(SHEET_NAME)

print(f"\n📋 Loading article brief...")
brief = load_article_brief(sp)

if not brief or not brief["primary_keyword"]:
    print("❌ No article brief found. Run the H1/Meta cell first.")
else:
    print(f"  📌 Primary: {brief['primary_keyword']}")
    print(f"  📌 Secondary: {brief['secondary_keywords']}")
    print(f"  🌍 Countries: {brief['target_countries']}")
    print(f"  🏷️ Chosen H1: {brief['chosen_h1'][:70]}")

    print(f"\n📊 Loading ALL supporting data (merged)...")
    competitor_h2s = load_all_competitor_h2s(sp)
    paa_all        = load_all_deduplicated(sp, PAA_TAB, "Question")
    ac_all         = load_autocomplete_suggestions(sp, AUTOCOMPLETE_TAB)
    rs_all         = load_all_deduplicated(sp, RELATED_TAB, "Related_Query")
    comp_faqs      = load_competitor_faqs(sp)

    print(f"  Competitors: {len(competitor_h2s)} | PAA: {len(paa_all)} | AC: {len(ac_all)} | RS: {len(rs_all)} | Comp FAQs: {len(comp_faqs)}")

    print(f"\n📊 Loading Forum_Master_Insights for outline...")
    forum_insights = load_forum_insights_for_outline(sp)
    print(f"  Forum insights: {'✅ loaded' if forum_insights else '⚠️ not available (run Cells A+B)'}")

    # ── FIX 1 applied here too: explicit max_tokens=16000 ─────────────
    print(f"\n🤖 Generating unified outline...")
    prompt = build_outline_prompt(brief, competitor_h2s, paa_all, ac_all, rs_all, comp_faqs, forum_insights)
    result = call_gemini(prompt, max_tokens=16000)   # ← FIX 1: was 6000

    if not result:
        print("  ⚠️ Empty response from Gemini")
    else:
        parsed       = parse_outline_sections(result)
        gap_analysis = parsed["gap_analysis"]
        outline_text = parsed["outline"]
        linking      = parsed["linking"]
        word_count   = parsed["word_count"]

        full_outline = outline_text
        if linking:
            full_outline += f"\n\n## Internal Linking Opportunities\n{linking}"

        out_ws = get_or_create_tab(sp, OUTPUT_TAB, rows=20, cols=8)
        HEADERS = [
            "Primary_Keyword", "Secondary_Keywords", "Target_Countries",
            "Chosen_H1", "Content_Gap_Analysis", "Complete_Outline",
            "Word_Count_Recommendation",
        ]
        rows_out = [
            HEADERS,
            [
                brief["primary_keyword"],
                ", ".join(brief["secondary_keywords"]),
                brief["target_countries"],
                brief["chosen_h1"],
                _trunc(gap_analysis),
                _trunc(full_outline),
                _trunc(word_count),
            ],
        ]

        _write_with_retry(out_ws, rows_out)
        _fmt_header(out_ws, len(HEADERS))

        h2_count  = len(re.findall(r'^H2:', full_outline, re.MULTILINE))
        faq_count = len(re.findall(r'^\s+Q\d+:', full_outline, re.MULTILINE))
        gap_count = full_outline.count("[GAP]")
        print(f"\n  ✅ Outline: {h2_count} H2s | {faq_count} FAQ questions | {gap_count} gap sections")
        print(f"  ✅ Word count: {word_count[:60] if word_count else '⚠️ empty'}")
        print(f"\n{'='*65}")
        print(f"✅ OUTLINE COMPLETE — single article")
        print(f"   Output tab: '{OUTPUT_TAB}' in '{SHEET_NAME}'")
        print(f"{'='*65}")
