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

# ─── CELL: H1 / Meta Title / Meta Description Generator ──────────────
import json, time, re
import gspread
from google import genai
from app.utils.helper import get_sheet_client
from google.genai import types

SHEET_NAME   = SPREADSHEET_NAME
KEYWORD_TAB  = "keyword"
URL_DATA_TAB = "Url_data_ext"
PAA_TAB      = "PAA"
OUTPUT_TAB   = "H1_Meta_Output"


GEMINI_MODEL      = "gemini-2.5-flash"
MAX_CELL          = 49000

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

gc = get_sheet_client(SCOPES)
gemini_client = genai.Client(api_key=GEMINI_API_KEY)

SAFETY_OFF = [
    types.SafetySetting(category="HARM_CATEGORY_HARASSMENT",        threshold="OFF"),
    types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH",       threshold="OFF"),
    types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="OFF"),
    types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="OFF"),
    types.SafetySetting(category="HARM_CATEGORY_CIVIC_INTEGRITY",   threshold="OFF"),
]

COUNTRY_MAP = {
    "ng": "Nigeria", "ae": "UAE", "gb": "UK", "us": "USA",
    "et": "Ethiopia", "ke": "Kenya", "za": "South Africa",
    "sa": "Saudi Arabia", "pk": "Pakistan", "bd": "Bangladesh",
    "au": "Australia", "ca": "Canada", "sg": "Singapore",
    "om": "Oman", "qa": "Qatar", "bh": "Bahrain", "kw": "Kuwait",
    "in": "India", "iq": "Iraq", "lk": "Sri Lanka", "np": "Nepal",
    "mm": "Myanmar", "af": "Afghanistan", "uz": "Uzbekistan",
}

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

def call_gemini(prompt, max_tokens=4000):
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
            if resp.candidates and resp.candidates[0].content.parts:
                return resp.candidates[0].content.parts[0].text.strip()
        except Exception as e:
            print(f"  ⚠️ Gemini attempt {attempt+1}/3: {e}")
            if attempt < 2:
                time.sleep(6 * (attempt + 1))
    return ""


# ═══════════════════════════════════════════════════════════════
# ROBUST META RESULT PARSER — replaces fragile regex approach
# ═══════════════════════════════════════════════════════════════
def _normalize_meta_text(text):
    """
    Strip ALL markdown decorations from meta_result in one pass.
    Handles **bold**, ## headers, ``` fences — any Gemini format variation.
    """
    if not text:
        return ""
    # Remove ALL **bold** wrappers (not just named patterns)
    text = re.sub(r'\*\*([^*\n]+)\*\*', r'\1', text)
    # Remove ## / ### header prefixes
    text = re.sub(r'^#{1,4}\s*', '', text, flags=re.MULTILINE)
    # Remove ``` fences
    text = re.sub(r'^```\w*\s*$', '', text, flags=re.MULTILINE)
    # Normalize multiple blank lines
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def parse_meta_result(text):
    """
    Regex-based parser for the second Gemini call output.
    Uses lookahead boundaries — immune to false section triggers inside
    the RECOMMENDED block (e.g. '- Meta Title: ...' and '- Meta Description: ...'
    lines that the old state machine wrongly treated as new section headers).

    Works on any format: plain text, **bold headers**, ## markdown,
    with or without the word OPTIONS or COMBINATION.
    """
    normalized = _normalize_meta_text(text)

    # META TITLE: capture until META DESC or RECOMMEND section starts
    mt = re.search(
        r'(?:^|\n)[ \t]*META\s*TITLE[^\n]*\n(.*?)(?=\n[ \t]*META\s*DESC|\n[ \t]*RECOMMEND|\Z)',
        normalized, re.DOTALL | re.IGNORECASE
    )
    # META DESCRIPTION: capture until RECOMMEND section starts
    md = re.search(
        r'(?:^|\n)[ \t]*META\s*DESC[^\n]*\n(.*?)(?=\n[ \t]*RECOMMEND|\Z)',
        normalized, re.DOTALL | re.IGNORECASE
    )
    # RECOMMENDED: capture everything to end of text
    rec = re.search(
        r'(?:^|\n)[ \t]*RECOMMEND[^\n]*\n(.*?)$',
        normalized, re.DOTALL | re.IGNORECASE
    )

    return {
        "meta_titles":       mt.group(1).strip() if mt else "",
        "meta_descriptions": md.group(1).strip() if md else "",
        "recommended":       rec.group(1).strip() if rec else "",
    }
    

def _is_section_header(line_upper):
    """
    Detect if a line is a section header.
    Returns the section name or None.
    Priority order matters — check DESC before TITLE to avoid false matches.
    """
    # Must look like a header: short line (< 80 chars) ending with : or all caps
    if len(line_upper) > 80:
        return None

    # RECOMMENDED section — only needs RECOMMEND, not COMBINATION
    if "RECOMMEND" in line_upper:
        return "recommended"

    # META DESCRIPTION — check before TITLE to avoid 'Meta Title' matching DESC
    if "DESC" in line_upper and ("META" in line_upper or "OPTION" in line_upper):
        return "meta_descriptions"

    # META TITLE
    if "TITLE" in line_upper and ("META" in line_upper or "OPTION" in line_upper):
        return "meta_titles"

    return None





def extract_intent_from_h1(text):
    """Extract INTENT line from h1_result."""
    if not text:
        return ""
    m = re.search(r'(?:^|\n)\s*INTENT\s*:\s*(.+?)(?:\n|$)', text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return ""


def extract_h1_block(text):
    """Extract the H1 OPTIONS block from h1_result."""
    if not text:
        return ""
    m = re.search(
        r'(?:^|\n)\s*H1\s*OPTIONS\s*:?\s*\n(.*?)(?=\n\s*META|\Z)',
        text, re.DOTALL | re.IGNORECASE
    )
    if m:
        return m.group(1).strip()
    # Fallback: everything after INTENT line
    lines = text.split('\n')
    started = False
    block = []
    for line in lines:
        if re.match(r'^\s*INTENT\s*:', line, re.IGNORECASE):
            started = True
            continue
        if started and line.strip():
            block.append(line)
    return '\n'.join(block).strip()


# ═══════════════════════════════════════════════════════════════
# DATA LOADERS
# ═══════════════════════════════════════════════════════════════

def build_article_brief(sp):
    ws = sp.worksheet(KEYWORD_TAB)
    records = ws.get_all_records()
    if not records:
        return None

    keyword_freq = {}
    country_codes = set()
    for r in records:
        kw = str(r.get("Keyword", "")).strip()
        cc = str(r.get("Country_Code", "")).strip().lower()
        if kw:
            keyword_freq[kw] = keyword_freq.get(kw, 0) + 1
        if cc:
            country_codes.add(cc)

    if not keyword_freq:
        return None

    unique_keywords = list(keyword_freq.keys())
    primary = sorted(
        unique_keywords,
        key=lambda k: (len(k.split()), keyword_freq[k]),
        reverse=True,
    )[0]
    secondary = [k for k in unique_keywords if k != primary]

    target_countries = []
    for cc in sorted(country_codes):
        name = COUNTRY_MAP.get(cc, cc.upper())
        target_countries.append({"code": cc, "name": name})

    brief = {
        "primary_keyword": primary,
        "secondary_keywords": secondary,
        "all_keywords": unique_keywords,
        "target_countries": target_countries,
        "country_names": [c["name"] for c in target_countries],
        "country_codes_str": ", ".join(c["code"] for c in target_countries),
    }
    print(f"  📌 Primary keyword   : {primary}")
    print(f"  📌 Secondary keywords: {secondary}")
    print(f"  🌍 Target countries  : {[c['name'] for c in target_countries]}")
    return brief


def load_all_competitor_h1s(sp):
    try:
        ws = sp.worksheet(URL_DATA_TAB)
        records = ws.get_all_records()
    except Exception:
        return []
    h1_list = []
    for r in records:
        h1_raw = str(r.get("H1_Data", "")).strip()
        url = str(r.get("URL", "")).strip()
        meta_title = str(r.get("Meta_Title", "")).strip()
        if h1_raw and h1_raw != "nan":
            for line in h1_raw.split("\n"):
                line = re.sub(r'^\d+\.\s*', '', line).strip()
                if line and len(line) > 5:
                    h1_list.append({"h1": line, "url": url, "meta_title": meta_title})
    return h1_list


def load_all_paa(sp):
    try:
        ws = sp.worksheet(PAA_TAB)
        records = ws.get_all_records()
    except Exception:
        return []
    questions = []
    seen = set()
    for r in records:
        q = str(r.get("Question", "")).strip()
        if q and q.lower() not in seen:
            questions.append(q)
            seen.add(q.lower())
    return questions


# ═══════════════════════════════════════════════════════════════
# PROMPT BUILDERS
# ═══════════════════════════════════════════════════════════════

def build_h1_meta_prompt(brief, competitor_h1s, paa_questions):
    primary = brief["primary_keyword"]
    secondary = brief["secondary_keywords"]
    countries = brief["country_names"]

    comp_block = ""
    if competitor_h1s:
        comp_block = "COMPETITOR H1s (from top ranking URLs):\n"
        for i, item in enumerate(competitor_h1s[:7], 1):
            comp_block += f'  {i}. "{item["h1"]}"'
            if item.get("meta_title") and item["meta_title"] not in ("nan", "(not found)", ""):
                comp_block += f'  |  Meta: "{item["meta_title"][:60]}"'
            comp_block += f'\n     URL: {item["url"][:60]}\n'

    paa_block = ""
    
    if paa_questions:
        paa_block = "PAA QUESTIONS (reveals what searchers want answered):\n"
        counter=len(paa_questions)
        for i, q in enumerate(paa_questions[:counter], 1):
            paa_block += f"  {i}. {q}\n"

    countries_str = ", ".join(countries)

    return f"""Generate 5 H1 heading options for a medical tourism blog.

KEYWORD: {primary}
SECONDARY: {", ".join(secondary) if secondary else "(none)"}
COUNTRIES: {countries_str}

SERP DATA:
{comp_block}
{paa_block}

STEP 1 — Classify intent: Informational / Commercial Investigation / Transactional / Navigational+Geo

STEP 2 — Generate 5 H1 options (55-65 chars each, HARD LIMIT 65). Each MUST use a DIFFERENT hook AND start with different words:
  1. Cost/saving hook (e.g., "Save Up to 70%...")
  2. Year hook (e.g., "(2026 Guide)...") — current year is 2026, NEVER 2024/2025
  3. Authority hook (e.g., "JCI-Accredited..." or "NABH-Certified...")
  4. Emotional hook (e.g., "Your Path to..." or "Hope for...")
  5. Data hook (e.g., "Starting at ₹..." or "From $300...")

Rules:
- Front-load the primary keyword within first 8 words.
- Do NOT lock to one country — the article serves {countries_str}.
- Tone: warm, authoritative. NOT clickbait, NOT clinical.
- Your H1 must NOT repeat any competitor H1's phrasing.

OUTPUT FORMAT (plain text, no bold or markdown):
INTENT: [classification]

H1 OPTIONS:
1. [H1 text] — [X] chars — Why: [1-line rationale]
2. [H1 text] — [X] chars — Why: [1-line rationale]
3. [H1 text] — [X] chars — Why: [1-line rationale]
4. [H1 text] — [X] chars — Why: [1-line rationale]
5. [H1 text] — [X] chars — Why: [1-line rationale]"""


def build_meta_only_prompt(brief, h1_result, comp_meta_block=""):
    primary = brief["primary_keyword"]
    countries_str = ", ".join(brief["country_names"])
    len_of_h1=len(h1_result)
    return f"""You are a Senior SEO Medical Writer for Divinheal.

The H1 options for this article have already been generated:
{h1_result[:len_of_h1] if h1_result else "Primary keyword: " + primary}

COMPETITOR META TITLES (your titles must outperform these):
{comp_meta_block if comp_meta_block else "(none available)"}

Now generate Meta Title and Meta Description options for this article.

PRIMARY KEYWORD: {primary}
TARGET COUNTRIES: {countries_str}

FOR META TITLE (generate 3 options):
1. Include the primary keyword within the first 30 characters.
2. Total length: under 60 characters (HARD limit).
3. End with "| Divinheal" if character count allows; otherwise drop it.
4. Use a value hook: cost figure, saving percentage, or year (2026).

FOR META DESCRIPTION (generate 3 options):
1. Length: 140-155 characters (HARD limit).
2. First sentence: Mirror the DOMINANT patient concern for this treatment:
   - If keyword contains "cost/price/affordable" → mirror cost anxiety.
   - If keyword contains "best/top/safe/quality" → mirror quality/safety concern.
   - If keyword contains "success rate/results" → mirror outcome anxiety.
   - If keyword contains city/hospital name → mirror trust/selection concern.
3. Second sentence: Promise specific value (cost range, hospital quality, success data).
4. End with CTA: "Get a free quote," "Compare now," "Start your journey."
5. Include the primary keyword once, naturally.
6. NO generic filler like "Read more to find out."

RECOMMENDED COMBINATION (pick the best from all generated options):
- H1: [pick best from the H1 options above]
- Meta Title: [pick best]
- Meta Description: [pick best]
- Rationale: [2-3 sentences why this combination wins]

OUTPUT FORMAT — use PLAIN TEXT headers, no bold or markdown:

META TITLE OPTIONS:
1. [Meta Title] — Char count: [X]
2. [Meta Title] — Char count: [X]
3. [Meta Title] — Char count: [X]

META DESCRIPTION OPTIONS:
1. [Meta Description] — Char count: [X]
2. [Meta Description] — Char count: [X]
3. [Meta Description] — Char count: [X]

RECOMMENDED COMBINATION:
- H1: [best]
- Meta Title: [best]
- Meta Description: [best]
- Rationale: [why]"""


# ═══════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════

print("\n" + "="*65)
print("🏷️  H1 / META TITLE / META DESCRIPTION GENERATOR")
print("   Mode: SINGLE ARTICLE — all keywords merged")
print("="*65)

sp = gc.open(SHEET_NAME)

print(f"\n📋 Building article brief from '{KEYWORD_TAB}' tab...")
brief = build_article_brief(sp)

if not brief:
    print("❌ No keywords found in keyword tab.")
else:
    print(f"\n📊 Loading competitor data...")
    comp_h1s = load_all_competitor_h1s(sp)
    paa_qs   = load_all_paa(sp)
    print(f"  Competitor H1s: {len(comp_h1s)} | PAA questions: {len(paa_qs)}")

    # Build comp_meta_block for Call 2
    raw_meta_titles = [h["meta_title"] for h in comp_h1s
                       if h.get("meta_title") and h["meta_title"] not in ("nan", "(not found)", "")]
    comp_meta_block = ""
    if raw_meta_titles:
        comp_meta_block = "COMPETITOR META TITLES:\n"
        for i, mt in enumerate(raw_meta_titles[:5], 1):
            comp_meta_block += f'  {i}. "{mt[:80]}"\n'

    # Call 1: H1 options
    print(f"\n🤖 Call 1: Generating H1 options...")
    h1_prompt = build_h1_meta_prompt(brief, comp_h1s, paa_qs)
    h1_result = call_gemini(h1_prompt, max_tokens=4000)
    time.sleep(3)

    # Call 2: Meta Title + Description
    print(f"🤖 Call 2: Generating Meta Title + Description...")
    meta_prompt = build_meta_only_prompt(brief, h1_result, comp_meta_block)
    meta_result = call_gemini(meta_prompt, max_tokens=8000)

    if not h1_result and not meta_result:
        print("  ⚠️ Both Gemini calls returned empty")
    else:
        # ── Parse h1_result for intent + H1 block ────────────────────
        intent   = extract_intent_from_h1(h1_result)
        h1_block = extract_h1_block(h1_result)

        # ── Parse meta_result with robust state-machine parser ────────
        # This handles ALL Gemini output variations: plain text, **bold**,
        # ## markdown, with/without OPTIONS word, RECOMMENDED/RECOMMENDED COMBINATION
        meta_parsed      = parse_meta_result(meta_result)
        meta_title_block = meta_parsed["meta_titles"]
        meta_desc_block  = meta_parsed["meta_descriptions"]
        recommended      = meta_parsed["recommended"]

        # ── Debug: show raw meta_result if any section still empty ────
        if not meta_desc_block or not recommended:
            print(f"\n  ⚠️ Debug — full meta_result ({len(meta_result or '')} chars):")
            print(meta_result or "(empty)")
            
        # ── Construct labelled display blocks ────────────────────────
        if h1_block:
            h1_block = "H1 OPTIONS:\n" + h1_block
        if meta_title_block:
            meta_title_block = "META TITLE OPTIONS:\n" + meta_title_block
        if meta_desc_block:
            meta_desc_block = "META DESCRIPTION OPTIONS:\n" + meta_desc_block
        if recommended:
            recommended = "RECOMMENDED COMBINATION:\n" + recommended

        # ── Write to sheet ────────────────────────────────────────────
        out_ws = get_or_create_tab(sp, OUTPUT_TAB, rows=20, cols=10)
        HEADERS = [
            "Primary_Keyword", "Secondary_Keywords", "Target_Countries",
            "Intent", "H1_Options", "Meta_Title_Options",
            "Meta_Description_Options", "Recommended_Combination",
        ]
        rows_out = [
            HEADERS,
            [
                brief["primary_keyword"],
                ", ".join(brief["secondary_keywords"]),
                ", ".join(brief["country_names"]),
                _trunc(intent),
                _trunc(h1_block),
                _trunc(meta_title_block),
                _trunc(meta_desc_block),
                _trunc(recommended),
            ],
        ]

        _write_with_retry(out_ws, rows_out)
        _fmt_header(out_ws, len(HEADERS))

        print(f"\n  ✅ Intent          : {intent[:60] or '⚠️ empty'}")
        print(f"  ✅ H1 options      : {'✅' if h1_block else '⚠️ empty'}")
        print(f"  ✅ Meta titles     : {'✅' if meta_title_block else '⚠️ empty'}")
        print(f"  ✅ Meta desc       : {'✅' if meta_desc_block else '⚠️ empty'}")
        print(f"  ✅ Recommended     : {'✅' if recommended else '⚠️ empty'}")
        print(f"\n{'='*65}")
        print(f"✅ H1/META GENERATOR COMPLETE")
        print(f"   Primary keyword : {brief['primary_keyword']}")
        print(f"   Target countries: {', '.join(brief['country_names'])}")
        print(f"   Output tab      : '{OUTPUT_TAB}' in '{SHEET_NAME}'")
        print(f"{'='*65}")

