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

# ─── CELL: URL Scraper → Url_data_ext ───────────────────────────────
# Reads  : Keyword_n8n > Final_Url tab > "URL" column
#         : Keyword_n8n > keyword tab > primary keyword (for topic context)
# Writes : Keyword_n8n > Url_data_ext tab
# Columns: URL | Raw_Data_Extracted | Texts_Only | H1 | H2 | H3 | FAQs | Others
# Scraper waterfall: Firecrawl → Trafilatura → BS4
# Gemini recovery: Layer1 (local clean) → Layer2 (Gemini repair) → Layer3 (regex)
#
# 3 BUGS FIXED in this version:
#   BUG 1: extract_headers took HTML path on Firecrawl markdown containing
#          <Base64-Image-Removed> tags → missed all # headings.
#          FIX: Try BOTH HTML + Markdown extraction, return whichever finds more.
#   BUG 2: _prepare_content_for_gemini sent first 5K + last 5K but FAQ sections
#          at 40-70% of the page fell in the omitted middle.
#          FIX: Actively search for FAQ section patterns in full text, include it.
#   BUG 3: "others" extraction returned empty for 6/8 URLs.
#          FIX: Strengthened prompt + pass page H2 headings as structural hint.
# ─────────────────────────────────────────────────────────────────────

import requests as req_lib
import json, time, re
from bs4 import BeautifulSoup
import gspread
from google import genai
from google.genai import types
from app.utils.helper import get_sheet_client

# ── Config ────────────────────────────────────────────────────────────
SHEET_NAME        = SPREADSHEET_NAME
SOURCE_TAB        = "Final_Url"
SOURCE_COL        = "URL"
KEYWORD_TAB       = "keyword"
OUTPUT_TAB        = "Url_data_ext"


GEMINI_MODEL      = "gemini-2.5-flash"
MAX_CELL          = 49000
MAX_SCRAPE_CHARS  = 25000

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

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
                print(f"    ⚠️  Sheet write attempt {attempt+1}/{retries} failed: {e} — retry in {wait}s")
                time.sleep(wait)
            else:
                print(f"    ❌ Sheet write failed after {retries} attempts: {e}")
                return False

def _fmt_header(ws, n_cols):
    col_letter = chr(64 + min(n_cols, 26))
    ws.format(f"A1:{col_letter}1", {
        "textFormat": {
            "bold": True,
            "foregroundColor": {"red": 1, "green": 1, "blue": 1}
        },
        "backgroundColor": {"red": 0.13, "green": 0.37, "blue": 0.60}
    })

def get_or_create_tab(spreadsheet, tab_name, rows=3000, cols=20):
    try:
        ws = spreadsheet.worksheet(tab_name)
        ws.clear()
        print(f"  📋 Tab '{tab_name}' cleared and ready.")
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=rows, cols=cols)
        print(f"  📋 Tab '{tab_name}' created.")
    return ws

def load_urls_from_tab(sheet_name, tab_name, col_header):
    ws   = gc.open(sheet_name).worksheet(tab_name)
    rows = ws.get_all_records()
    urls = []
    for r in rows:
        val = str(r.get(col_header, "")).strip()
        if val and val.lower() != "nan":
            urls.append(val)
    print(f"  ✅ Loaded {len(urls)} URLs from '{tab_name}' > '{col_header}'")
    return urls

def load_primary_keyword(spreadsheet):
    try:
        ws = spreadsheet.worksheet(KEYWORD_TAB)
        records = ws.get_all_records()
        if records:
            kw = str(records[0].get("Keyword", "")).strip()
            if kw:
                print(f"  🔑 Primary keyword for topic context: '{kw}'")
                return kw
    except Exception as e:
        print(f"  ⚠️ Could not load primary keyword: {e}")
    return "medical treatment in India"


# ── Scraper waterfall ─────────────────────────────────────────────────
_scrape_cache = {}

def _scrape_firecrawl(url, max_chars=MAX_SCRAPE_CHARS):
    if not FIRECRAWL_API_KEY:
        return "", None
    try:
        try:
            from firecrawl import Firecrawl as _FC
        except ImportError:
            from firecrawl import FirecrawlApp as _FC
        app = _FC(api_key=FIRECRAWL_API_KEY)
        try:
            resp = app.scrape(url, formats=["markdown"], only_main_content=True, timeout=30000)
        except AttributeError:
            resp = app.scrape_url(url, params={"formats": ["markdown"], "onlyMainContent": True, "timeout": 30000})
        md = ""
        if isinstance(resp, dict):
            md = resp.get("markdown", "") or resp.get("content", "")
        elif hasattr(resp, "markdown"):
            md = resp.markdown or ""
        elif hasattr(resp, "content"):
            md = resp.content or ""
        if md and len(md.strip()) > 200:
            return md[:max_chars], "firecrawl"
        return "", None
    except Exception as e:
        print(f"    ⚠️  Firecrawl: {e}")
        return "", None

def _scrape_trafilatura(url, max_chars=MAX_SCRAPE_CHARS):
    try:
        import trafilatura
        dl = trafilatura.fetch_url(url)
        if dl:
            ex = trafilatura.extract(dl, include_tables=True, include_comments=False, no_fallback=False)
            if ex and len(ex.strip()) > 200:
                return ex[:max_chars], "trafilatura"
    except Exception as e:
        print(f"    ⚠️  Trafilatura: {e}")
    return "", None

def _scrape_bs4(url, max_chars=MAX_SCRAPE_CHARS):
    try:
        r = req_lib.get(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 Chrome/120 Safari/537.36"
        }, timeout=20)
        r.raise_for_status()
        return r.text[:max_chars * 5], "bs4_raw"
    except Exception as e:
        print(f"    ⚠️  BS4 fetch: {e}")
    return "", None

def scrape_full(url):
    if url in _scrape_cache:
        return _scrape_cache[url]
    if not url.startswith("http"):
        url = "https://" + url
    text, method = _scrape_firecrawl(url)
    if text:
        result = {"raw": text, "method": method, "html": ""}
        _scrape_cache[url] = result
        print(f"    📄 [{method}] {len(text):,} chars")
        return result
    text, method = _scrape_trafilatura(url)
    if text:
        result = {"raw": text, "method": method, "html": ""}
        _scrape_cache[url] = result
        print(f"    📄 [{method}] {len(text):,} chars")
        return result
    html, method = _scrape_bs4(url)
    if html:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all(["nav", "footer", "header", "aside", "script", "style"]):
            tag.decompose()
        result = {"raw": html[:MAX_SCRAPE_CHARS], "method": "bs4", "html": html}
        _scrape_cache[url] = result
        print(f"    📄 [bs4] {len(html):,} chars HTML")
        return result
    print(f"    ⚠️  All scrapers failed for {url[:70]}")
    return {"raw": "", "method": "FAILED", "html": ""}


# ── Text cleaner ──────────────────────────────────────────────────────
def extract_clean_text(raw, html=""):
    source = html if html else raw
    if "<" in source and ">" in source:
        soup = BeautifulSoup(source, "html.parser")
        for tag in soup.find_all(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
    else:
        text = re.sub(r"[#*_`~>\-]{2,}", " ", raw)
        text = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


# ══════════════════════════════════════════════════════════════════════
# BUG 1 FIX: extract_headers — try BOTH HTML + Markdown, pick the best
# ══════════════════════════════════════════════════════════════════════
def extract_headers(raw, html=""):
    """
    Extract H1/H2/H3 from content.

    ROOT CAUSE of old bug: Firecrawl markdown often contains stray tags like
    <Base64-Image-Removed> or <br>. The old code checked
    `if "<" in source and ">" in source` and took the BeautifulSoup HTML path,
    which found 0 <h1>/<h2> tags because the actual headings are # markdown.

    FIX: Try BOTH extraction methods, return whichever found MORE headers.
    Also handles: bold-line headings (**text**), heading-level promotion when
    a page uses only ### with no # or ##.
    """
    # ── Method 1: HTML tag extraction ─────────────────────────────
    html_h1, html_h2, html_h3 = [], [], []
    source = html if html else raw
    if "<" in source and ">" in source:
        try:
            soup = BeautifulSoup(source, "html.parser")
            html_h1 = [t.get_text(strip=True) for t in soup.find_all("h1") if t.get_text(strip=True)]
            html_h2 = [t.get_text(strip=True) for t in soup.find_all("h2") if t.get_text(strip=True)]
            html_h3 = [t.get_text(strip=True) for t in soup.find_all("h3") if t.get_text(strip=True)]
        except Exception:
            pass

    # ── Method 2: Markdown regex extraction (ALWAYS try on raw) ───
    md_h1, md_h2, md_h3 = [], [], []
    for line in raw.splitlines():
        stripped = line.strip()
        m = re.match(r'^(#{1,6})\s+(.+)', stripped)
        if m:
            level = len(m.group(1))
            text = m.group(2).strip()
            # Strip bold/italic wrapping: **heading** → heading
            text = re.sub(r'^\*{1,2}(.+?)\*{1,2}$', r'\1', text).strip()
            text = re.sub(r'^_+(.+?)_+$', r'\1', text).strip()
            if not text or len(text) < 3:
                continue
            if level == 1:   md_h1.append(text)
            elif level == 2: md_h2.append(text)
            elif level == 3: md_h3.append(text)

    # ── Method 3: Bold-line fallback (**text** as heading) ────────
    bold_h2 = []
    for line in raw.splitlines():
        stripped = line.strip()
        m = re.match(r'^\*\*([^*]{10,120})\*\*$', stripped)
        if m:
            bold_h2.append(m.group(1).strip())

    # ── Pick the method that found MORE headers ───────────────────
    html_total = len(html_h1) + len(html_h2) + len(html_h3)
    md_total   = len(md_h1) + len(md_h2) + len(md_h3)

    if md_total >= html_total:
        h1_list, h2_list, h3_list = md_h1, md_h2, md_h3
        method = "markdown"
    else:
        h1_list, h2_list, h3_list = html_h1, html_h2, html_h3
        method = "html"

    # ── Heading-level promotion (page uses ### as top level) ──────
    # If no H1 or H2 found but H3 exists, promote:
    #   first H3 → H1, remaining H3s → H2
    if not h1_list and not h2_list and h3_list:
        h1_list = [h3_list[0]]
        h2_list = h3_list[1:]
        h3_list = []
        method += "+promoted"

    # If no H2 but bold-line headings exist, use them as H2
    if not h2_list and bold_h2:
        h2_list = bold_h2[:15]
        method += "+bold"

    # ── Filter out nav/footer headings ────────────────────────────
    NAV_WORDS = {"contact", "address", "follow us", "our services", "our blogs",
                 "leave a reply", "cancel reply", "related post", "recent post",
                 "categories", "archives", "sidebar", "footer", "copyright",
                 "welcome to", "menu", "navigation", "subscribe", "useful links",
                 "all right reserved", "privacy policy", "terms"}

    def is_content_heading(text):
        return not any(nav in text.lower() for nav in NAV_WORDS)

    h1_list = [h for h in h1_list if is_content_heading(h)]
    h2_list = [h for h in h2_list if is_content_heading(h)]
    h3_list = [h for h in h3_list if is_content_heading(h)]

    return {"h1": h1_list, "h2": h2_list, "h3": h3_list, "_method": method}


# ── Gemini JSON helpers (3-layer recovery) ────────────────────────────

def _clean_json_string(raw):
    if not raw:
        return ""
    clean = re.sub(r"^```json\s*", "", raw.strip())
    clean = re.sub(r"```\s*$",     "", clean.strip())
    brace_match = re.search(r"\{[\s\S]*\}", clean)
    if brace_match:
        clean = brace_match.group(0)
    clean = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", clean)
    clean = re.sub(r",\s*(\})", r"\1", clean)
    clean = re.sub(r",\s*(\])", r"\1", clean)
    return clean.strip()

def _repair_with_gemini(broken_json):
    repair_prompt = (
        "The following JSON is malformed. Fix ALL syntax errors "
        "(unterminated strings, missing commas, trailing commas, bad escapes) "
        "and return ONLY the corrected JSON — no explanation, no markdown fences.\n\n"
        f"BROKEN JSON:\n{broken_json[:3000]}"
    )
    try:
        resp = gemini_client.models.generate_content(
            model=GEMINI_MODEL, contents=repair_prompt,
            config=types.GenerateContentConfig(max_output_tokens=4000, safety_settings=SAFETY_OFF)
        )
        if not resp.candidates:
            return None
        parts = resp.candidates[0].content.parts
        if not parts:
            return None
        repaired = _clean_json_string(parts[0].text.strip())
        return json.loads(repaired)
    except Exception as e:
        print(f"    ⚠️  Gemini repair failed: {e}")
        return None

def _regex_fallback_extract(raw_text):
    faqs, others = [], []
    faq_pairs = re.findall(
        r'"question"\s*:\s*"([^"]+)"\s*,\s*"answer"\s*:\s*"([^"]+)"',
        raw_text, re.IGNORECASE
    )
    for q, a in faq_pairs:
        faqs.append({"question": q.strip(), "answer": a.strip()})
    others_block = re.search(r'"others"\s*:\s*\[([\s\S]*?)\]', raw_text)
    if others_block:
        items = re.findall(r'"([^"]{5,})"', others_block.group(1))
        others = [i.strip() for i in items]
    return faqs, others


# ══════════════════════════════════════════════════════════════════════
# BUG 2 FIX: Smart content selection — first 4K + FAQ section + last 3K
# ══════════════════════════════════════════════════════════════════════
def _prepare_content_for_gemini(text, max_total=10000):
    """
    ROOT CAUSE of old bug: Sent first 5K + last 5K. FAQ sections at 40-70%
    of the page (e.g., char 10K-16K on a 23K page) fell in the omitted middle.

    FIX: Actively SEARCH for FAQ section in the full text.
    Strategy: first 4K (intro+costs) + FAQ section if found + last 3K (conclusion).
    """
    if len(text) <= max_total:
        return text

    top = text[:4000]
    bottom = text[-3000:]

    # Actively find FAQ section anywhere in the full text
    faq_section = ""
    faq_patterns = [
        r'(?:#{1,3}\s*)?(?:FAQ|Frequently\s+Asked\s+Questions)[^\n]*\n([\s\S]{100,3000}?)(?=\n#{1,3}\s[A-Z]|\Z)',
        r'\*\*(?:FAQ|Frequently Asked)[^\n]*\*\*\s*\n([\s\S]{100,3000}?)(?=\n\*\*[A-Z]|\Z)',
    ]
    for pattern in faq_patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            faq_start = max(0, m.start() - 100)
            faq_end = min(len(text), m.end() + 200)
            faq_section = text[faq_start:faq_end]
            break

    # Strategy 2: Find ### question? headings (even 1 — it's a Q&A heading in the middle)
    if not faq_section:
        qa_blocks = list(re.finditer(r'###\s+[^#\n]*\?\s*\n', text))
        if qa_blocks:
            first = qa_blocks[0].start()
            last = qa_blocks[-1].end()
            faq_section = text[max(0, first - 100):min(len(text), last + 500)][:3000]

    # Strategy 3: Find cost/price headings in the middle (e.g., ### How much does X cost?)
    if not faq_section:
        cost_heading = re.search(
            r'#{1,3}\s+[^\n]*(cost|price|how much|affordable|expense)[^\n]*\n([\s\S]{200,3000}?)(?=\n#{1,3}\s|\Z)',
            text, re.IGNORECASE
        )
        if cost_heading:
            start = max(0, cost_heading.start() - 50)
            end = min(len(text), cost_heading.end() + 200)
            candidate = text[start:end]
            if candidate[:60] not in top and candidate[:60] not in bottom:
                faq_section = candidate[:3000]

    # Assemble: top + FAQ (if found and not in top/bottom) + bottom
    if faq_section:
        faq_in_top = faq_section[:60] in top
        faq_in_bottom = faq_section[:60] in bottom
        if not faq_in_top and not faq_in_bottom:
            combined = (
                top
                + "\n\n[...content omitted...]\n\n"
                + faq_section
                + "\n\n[...content omitted...]\n\n"
                + bottom
            )
            return combined[:max_total]

    return top + "\n\n[...middle content omitted...]\n\n" + bottom


# ══════════════════════════════════════════════════════════════════════
# BUG 3 FIX: FAQ/Others extraction — page headers as hint + stronger Others
# ══════════════════════════════════════════════════════════════════════
def extract_faqs_and_others(raw_text, url="", topic_keyword="medical treatment in India", page_headers=None):
    """
    ROOT CAUSE of old bug (Others empty): The prompt's "others" examples
    were generic placeholders. Gemini returned empty arrays.

    FIX: Pass page H2 headings as structural hint so Gemini knows what
    the page covers. Strengthened Others instruction with explicit
    "do NOT return empty" rule.
    """
    content_for_gemini = _prepare_content_for_gemini(raw_text, max_total=10000)

    # Build header hint to help Gemini understand page structure
    header_hint = ""
    if page_headers:
        h2s = page_headers.get("h2", [])[:10]
        if h2s:
            header_hint = "\nPAGE STRUCTURE (H2 headings on this page):\n"
            header_hint += "\n".join(f"  - {h}" for h in h2s) + "\n"

    prompt = f"""Extract FAQs and key medical data from this web page about {topic_keyword}.
Return ONLY a valid JSON object.
{header_hint}
EXTRACTION RULES:
- Extract ONLY FAQs about {topic_keyword}: treatment costs, procedures, hospitals, outcomes, logistics.
- SKIP FAQs about: website policies, cookies, privacy, shipping, business hours, unrelated services.
- Extract up to 20 most relevant FAQs. Prioritize cost, comparison, and treatment FAQs.
- If two FAQs ask the same thing differently, keep the version with stronger commercial intent.
- Preserve EXACT cost figures as written — do NOT round, convert, or paraphrase numbers.
- If the page shows CONFLICTING cost ranges for the same procedure, extract ALL of them
  and note the section/context where each appears (e.g., "₹50K-1.5L in text vs ₹40K starting in sidebar").
- FAQ answers: up to 150 words each. Preserve cost figures and hospital names.

FOR "others" — extract EVERY data point found on the page:
- ALL cost figures with exact ₹/$ amounts (e.g., "₹40,000 – ₹3,00,000 per session")
- ALL hospital/clinic names with their city
- ALL doctor names with specialization
- ALL accreditations (NABH, JCI, NABL, etc.)
- ALL success/survival rates or outcomes data
- ALL treatment duration or timeline mentions
- ALL country comparison data (India vs UK/UAE/USA)
- Do NOT return an empty "others" array if the page mentions ANY cost or hospital name.
  If you found even 1 cost figure or 1 hospital name, include it.

JSON RULES:
- Return ONLY the JSON — no text before or after, no markdown fences
- Every string must be properly closed with a double-quote
- No trailing commas. Escape quotes inside values with \\"

Required format:
{{
  "faqs": [
    {{"question": "Q text", "answer": "A text with exact figures preserved"}}
  ],
  "others": [
    "Cost: ₹X – ₹Y per session/cycle",
    "Hospital Name, City",
    "Dr. Name — Specialization",
    "Accreditation: NABH/JCI",
    "Success rate: X%",
    "Duration: X weeks/months"
  ]
}}

Page URL: {url}
Page content:
{content_for_gemini}"""

    raw_response = ""
    for attempt in range(3):
        try:
            resp = gemini_client.models.generate_content(
                model=GEMINI_MODEL, contents=prompt,
                config=types.GenerateContentConfig(max_output_tokens=4000, safety_settings=SAFETY_OFF)
            )
            if resp.candidates and resp.candidates[0].content.parts:
                raw_response = resp.candidates[0].content.parts[0].text.strip()
                break
        except Exception as e:
            print(f"    ⚠️  Gemini attempt {attempt+1}/3: {e}")
            if attempt < 2:
                time.sleep(6 * (attempt + 1))

    if not raw_response:
        print(f"    ❌ Gemini returned no response")
        return [], []

    # Layer 1: local clean + parse
    cleaned = _clean_json_string(raw_response)
    try:
        result = json.loads(cleaned)
        return result.get("faqs", []), result.get("others", [])
    except json.JSONDecodeError as e:
        print(f"    ⚠️  Layer 1 JSON parse failed ({e}) — trying Gemini repair...")

    # Layer 2: Gemini repair
    repaired = _repair_with_gemini(cleaned or raw_response)
    if repaired is not None:
        print(f"    ✅  Layer 2 repair succeeded")
        return repaired.get("faqs", []), repaired.get("others", [])

    print(f"    ⚠️  Layer 2 failed — regex fallback...")

    # Layer 3: regex mining
    faqs, others = _regex_fallback_extract(raw_response)
    if faqs or others:
        print(f"    ✅  Layer 3 regex — FAQs:{len(faqs)}  Others:{len(others)}")
    else:
        print(f"    ❌  All 3 layers failed")
    return faqs, others


# ── Format helpers ────────────────────────────────────────────────────
def fmt_headers_cell(header_list):
    if not header_list:
        return ""
    return "\n".join(f"{i+1}. {h}" for i, h in enumerate(header_list))

def fmt_faqs_cell(faqs):
    if not faqs:
        return ""
    lines = []
    for i, faq in enumerate(faqs, 1):
        if isinstance(faq, dict):
            lines.append(f"Q{i}: {faq.get('question', '')}")
            lines.append(f"A{i}: {faq.get('answer',   '')}")
            lines.append("")
        else:
            lines.append(f"{i}. {faq}")
    return "\n".join(lines).strip()

def fmt_others_cell(others):
    if not others:
        return ""
    return "\n".join(f"• {o}" for o in others)


# ── MAIN PIPELINE ─────────────────────────────────────────────────────
print("\n" + "="*65)
print("🚀 URL SCRAPER PIPELINE")
print("="*65)

print(f"\n📋 Loading URLs from '{SHEET_NAME}' > '{SOURCE_TAB}' > '{SOURCE_COL}'...")
sp   = gc.open(SHEET_NAME)
urls = load_urls_from_tab(SHEET_NAME, SOURCE_TAB, SOURCE_COL)
primary_keyword = load_primary_keyword(sp)

if not urls:
    print("❌ No URLs found. Check sheet name, tab name, and column header.")
else:
    out_ws = get_or_create_tab(sp, OUTPUT_TAB, rows=max(len(urls) + 50, 500), cols=10)

    HEADERS = [
        "URL", "Raw_Data_Extracted", "Texts_Only",
        "H1_Data", "H2_Data", "H3_Data", "FAQs", "Others",
    ]
    all_rows = [HEADERS]

    for idx, url in enumerate(urls, 1):
        print(f"\n{'─'*60}")
        print(f"[{idx}/{len(urls)}] 🌐 {url[:80]}")

        try:
            scraped = scrape_full(url)
            raw     = scraped["raw"]
            html    = scraped["html"]
            method  = scraped["method"]

            if not raw or method == "FAILED":
                print(f"  ⚠️  Scrape failed — writing error row")
                all_rows.append([url, "SCRAPE_FAILED", "", "", "", "", "", ""])
                continue

            clean_text = extract_clean_text(raw, html)
            print(f"  🔤 Clean text: {len(clean_text):,} chars")

            # ── Headers (FIXED: tries BOTH HTML + Markdown) ─────────
            headers = extract_headers(raw, html)
            h1_cell = fmt_headers_cell(headers["h1"])
            h2_cell = fmt_headers_cell(headers["h2"])
            h3_cell = fmt_headers_cell(headers["h3"])
            hdr_method = headers.get("_method", "?")
            print(f"  🏷️  H1:{len(headers['h1'])}  H2:{len(headers['h2'])}  H3:{len(headers['h3'])}  [{hdr_method}]")

            # ── FAQs + Others (FIXED: FAQ search + header hints) ────
            print(f"  🤖 Calling Gemini for FAQs + Others...")
            faqs, others = extract_faqs_and_others(
                clean_text or raw, url, primary_keyword, page_headers=headers
            )
            faq_cell    = fmt_faqs_cell(faqs)
            others_cell = fmt_others_cell(others)
            print(f"  ❓ FAQs:{len(faqs)}  Others:{len(others)}")

            row = [
                url, _trunc(raw), _trunc(clean_text),
                _trunc(h1_cell), _trunc(h2_cell), _trunc(h3_cell),
                _trunc(faq_cell), _trunc(others_cell),
            ]
            all_rows.append(row)
            print(f"  ✅ Row ready [{method}]")

        except Exception as e:
            print(f"  ❌ Unexpected error for {url}: {e}")
            all_rows.append([url, f"ERROR: {e}", "", "", "", "", "", ""])

        if idx % 5 == 0:
            print(f"\n  💾 Incremental save at URL #{idx}...")
            _write_with_retry(out_ws, [[_trunc(c) for c in r] for r in all_rows])
            _fmt_header(out_ws, len(HEADERS))

        time.sleep(2)

    print(f"\n{'='*65}")
    print(f"💾 Writing final data to '{OUTPUT_TAB}'...")
    _write_with_retry(out_ws, [[_trunc(c) for c in r] for r in all_rows])
    _fmt_header(out_ws, len(HEADERS))

    ok_rows     = [r for r in all_rows[1:] if r[1] not in ("SCRAPE_FAILED", "") and not str(r[1]).startswith("ERROR")]
    failed_rows = [r for r in all_rows[1:] if r[1] == "SCRAPE_FAILED" or str(r[1]).startswith("ERROR")]

    print(f"\n✅ PIPELINE COMPLETE")
    print(f"   Total URLs  : {len(urls)}")
    print(f"   Scraped OK  : {len(ok_rows)}")
    print(f"   Failed      : {len(failed_rows)}")
    print(f"   Topic       : {primary_keyword}")
    print(f"   Output tab  : '{OUTPUT_TAB}' in '{SHEET_NAME}'")
    if failed_rows:
        print(f"\n   ⚠️  Failed URLs:")
        for r in failed_rows:
            print(f"      • {r[0][:70]}")
