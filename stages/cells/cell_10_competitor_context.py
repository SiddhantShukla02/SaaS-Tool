# ─────────────────────────────────────────────────────────────
# COMPETITOR CONTEXT PIPELINE
# ─────────────────────────────────────────────────────────────
# PURPOSE:
#   Extracts structured competitor intelligence from selected URLs.
#
# INPUT:
#   - selected_urls (Postgres via get_selected_urls)
#   - run_keywords (for primary keyword detection)
#
# PROCESS:
#   - Scrapes page content (Firecrawl → Trafilatura → BS4 fallback)
#   - Cleans text and extracts headers (H1/H2/H3)
#   - Extracts FAQs + structured "others" data using Gemini
#   - Extracts meta title using multiple fallback strategies
#   - Extracts structured keyword clusters (7 categories)
#
# OUTPUT:
#   - competitor_pages table (content + metadata)
#   - competitor_keywords table (clustered keywords)
#   - raw + clean text stored in R2
#
# NOTES:
#   - Uses multi-layer scraping fallback for robustness
#   - Gemini used for structured extraction (FAQs + keywords)
#   - High-complexity cell — avoid casual logic changes
# ─────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────

import json
import os
import re
import time

import requests as req_lib
from bs4 import BeautifulSoup
from google import genai
from google.genai import types

from app.repositories.run_repo import get_run_keywords
from app.repositories.search_repo import (
    get_selected_urls,
    insert_competitor_page,
    insert_competitor_keywords,
)
from app.storage import (
    r2_put_text,
    scrape_raw_key,
    scrape_clean_key,
)

from config import (
    FIRECRAWL_API_KEY,
    GEMINI_API_KEY,
    GEMINI_MODEL,
    MAX_CELL,
    MAX_SCRAPE_CHARS,
    SAFETY_OFF,
    detect_specialty,
)


# ── Extraction limits ─────────────────────────────────────────

MAX_TEXT_CHARS = 30_000

gemini_client = genai.Client(api_key=GEMINI_API_KEY)

# ─────────────────────────────────────────────
# SHARED FORMAT HELPERS
# ─────────────────────────────────────────────

def _trunc(v):
    s = str(v) if v is not None else ""
    return s[:MAX_CELL] + "\n[TRUNCATED]" if len(s) > MAX_CELL else s


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
            lines.append(f"A{i}: {faq.get('answer', '')}")
            lines.append("")
        else:
            lines.append(f"{i}. {faq}")

    return "\n".join(lines).strip()


def fmt_others_cell(others):
    if not others:
        return ""
    return "\n".join(f"• {o}" for o in others)


# ─────────────────────────────────────────────
# SCRAPER WATERFALL
# ─────────────────────────────────────────────

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


# ─────────────────────────────────────────────
# TEXT CLEANING + HEADER EXTRACTION
# ─────────────────────────────────────────────

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



# ─────────────────────────────────────────────
# GEMINI JSON RECOVERY HELPERS
# ─────────────────────────────────────────────


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


# ─────────────────────────────────────────────
# FAQ + OTHERS EXTRACTION
# ─────────────────────────────────────────────

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



# ─────────────────────────────────────────────
# META TITLE EXTRACTION
# ─────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Googlebot/2.1 (+http://www.google.com/bot.html)",
]

HTML_ENTITY_MAP = {
    "&amp;": "&", "&#8211;": "–", "&#8212;": "—",
    "&nbsp;": " ", "&#039;": "'", "&quot;": '"',
    "&lt;": "<", "&gt;": ">",
}

def clean_entities(text: str) -> str:
    for entity, char in HTML_ENTITY_MAP.items():
        text = text.replace(entity, char)
    return text.strip()

def extract_meta_title(url: str, timeout: int = 15) -> tuple[str, str]:
    """
    Returns (title, source). Source is one of:
      'title_tag', 'og:title', 'twitter:title', 'bs4_fallback', 'failed'
    Never raises — always returns a string (empty = nothing found).
    """
    last_error = ""

    for ua_idx, ua in enumerate(USER_AGENTS, start=1):
        try:
            resp = req_lib.get(
                url,
                headers={
                    "User-Agent": ua,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                },
                timeout=timeout,
                allow_redirects=True,
            )

            status       = resp.status_code
            content_type = resp.headers.get("Content-Type", "")

            if status == 429:
                print(f"       ⏳ 429 Rate-limited — waiting 10s...")
                time.sleep(10)
                continue

            if "text/html" not in content_type and "text/plain" not in content_type:
                print(f"       ⚠️ Non-HTML response ({content_type[:50]}) — skipping")
                return "", "failed"

            # Decode with correct encoding
            resp.encoding = resp.apparent_encoding or "utf-8"
            html = resp.text

            print(f"       ℹ️ Status {status} | {len(html):,} chars | UA #{ua_idx}")

            # ── Strategy 1: <title> tag — search FULL page, not just first 10K ──
            title_match = re.search(
                r'<title[^>]*>(.*?)</title>',
                html,
                re.IGNORECASE | re.DOTALL
            )
            if title_match:
                t = clean_entities(title_match.group(1))
                if len(t) > 5:
                    return t[:200], "title_tag"

            # ── Strategy 2: og:title — almost always static even on SPAs ──
            og_match = re.search(
                r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
                html, re.IGNORECASE
            ) or re.search(
                r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:title["\']',
                html, re.IGNORECASE
            )
            if og_match:
                t = clean_entities(og_match.group(1))
                if len(t) > 5:
                    return t[:200], "og:title"

            # ── Strategy 3: twitter:title ──
            tw_match = re.search(
                r'<meta[^>]+name=["\']twitter:title["\'][^>]+content=["\']([^"\']+)["\']',
                html, re.IGNORECASE
            ) or re.search(
                r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']twitter:title["\']',
                html, re.IGNORECASE
            )
            if tw_match:
                t = clean_entities(tw_match.group(1))
                if len(t) > 5:
                    return t[:200], "twitter:title"

            # ── Strategy 4: BeautifulSoup full parse ──
            soup = BeautifulSoup(html, "html.parser")
            tag  = soup.find("title")
            if tag and tag.get_text(strip=True):
                t = clean_entities(tag.get_text(strip=True))
                if len(t) > 5:
                    return t[:200], "bs4_fallback"

            print(f"       ↩️ No title found with UA #{ua_idx}, trying next...")

        except req_lib.exceptions.Timeout:
            last_error = "Timeout"
            print(f"       ⏰ Timeout with UA #{ua_idx}")
        except req_lib.exceptions.TooManyRedirects:
            return "", "failed"
        except Exception as e:
            last_error = str(e)
            print(f"       ❌ Error with UA #{ua_idx}: {e}")

    print(f"       ✖ All strategies failed. Last error: {last_error}")
    return "", "failed"


# ─────────────────────────────────────────────
# KEYWORD EXTRACTION — SPECIALTY EXAMPLES
# ─────────────────────────────────────────────

SPECIALTY_EXAMPLES = {
    "fertility": {
        "procedure":     "IVF, ICSI, IUI, donor egg IVF, frozen embryo transfer, PGT-A, blastocyst transfer",
        "concerns":      "failed IVF cycle, low AMH, PCOS and IVF, low sperm count, repeated implantation failure",
        "safety":        "JCI accredited IVF centre India, ICMR guidelines IVF, IVF success rate India, ART registration",
        "recovery":      "post embryo transfer care, IVF bed rest, OHSS symptoms, two-week wait, beta hCG timing",
        "logistics":     "IVF India for UK patients, IVF package India, surrogacy legality India, IVF tourism Delhi",
        "cost":          "IVF cost India vs UK, IVF package all inclusive India, donor IVF cost, affordable IVF India",
        "brand":         "Nova IVF Fertility, Cloudnine Fertility, Apollo Fertility, Indira IVF, Manipal Fertility",
    },
    "cardiac": {
        "procedure":     "CABG, OPCAB, off-pump bypass, beating heart surgery, minimally invasive bypass, TAVR, valve replacement",
        "concerns":      "chest pain, shortness of breath, blocked arteries, leaky valve, irregular heartbeat, LAD blockage",
        "safety":        "JCI accredited cardiac centre India, NABH cardiac hospital, mortality rate cardiac surgery India",
        "recovery":      "cardiac rehab India, recovery after bypass, driving after heart surgery, when can I fly after bypass",
        "logistics":     "bypass surgery India for UK patients, cardiac package India, medical visa heart surgery",
        "cost":          "bypass surgery cost India vs UK, CABG package price India, heart valve replacement cost India",
        "brand":         "Fortis Escorts Heart Institute, Medanta Heart Institute, Narayana Hrudayalaya, Asian Heart Institute",
    },
    "orthopedic": {
        "procedure":     "TKR, UKR, PKR, robotic knee replacement, MAKO knee, anterior hip approach, hip resurfacing, ACL reconstruction",
        "concerns":      "knee pain, hip pain, arthritis, meniscus tear, cartilage damage, bone-on-bone knee",
        "safety":        "JCI accredited orthopedic hospital India, implant brands India, robotic TKR India, surgeon experience",
        "recovery":      "physiotherapy after knee replacement, walking timeline after TKR, flying after knee surgery",
        "logistics":     "knee replacement India for UK patients, orthopedic package India, medical visa knee surgery",
        "cost":          "knee replacement cost India vs UK, TKR package price India, bilateral knee cost India",
        "brand":         "Apollo Orthopedics, Max Super Speciality, Fortis Bone and Joint, Manipal Hospitals",
    },
    "cosmetic": {
        "procedure":     "rhinoplasty, liposuction, abdominoplasty, BBL, facelift, blepharoplasty, breast augmentation, otoplasty",
        "concerns":      "dorsal hump, deviated septum, bulbous tip, post-pregnancy body, loose skin, aging face",
        "safety":        "board-certified plastic surgeon India, ISAPS member India, JCI cosmetic hospital, complication rates",
        "recovery":      "recovery timeline rhinoplasty, swelling after liposuction, final results timeline, scar fading",
        "logistics":     "rhinoplasty India for UK patients, cosmetic surgery tourism India, recovery accommodation Delhi",
        "cost":          "rhinoplasty cost India vs UK, liposuction package India, BBL price India, mommy makeover India",
        "brand":         "Olmec, Designer Bodyz, SCULPT, Richardsons Cosmetic, Dr. Anup Dhir clinic",
    },
    "oncology": {
        "procedure":     "chemotherapy, immunotherapy, proton therapy, CAR-T therapy, radiation therapy, robotic oncosurgery, HIPEC",
        "concerns":      "stage 4 cancer, metastatic cancer, recurrence, second opinion, survival rate, palliative care",
        "safety":        "JCI accredited cancer hospital India, ICMR tumor board, molecular tumor board India, NABH oncology",
        "recovery":      "chemotherapy side effects, post-surgery rehab cancer, immunity after chemo, nutrition after cancer",
        "logistics":     "cancer treatment India for UK patients, oncology package India, medical visa cancer treatment",
        "cost":          "cancer treatment cost India vs UK, immunotherapy cost India, proton therapy cost India",
        "brand":         "Tata Memorial Centre, HCG Oncology, Apollo Cancer Centre, Max Institute of Cancer Care, Fortis Cancer",
    },
    "dental": {
        "procedure":     "dental implants, full mouth rehabilitation, all-on-4, all-on-6, zirconia crown, veneers, root canal",
        "concerns":      "missing teeth, broken tooth, gum disease, bad bite, discoloured teeth, tooth pain",
        "safety":        "DCI registered dentist India, implant brands India, sterilisation protocol, dental x-ray safety",
        "recovery":      "osseointegration timeline, after implant care, healing after extraction, crown placement timing",
        "logistics":     "dental treatment India for UK patients, dental tourism India, 2-visit dental plan, accommodation",
        "cost":          "dental implant cost India vs UK, full mouth implants cost India, veneer price India, root canal cost",
        "brand":         "Clove Dental, Apollo White, FMS Dental, Sabka Dentist, Dr. Smile Clinic",
    },
    "ophthalmology": {
        "procedure":     "DMEK, DALK, PKP, phacoemulsification, femto LASIK, SMILE, ICL, retinal detachment surgery, vitrectomy",
        "concerns":      "keratoconus, corneal scarring, cataract, myopia, hypermetropia, macular degeneration, diabetic retinopathy",
        "safety":        "eye bank India, All India Ophthalmological Society, cornea grading, LASIK safety, surgeon fellowship",
        "recovery":      "after cornea transplant care, eye drops schedule, visual recovery timeline, when to drive after LASIK",
        "logistics":     "eye surgery India for UK patients, eye hospital package India, accommodation near eye hospital",
        "cost":          "cornea transplant cost India vs UK, LASIK price India, cataract surgery cost India, SMILE cost India",
        "brand":         "LV Prasad Eye Institute, Sankara Nethralaya, Narayana Nethralaya, Centre for Sight, Dr. Agarwal's",
    },
    "bariatric": {
        "procedure":     "sleeve gastrectomy, gastric bypass, mini gastric bypass, revision bariatric, intragastric balloon, SADI-S",
        "concerns":      "obesity, BMI above 35, diabetes and obesity, weight regain, loose skin after weight loss",
        "safety":        "IFSO member India, bariatric surgeon fellowship, JCI bariatric hospital, complication rate sleeve",
        "recovery":      "liquid diet after sleeve, weight loss timeline bariatric, exercise after surgery, hair loss post-surgery",
        "logistics":     "bariatric surgery India for UK patients, obesity surgery package India, medical visa weight loss",
        "cost":          "bariatric surgery cost India vs UK, sleeve gastrectomy price India, gastric bypass cost India",
        "brand":         "Max Institute of Minimal Access, Sir Ganga Ram, Apollo Bariatric, Fortis Obesity, Dr. Tarun Mittal",
    },
    "transplant": {
        "procedure":     "kidney transplant, liver transplant, bone marrow transplant, stem cell transplant, deceased donor, living donor",
        "concerns":      "end stage renal disease, liver failure, waiting list, donor matching, rejection, GVHD",
        "safety":        "NOTTO registration, THOA compliance India, JCI transplant centre, transplant committee approval",
        "recovery":      "immunosuppressant schedule, GVHD signs, rejection symptoms, infection risk post-transplant, isolation",
        "logistics":     "transplant India for UK patients, transplant package India, medical visa organ transplant, donor travel",
        "cost":          "kidney transplant cost India vs UK, liver transplant package India, BMT cost India",
        "brand":         "Medanta Transplant, Apollo Transplant, Global Hospitals, BLK Max Transplant, Fortis Transplant",
    },
    "general": {
        "procedure":     "(specific procedure names from source content)",
        "concerns":      "(symptoms and conditions patients describe)",
        "safety":        "JCI accreditation, NABH accreditation, surgeon credentials, hospital experience",
        "recovery":      "recovery timeline, post-procedure care, follow-up schedule, return to normal activity",
        "logistics":     "medical tourism India, medical visa India, accommodation, duration of stay",
        "cost":          "cost India vs UK, package price India, affordable treatment India",
        "brand":         "Apollo Hospitals, Fortis Healthcare, Max Healthcare, Medanta, Manipal Hospitals",
    },
}



def build_specialty_examples(specialty: str) -> dict:
    """Return the 7-category example set for the specialty, with 'general' fallback."""
    return SPECIALTY_EXAMPLES.get(specialty, SPECIALTY_EXAMPLES["general"])


# ─────────────────────────────────────────────
# KEYWORD EXTRACTION LOGIC
# ─────────────────────────────────────────────

def call_gemini_json(prompt, max_tokens=8000, retries=3):
    """Call Gemini and return parsed JSON dict, or empty dict on failure."""
    for attempt in range(retries):
        try:
            resp = gemini_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    max_output_tokens=max_tokens,
                    safety_settings=SAFETY_OFF,
                    response_mime_type="application/json",
                ),
            )
            if not resp.candidates:
                continue
            raw = resp.text.strip()
            # Strip markdown fences if Gemini added them despite JSON mode
            raw = re.sub(r"^```\w*\s*", "", raw)
            raw = re.sub(r"```\s*$", "", raw)
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError) as e:
            print(f"    ⚠️  Gemini JSON parse attempt {attempt+1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(4 * (attempt + 1))
        except Exception as e:
            print(f"    ⚠️  Gemini attempt {attempt+1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(6 * (attempt + 1))
    return {}



def extract_keywords(url: str, texts_only: str, primary_kw: str) -> dict:
    """
    Extract keywords from competitor content.

    Returns structured dict:
    {
        "procedure_types": [keywords...],
        "patient_concerns": [keywords...],
        "safety_quality": [keywords...],
        "recovery_results": [keywords...],
        "travel_logistics": [keywords...],
        "cost_value": [keywords...],
        "hospital_surgeon_brand": [keywords...],
        "specialty_detected": "cardiac|orthopedic|...",
    }
    """
    # Derive page topic from URL path
    try:
        path   = url.rstrip("/").split("/")[-1]
        header = re.sub(r"[-_]", " ", path).strip()
        header = re.sub(r"[^a-zA-Z0-9 ]", "", header).title() or "the topic"
    except Exception:
        header = "the topic"

    # Detect medical specialty and load matching examples
    specialty = detect_specialty(primary_kw)
    examples  = build_specialty_examples(specialty)

    prompt = f"""You are a Senior SEO Strategist for Divinheal, an Indian medical tourism platform.
Extract a BALANCED keyword set covering the full patient decision journey — not just cost.

TREATMENT TOPIC: {primary_kw}
PAGE TOPIC    : {header}
DETECTED SPECIALTY: {specialty}
TARGET AUDIENCE: Patients from Middle East, GCC, Africa, South Asia researching treatment in India

HARD CAP ON COST KEYWORDS: At most 20% of your total output may be cost-related.
If the source URL is cost-heavy, you MUST infer non-cost keywords from the treatment
name and medical knowledge.

OUTPUT FORMAT — Return a JSON object with EXACTLY these 7 arrays:

{{
  "procedure_types": [
    // 12-18 keywords: what the procedure IS, its variants, technique names
    // Example keywords for {specialty}: {examples['procedure']}
  ],
  "patient_concerns": [
    // 12-18 keywords: symptoms, conditions, problems patients want fixed
    // Example keywords for {specialty}: {examples['concerns']}
  ],
  "safety_quality": [
    // 12-18 keywords: accreditations, certifications, surgeon credentials
    // Example keywords for {specialty}: {examples['safety']}
  ],
  "recovery_results": [
    // 12-18 keywords: post-procedure timeline, healing, outcomes
    // Example keywords for {specialty}: {examples['recovery']}
  ],
  "travel_logistics": [
    // 12-18 keywords: medical tourism logistics for international patients
    // Example keywords for {specialty}: {examples['logistics']}
  ],
  "cost_value": [
    // EXACTLY 12-15 keywords: cost comparison, value, packages
    // Example keywords for {specialty}: {examples['cost']}
    // DO NOT output synonym-flooded variants (cost/price/fee/charges/expenses all at once).
    // Pick the single most-searched variant per concept and move on.
  ],
  "hospital_surgeon_brand": [
    // 10-15 keywords: named entities found in the source content
    // Example keywords for {specialty}: {examples['brand']}
    // Only include names actually present in the source content below.
  ]
}}

RULES:
- Return ONLY valid JSON. No preamble, no markdown fences.
- Do not output the example keywords verbatim unless they actually appear in source content.
- Every keyword must be a realistic Google search term a patient would type.
- Keywords must be lowercase unless a proper noun (hospital names, doctor names).

SOURCE CONTENT:
{texts_only[:MAX_TEXT_CHARS]}"""

    result = call_gemini_json(prompt, max_tokens=8000)

    # Fallback for parse failure — return empty arrays
    if not result:
        return {
            "procedure_types": [], "patient_concerns": [], "safety_quality": [],
            "recovery_results": [], "travel_logistics": [], "cost_value": [],
            "hospital_surgeon_brand": [], "specialty_detected": specialty,
        }

    result["specialty_detected"] = specialty
    # Ensure all 7 keys exist with list type
    for key in ["procedure_types", "patient_concerns", "safety_quality",
                "recovery_results", "travel_logistics", "cost_value",
                "hospital_surgeon_brand"]:
        if key not in result or not isinstance(result[key], list):
            result[key] = []

    return result


def count_keywords(extracted: dict) -> tuple:
    """Count keywords and enforce the 20% cost cap."""
    non_cost_cats = ["procedure_types", "patient_concerns", "safety_quality",
                     "recovery_results", "travel_logistics", "hospital_surgeon_brand"]
    non_cost_count = sum(len(extracted.get(c, [])) for c in non_cost_cats)
    cost_count     = len(extracted.get("cost_value", []))
    total          = non_cost_count + cost_count
    cost_pct       = (cost_count / total * 100) if total else 0
    return total, cost_count, cost_pct

def format_keyword_summary(extracted: dict) -> str:
    """Human-readable summary of extracted keyword clusters."""
    lines = []
    for cat in ["procedure_types", "patient_concerns", "safety_quality",
                "recovery_results", "travel_logistics", "cost_value",
                "hospital_surgeon_brand"]:
        label = cat.upper().replace("_", " ")
        items = extracted.get(cat, [])
        if items:
            lines.append(f"{label}: {', '.join(items)}")
    return "\n".join(lines)


# ─────────────────────────────────────────────
# RUN INPUT HELPERS
# ─────────────────────────────────────────────

def load_primary_keyword_for_run(run_id: int) -> str:
    keyword_rows = get_run_keywords(run_id)

    kw_list = [
        str(row.get("keyword", "")).strip()
        for row in keyword_rows
        if str(row.get("keyword", "")).strip()
    ]

    if not kw_list:
        raise RuntimeError(f"No keywords found for run_id={run_id}")

    # Same logic as old pipeline → pick longest keyword
    primary = sorted(kw_list, key=lambda k: len(k.split()), reverse=True)[0]

    print(f"  📌 Primary keyword  : {primary}")
    print(f"  🏥 Detected specialty: {detect_specialty(primary)}")

    return primary


# ─────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────

def main(run_id: int):
    print("\n" + "=" * 65)
    print("🚀 COMPETITOR CONTEXT PIPELINE")
    print("=" * 65)

    urls = get_selected_urls(run_id)
    primary_keyword = load_primary_keyword_for_run(run_id)
    specialty = detect_specialty(primary_keyword)

    if not urls:
        raise RuntimeError(f"No selected URLs found for run_id={run_id}")

    print(f"  ✅ Loaded {len(urls)} selected URLs")

    for idx, url in enumerate(urls, 1):
        print(f"\n{'─' * 60}")
        print(f"[{idx}/{len(urls)}] 🌐 {url[:80]}")

        try:
            scraped = scrape_full(url)
            raw = scraped["raw"]
            html = scraped["html"]
            method = scraped["method"]

            if not raw or method == "FAILED":
                print("  ⚠️  Scrape failed — writing error row")

                insert_competitor_page(
                    run_id=run_id,
                    url=url,
                    scrape_method=method,
                    raw_r2_key=None,
                    clean_r2_key=None,
                    h1_data="",
                    h2_data="",
                    h3_data="",
                    faqs_json=[],
                    others_json=[],
                    meta_title="",
                    meta_title_source="failed",
                    status="failed",
                    error_message="SCRAPE_FAILED",
                )

                continue

            clean_text = extract_clean_text(raw, html)
            print(f"  🔤 Clean text: {len(clean_text):,} chars")

            raw_key = scrape_raw_key(run_id, url)
            clean_key = scrape_clean_key(run_id, url)

            r2_put_text(raw_key, raw)
            r2_put_text(clean_key, clean_text)

            headers = extract_headers(raw, html)

            h1_cell = fmt_headers_cell(headers["h1"])
            h2_cell = fmt_headers_cell(headers["h2"])
            h3_cell = fmt_headers_cell(headers["h3"])

            hdr_method = headers.get("_method", "?")
            print(
                f"  🏷️  H1:{len(headers['h1'])} "
                f"H2:{len(headers['h2'])} "
                f"H3:{len(headers['h3'])} [{hdr_method}]"
            )

            print("  🤖 Calling Gemini for FAQs + Others...")
            faqs, others = extract_faqs_and_others(
                clean_text or raw,
                url,
                primary_keyword,
                page_headers=headers,
            )
            print(f"  ❓ FAQs:{len(faqs)}  Others:{len(others)}")

            print("  🏷️  Extracting meta title...")
            meta_title, meta_title_source = extract_meta_title(url)

            print("  🔑 Extracting keywords...")
            extracted_keywords = extract_keywords(url, clean_text, primary_keyword)
            keyword_count, cost_count, cost_pct = count_keywords(extracted_keywords)
            keyword_summary = format_keyword_summary(extracted_keywords)

            print(
                f"  🔢 {keyword_count} keywords extracted | "
                f"{cost_count} cost ({cost_pct:.0f}%)"
            )

            insert_competitor_page(
                run_id=run_id,
                url=url,
                scrape_method=method,
                raw_r2_key=raw_key,
                clean_r2_key=clean_key,
                h1_data=_trunc(h1_cell),
                h2_data=_trunc(h2_cell),
                h3_data=_trunc(h3_cell),
                faqs_json=faqs,
                others_json=others,
                meta_title=meta_title,
                meta_title_source=meta_title_source,
                status="success",
                error_message=None,
            )

            insert_competitor_keywords(
                run_id=run_id,
                url=url,
                primary_keyword=primary_keyword,
                specialty=specialty,
                extracted_json=extracted_keywords,
                summary=_trunc(keyword_summary),
                keyword_count=keyword_count,
            )

            print(f"  ✅ Saved competitor context [{method}]")

        except Exception as e:
            print(f"  ❌ Unexpected error for {url}: {e}")

            insert_competitor_page(
                run_id=run_id,
                url=url,
                scrape_method=None,
                raw_r2_key=None,
                clean_r2_key=None,
                h1_data="",
                h2_data="",
                h3_data="",
                faqs_json=[],
                others_json=[],
                meta_title="",
                meta_title_source="failed",
                status="failed",
                error_message=str(e),
            )

        time.sleep(2)

    print("\n" + "=" * 65)
    print("✅ COMPETITOR CONTEXT PIPELINE COMPLETE")
    print(f"   Total URLs : {len(urls)}")
    print(f"   Topic      : {primary_keyword}")
    print("=" * 65)


if __name__ == "__main__":
    run_id_raw = os.getenv("RUN_ID")

    if not run_id_raw:
        raise RuntimeError("RUN_ID env var is required")

    main(int(run_id_raw))