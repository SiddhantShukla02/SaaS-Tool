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

# ─── CELL: Meta Title Extractor → Url_data_ext Enhancement (v2) ──────
# Reads  : Keyword_n8n > Url_data_ext tab  (URL column)
# Updates: Keyword_n8n > Url_data_ext tab   (adds Meta_Title column)
# Fixes  : og:title fallback, full HTML search, UA rotation, no raise_for_status
# ─────────────────────────────────────────────────────────────────────

import requests as req_lib
import time, re
from bs4 import BeautifulSoup
import gspread
from app.utils.helper import get_sheet_client

# ── Config ────────────────────────────────────────────────────────────
SHEET_NAME = SPREADSHEET_NAME
TAB_NAME   = "Url_data_ext"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

gc = get_sheet_client(SCOPES)

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


# ═══════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════

print("\n" + "="*65)
print("🏷️  META TITLE EXTRACTOR (v2 — multi-strategy)")
print("="*65)

sp = gc.open(SHEET_NAME)

try:
    ws      = sp.worksheet(TAB_NAME)
    records = ws.get_all_records()
except Exception:
    print(f"❌ Tab '{TAB_NAME}' not found. Run the URL Scraper cell first.")
    records = []

if records:
    headers = list(records[0].keys())

    if "Meta_Title" in headers:
        meta_col_idx = headers.index("Meta_Title") + 1
        print(f"  📋 Meta_Title column found at col {meta_col_idx}")
    else:
        meta_col_idx = len(headers) + 1
        col_letter   = chr(64 + min(meta_col_idx, 26))
        ws.update_acell(f"{col_letter}1", "Meta_Title")
        print(f"  📋 Added Meta_Title column at col {meta_col_idx}")

    col_letter = chr(64 + min(meta_col_idx, 26))

    extracted     = 0
    skipped       = 0
    failed        = 0
    source_counts: dict[str, int] = {}

    for i, row in enumerate(records, start=2):
        url           = str(row.get("URL", "")).strip()
        existing_meta = str(row.get("Meta_Title", "")).strip()

        if not url or url.lower() in ("nan", ""):
            continue

        # Re-process "(not found)" rows — they'll get a second attempt
        if existing_meta and existing_meta not in ("nan", "(not found)", "") and len(existing_meta) > 5:
            print(f"  [{i}] ✅ Skip (already extracted)")
            skipped += 1
            continue

        print(f"\n  [{i}] 🌐 {url[:70]}")
        title, source = extract_meta_title(url)

        if title:
            ws.update_acell(f"{col_letter}{i}", title)
            print(f"       ✔ [{source}] \"{title[:80]}\"")
            extracted += 1
            source_counts[source] = source_counts.get(source, 0) + 1
        else:
            ws.update_acell(f"{col_letter}{i}", "(not found)")
            print(f"       ✖ No title found — marked (not found)")
            failed += 1

        time.sleep(1.5)

    print(f"\n{'='*65}")
    print(f"✅ META TITLE EXTRACTION COMPLETE")
    print(f"   Extracted : {extracted}  |  Skipped: {skipped}  |  Failed: {failed}")
    if source_counts:
        print(f"   By source : {source_counts}")
    print(f"{'='*65}")