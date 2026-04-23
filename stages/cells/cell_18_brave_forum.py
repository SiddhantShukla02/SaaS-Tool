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

"""
Brave Forum Search Collector
==============================
Uses Brave Search API to find patient discussions on Quora,
health forums, and review sites.

Reads  : Keyword_n8n → "keyword" tab
Writes : Keyword_n8n → "Google_Forum_Insights" tab

Setup:
  1. Get a Brave Search API key: https://api-dashboard.search.brave.com/register
  2. Choose the "Data for Search" plan (free — 2,000 queries/month)
  3. Set your API key below or as environment variable BRAVE_API_KEY

NOTE: If you don't have a Brave key yet, skip this cell — Reddit insights
alone (Cell above) are sufficient for the content pipeline.
"""

import requests
import json
import time
import os
import gspread
# ─── PATCHED v16.1: Brave key from config ────
from config import BRAVE_API_KEY, CREDS_FILE, SPREADSHEET_NAME, SCOPES
# ─────────────────────────────────────────────

from google.oauth2.service_account import Credentials

# ── Config ────────────────────────────────────────────────────────────
SHEET_NAME        = SPREADSHEET_NAME
INPUT_TAB         = "keyword"
OUTPUT_TAB        = "Google_Forum_Insights"
CREDS_FILE        = "creds_data.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

MAX_CELL = 49000

# ── Auth ──────────────────────────────────────────────────────────────
creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
gc    = gspread.authorize(creds)

class BraveForumSearchCollector:
    """Collects patient discussions from Quora, forums, and review sites via Brave Search API."""

    BRAVE_SEARCH_URL = "https://api.search.brave.com/res/v1/web/search"

    SOURCE_CONFIGS = {
        "quora": {
            "site_restrict": "site:quora.com",
            "extra_terms": "",
            "description": "Quora Q&A",
        },
        "forums": {
            "site_restrict": "",
            "extra_terms": "forum OR community OR discussion OR patient experience",
            "description": "Health Forums",
        },
        "reviews": {
            "site_restrict": "site:trustpilot.com OR site:google.com/maps",
            "extra_terms": "medical tourism India review",
            "description": "Review Sites",
        },
        "reddit_via_brave": {
            "site_restrict": "site:reddit.com",
            "extra_terms": "",
            "description": "Reddit (via Brave — finds top-ranked threads)",
        },
    }

    def __init__(self, api_key, rate_limit_seconds=1.0):
        self.api_key = api_key
        self.rate_limit = rate_limit_seconds
        self.last_request_time = 0

    def _search(self, query, num_results=10):
        """
        Search using Brave Search API.
        
        Brave API differences from Google CSE:
        - Auth via header (X-Subscription-Token) instead of query param
        - Max 20 results per request via 'count' param
        - Uses 'offset' for pagination instead of 'start'
        - Results are in response["web"]["results"] instead of response["items"]
        """
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)

        headers = {
            "X-Subscription-Token": self.api_key,
            "Accept": "application/json",
        }
        params = {
            "q": query,
            "count": min(num_results, 20),
        }

        try:
            response = requests.get(self.BRAVE_SEARCH_URL, headers=headers, params=params, timeout=15)
            self.last_request_time = time.time()

            if response.status_code == 200:
                data = response.json()
                web_results = data.get("web", {}).get("results", [])
                return [
                    {
                        "title": item.get("title", ""),
                        "snippet": item.get("description", ""),
                        "url": item.get("url", ""),
                        "source": item.get("meta_url", {}).get("hostname", item.get("url", "").split("/")[2] if "://" in item.get("url", "") else ""),
                    }
                    for item in web_results
                ]
            elif response.status_code == 429:
                print(f"  [WARN] Rate limited — waiting 5 seconds...")
                time.sleep(5)
                return []
            else:
                error_msg = ""
                try:
                    error_msg = response.json().get("message", response.text[:200])
                except Exception:
                    error_msg = response.text[:200]
                print(f"  [ERROR] Brave API returned {response.status_code}: {error_msg}")
                return []
        except Exception as e:
            print(f"  [ERROR] Request failed: {e}")
            return []

    def collect_from_sources(self, keyword, sources=None, results_per_source=10):
        """
        Collect results from multiple source types for a given keyword.
        
        Args:
            keyword: Search keyword
            sources: List of source keys (default: quora, forums, reviews)
            results_per_source: Max results per source type
        
        Returns:
            Dict with keyword, total_results, and results list
        """
        if sources is None:
            sources = ["quora", "forums", "reviews"]

        all_results = []
        for source in sources:
            config = self.SOURCE_CONFIGS.get(source)
            if not config:
                continue

            query_parts = [keyword]
            if config.get("site_restrict"):
                query_parts.append(config["site_restrict"])
            if config.get("extra_terms"):
                query_parts.append(config["extra_terms"])
            query = " ".join(query_parts)

            print(f"  Searching {config['description']}: {query[:80]}...")
            results = self._search(query, num_results=results_per_source)
            for r in results:
                r["source_type"] = source
            all_results.extend(results)
            print(f"  Found {len(results)} results")

        return {
            "keyword": keyword,
            "total_results": len(all_results),
            "results": all_results,
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
            if attempt < retries - 1:
                time.sleep(4 * (attempt + 1))
            else:
                print(f"  ❌ Sheet write failed: {e}")
                return False

def _fmt_header(ws, n_cols):
    col_letter = chr(64 + min(n_cols, 26))
    ws.format(f"A1:{col_letter}1", {
        "textFormat": {"bold": True, "foregroundColor": {"red":1,"green":1,"blue":1}},
        "backgroundColor": {"red": 0.13, "green": 0.37, "blue": 0.60}
    })

def get_or_create_tab(spreadsheet, tab_name, rows=1000, cols=6):
    try:
        ws = spreadsheet.worksheet(tab_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=rows, cols=cols)
    return ws

# ═══════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════

if not BRAVE_API_KEY or BRAVE_API_KEY == "YOUR_BRAVE_API_KEY_HERE":
    print("⚠️  SKIPPING Brave Forum Search — no API key configured.")
    print("   Set BRAVE_API_KEY above or as environment variable to enable.")
    print("   Reddit insights (previous cell) are sufficient for the pipeline.")
else:
    print("\n" + "="*65)
    print("🔍 BRAVE FORUM SEARCH COLLECTOR")
    print("="*65)

    sp = gc.open(SHEET_NAME)
    ws_kw = sp.worksheet(INPUT_TAB)
    records = ws_kw.get_all_records()

    collector = BraveForumSearchCollector(
        api_key=BRAVE_API_KEY,
    )

    out_ws = get_or_create_tab(sp, OUTPUT_TAB, rows=1000, cols=6)
    HEADERS = ["Keyword", "Source_Type", "Title", "Snippet", "URL", "Display_Link"]
    rows_out = [HEADERS]

    for row in records:
        keyword = str(row.get("Keyword", "")).strip()
        if not keyword:
            continue

        print(f"\n  Keyword: '{keyword}'")
        results = collector.collect_from_sources(
            keyword=keyword,
            sources=["quora", "forums", "reviews"],
            results_per_source=10,
        )

        for r in results["results"]:
            rows_out.append([
                keyword,
                r["source_type"],
                _trunc(r["title"]),
                _trunc(r["snippet"]),
                r["url"],
                r["source"],
            ])

    _write_with_retry(out_ws, rows_out)
    _fmt_header(out_ws, len(HEADERS))

    print(f"\n{'='*65}")
    print(f"✅ BRAVE FORUM SEARCH COMPLETE — {len(rows_out)-1} results in '{OUTPUT_TAB}'")
    print(f"{'='*65}")