"""
Related Searches Fetcher (via Brave Search API)
=================================================
Reads:  Keyword_n8n → "keyword" tab  (columns: Keyword, Country_Code)
Writes: Keyword_n8n → "Related_search" tab

WHY BRAVE INSTEAD OF PYTRENDS:
  - pytrends hits Google 429 rate limits on 2/3 requests
  - pytrends returns NO_DATA for 95% of niche medical keywords
  - Brave Search API: 2,000 free queries/month, reliable, returns
    "related searches" directly in the response JSON

Strategy:
  For each keyword × country, we make TWO Brave searches:
    1. The keyword itself → extract Brave's "related searches" from response
    2. The keyword + "related" → extract title-derived query suggestions
  This reliably produces 15-30 related queries per keyword vs pytrends' 0.
"""

import time
import requests
import gspread
# ─── PATCHED v16.1: Brave key from config, break bug fixed ────
from config import BRAVE_API_KEY, SPREADSHEET_NAME, SCOPES

from app.utils.helper import get_sheet_client
# ──────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
INPUT_TAB            = "keyword"
OUTPUT_TAB           = "Related_search"

REQUEST_DELAY = 1.0   # Seconds between Brave API calls

# Country code → Brave 'country' param (ISO 3166-1 alpha-2 uppercase)
COUNTRY_MAP = {
    "ae": "AE", "gb": "GB", "uk": "GB", "us": "US", "in": "IN",
    "au": "AU", "ca": "CA", "et": "ET", "ng": "NG", "ke": "KE",
    "za": "ZA", "sg": "SG", "de": "DE", "fr": "FR", "sa": "SA",
    "pk": "PK", "bd": "BD", "lk": "LK", "np": "NP", "iq": "IQ",
    "om": "OM", "qa": "QA", "bh": "BH", "kw": "KW",
}




# ─────────────────────────────────────────────
# READ KEYWORDS
# ─────────────────────────────────────────────
def read_keywords(spreadsheet) -> list:
    ws      = spreadsheet.worksheet(INPUT_TAB)
    records = ws.get_all_records()

    if not records:
        raise ValueError(f"❌ '{INPUT_TAB}' tab is empty or missing headers.")

    keywords = []
    for i, row in enumerate(records, start=2):
        kw      = str(row.get("Keyword",      "")).strip()
        country = str(row.get("Country_Code", "")).strip().lower()

        if not kw:
            continue
        if not country:
            continue

        keywords.append({"keyword": kw, "country": country})

    print(f"✅ Loaded {len(keywords)} keyword/country pairs from '{INPUT_TAB}'")
    return keywords

# ─────────────────────────────────────────────
# BRAVE SEARCH → RELATED QUERIES
# ─────────────────────────────────────────────
def brave_search(query, country_code="", count=10):
    """
    Call Brave Search API and return the full JSON response.
    """
    headers = {
        "X-Subscription-Token": BRAVE_API_KEY,
        "Accept": "application/json",
    }
    params = {
        "q": query,
        "count": count,
    }
    if country_code:
        params["country"] = country_code

    try:
        resp = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            headers=headers,
            params=params,
            timeout=15,
        )
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 429:
            print(f"    ⚠️  Rate limited — waiting 10s...")
            time.sleep(10)
            return None
        else:
            print(f"    ⚠️  Brave returned {resp.status_code}")
            return None
    except Exception as e:
        print(f"    ⚠️  Brave request failed: {e}")
        return None

def extract_related_queries(keyword, country_code):
    """
    Extract related search queries from Brave Search results.

    Brave returns related searches in response["query"]["related_queries"]
    and also sometimes in response["web"]["results"] titles.

    We make two searches:
      1. The keyword itself → Brave's native related queries
      2. "{keyword} vs" or "{keyword} alternatives" → title-derived queries
    """
    geo = COUNTRY_MAP.get(country_code, country_code.upper())
    all_queries = {}   # dict to deduplicate: query_text → {"type": ..., "source": ...}

    # ── Search 1: Direct keyword → native related queries ────
    data = brave_search(keyword, geo, count=20)
    if data:
        # Extract from Brave's related queries section
        related = data.get("query", {}).get("related_queries", [])
        if isinstance(related, list):
            for q in related:
                if isinstance(q, str) and q.strip():
                    all_queries[q.strip().lower()] = {
                        "query": q.strip(),
                        "type": "RELATED",
                        "source": "brave_related",
                    }
                elif isinstance(q, dict):
                    qt = q.get("query", q.get("text", "")).strip()
                    if qt:
                        all_queries[qt.lower()] = {
                            "query": qt,
                            "type": "RELATED",
                            "source": "brave_related",
                        }

        # Extract from "also searched for" / "infobox" if present
        infobox = data.get("infobox", {})
        if isinstance(infobox, dict):
            for item in infobox.get("results", []):
                title = item.get("title", "").strip()
                if title and keyword.split()[0].lower() in title.lower():
                    all_queries[title.lower()] = {
                        "query": title,
                        "type": "ALSO_SEARCHED",
                        "source": "brave_infobox",
                    }

        # Extract long-tail variations from SERP result titles
        for result in data.get("web", {}).get("results", [])[:10]:
            title = result.get("title", "")
            # Only keep titles that contain the core keyword terms
            kw_words = set(keyword.lower().split())
            title_words = set(title.lower().split())
            overlap = len(kw_words & title_words)
            if overlap >= max(1, len(kw_words) // 2) and title.lower() != keyword.lower():
                # Clean: remove site name after | or -
                clean_title = title.split("|")[0].split(" - ")[0].strip()
                if len(clean_title) > 10 and len(clean_title) < 100:
                    all_queries[clean_title.lower()] = {
                        "query": clean_title,
                        "type": "SERP_DERIVED",
                        "source": "brave_title",
                    }

    time.sleep(REQUEST_DELAY)

    # ── Search 2: "{keyword} cost/treatment/guide" → more related ─
    # Use a modifier that expands coverage
    modifiers = ["cost", "treatment", "guide"]
    core_word = keyword.split()[0]  # e.g., first word from the keyword phrase
    for mod in modifiers:
        mod_query = f"{core_word} {mod}"
        if mod_query.lower() == keyword.lower():
            continue  # Skip if identical to original

        data2 = brave_search(mod_query, geo, count=10)
        if data2:
            related2 = data2.get("query", {}).get("related_queries", [])
            if isinstance(related2, list):
                for q in related2:
                    qt = q.strip() if isinstance(q, str) else (q.get("query", q.get("text", "")) if isinstance(q, dict) else "")
                    qt = qt.strip()
                    if qt and qt.lower() not in all_queries:
                        all_queries[qt.lower()] = {
                            "query": qt,
                            "type": "EXPANDED",
                            "source": f"brave_{mod}",
                        }

            # Also extract titles
            for result in data2.get("web", {}).get("results", [])[:5]:
                title = result.get("title", "")
                kw_words = set(keyword.lower().split())
                title_words = set(title.lower().split())
                if len(kw_words & title_words) >= 1:
                    clean = title.split("|")[0].split(" - ")[0].strip()
                    if 10 < len(clean) < 100 and clean.lower() not in all_queries:
                        all_queries[clean.lower()] = {
                            "query": clean,
                            "type": "EXPANDED",
                            "source": f"brave_{mod}_title",
                        }

        time.sleep(REQUEST_DELAY)  # (v16.1: removed break — iterate all 3 modifiers as intended)

    return list(all_queries.values())

# ─────────────────────────────────────────────
# WRITE RESULTS
# ─────────────────────────────────────────────
def write_results(spreadsheet, rows):
    try:
        ws = spreadsheet.worksheet(OUTPUT_TAB)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=OUTPUT_TAB, rows=2000, cols=10)
        print(f"  ℹ️  Created new tab: '{OUTPUT_TAB}'")

    ws.clear()
    ws.update(rows, value_input_option="USER_ENTERED")
    print(f"✅ Written {len(rows) - 1} data rows to '{OUTPUT_TAB}'")

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print("🔗 Connecting to Google Sheets...")
    client = get_sheet_client(SCOPES)
    spreadsheet = client.open(SPREADSHEET_NAME)
    print(f"   Opened: '{SPREADSHEET_NAME}'")

    keyword_pairs = read_keywords(spreadsheet)
    if not keyword_pairs:
        raise ValueError("❌ No valid keyword/country pairs found.")

    # Deduplicate: same keyword + same country = one search
    seen = set()
    unique_pairs = []
    for pair in keyword_pairs:
        key = (pair["keyword"].lower(), pair["country"])
        if key not in seen:
            seen.add(key)
            unique_pairs.append(pair)

    print(f"\n🔎 Fetching related searches via Brave Search API")
    print(f"   Unique keyword×country pairs: {len(unique_pairs)}")
    print(f"   Est. time: ~{round(len(unique_pairs) * REQUEST_DELAY * 2 / 60, 1)} min\n")

    output_rows = [[
        "Keyword",
        "Country_Code",
        "Geo",
        "Type",            # RELATED / SERP_DERIVED / ALSO_SEARCHED / EXPANDED
        "Related_Query",
        "Source",          # brave_related / brave_title / brave_infobox / brave_{mod}
    ]]

    total_fetched = 0

    for pair in unique_pairs:
        kw      = pair["keyword"]
        country = pair["country"]
        geo     = COUNTRY_MAP.get(country, country.upper())

        print(f"\n  📍 '{kw}'  [country: {country} → geo: {geo}]")

        queries = extract_related_queries(kw, country)

        if not queries:
            print(f"     ⚠️  No related searches found")
            output_rows.append([kw, country, geo, "NO_DATA", "", ""])
            continue

        for q in queries:
            output_rows.append([
                kw,
                country,
                geo,
                q["type"],
                q["query"],
                q["source"],
            ])
            total_fetched += 1

        # Show sample
        sample = [q["query"] for q in queries[:3]]
        print(f"     ✅ {len(queries)} queries found → e.g. {sample}")

    # Write results
    print(f"\n📝 Writing results to sheet...")
    write_results(spreadsheet, output_rows)

    print(f"\n📊 SUMMARY")
    print(f"   Keyword/country pairs processed : {len(unique_pairs)}")
    print(f"   Total related queries fetched   : {total_fetched}")
    print(f"   Rows written to sheet           : {len(output_rows) - 1}")
    print(f"\n✅ Done — check '{OUTPUT_TAB}' tab in '{SPREADSHEET_NAME}'")

if __name__ == "__main__":
    main()