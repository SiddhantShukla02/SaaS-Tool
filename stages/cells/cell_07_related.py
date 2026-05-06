# ─────────────────────────────────────────────────────────────
# RELATED SEARCHES FETCHER
# ─────────────────────────────────────────────────────────────
# PURPOSE:
#   Fetches related search queries for each keyword using Brave Search API.
#
# INPUT:
#   - run_keywords (Postgres)
#     → keyword, country_code
#
# PROCESS:
#   - Deduplicates keyword/country pairs
#   - Calls Brave Search API
#   - Extracts:
#       → native related queries
#       → infobox suggestions
#       → title-derived queries
#   - Expands coverage using modifiers (cost, treatment, guide)
#
# OUTPUT:
#   - Stored in DB via save_related_searches
#   - Used in question bank and blog generation
#
# NOTES:
#   - REQUEST_DELAY controls API pacing
#   - No Google Sheets dependency
#
# WHY BRAVE (instead of pytrends):
#   - pytrends frequently hits rate limits
#   - returns little/no data for niche medical queries
#   - Brave provides consistent related query data via API
# ─────────────────────────────────────────────────────────────

import os
import time

import requests

from app.repositories.run_repo import get_run_keywords
from app.repositories.search_repo import save_related_searches
from config import BRAVE_API_KEY


# ── Related search settings ──────────────────────────────────

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
# MAIN
# ─────────────────────────────────────────────
def main():

    run_id_str = os.environ.get("SAAS_RUN_ID")
    if not run_id_str:
        raise ValueError("SAAS_RUN_ID not set")
    run_id = int(run_id_str)

    keyword_pairs = [
    {
        "keyword": row["keyword"],
        "country": row["country_code"],
    }
    for row in get_run_keywords(run_id)
    ]

    if not keyword_pairs:
        raise ValueError("❌ No valid keyword/country pairs found.")

    # Deduplicate: same keyword + same country = one search
    seen = set()
    unique_pairs = []
    all_related_rows = []
    for pair in keyword_pairs:
        key = (pair["keyword"].lower(), pair["country"])
        if key not in seen:
            seen.add(key)
            unique_pairs.append(pair)

    print(f"\n🔎 Fetching related searches via Brave Search API")
    print(f"   Unique keyword×country pairs: {len(unique_pairs)}")
    print(f"   Est. time: ~{round(len(unique_pairs) * REQUEST_DELAY * 2 / 60, 1)} min\n")


    total_fetched = 0

    for pair in unique_pairs:
        kw      = pair["keyword"]
        country = pair["country"]
        geo     = COUNTRY_MAP.get(country, country.upper())

        print(f"\n  📍 '{kw}'  [country: {country} → geo: {geo}]")

        queries = extract_related_queries(kw, country)

        if not queries:
            print(f"     ⚠️  No related searches found")
            continue

        for idx,q in enumerate(queries, start=1):
            all_related_rows.append({
                "keyword": kw,
                "country_code": country,
                "type": q["type"],
                "position": idx ,
                "query": q["query"],
                "source": q["source"],
            })            
            total_fetched += 1

        # Show sample
        sample = [q["query"] for q in queries[:3]]
        print(f"     ✅ {len(queries)} queries found → e.g. {sample}")
    
    save_related_searches(run_id, all_related_rows)
    

    print(f"\n📊 SUMMARY")
    print(f"   Keyword/country pairs processed : {len(unique_pairs)}")
    print(f"   Total related queries fetched   : {total_fetched}")
    print(f"   Rows processed                  : {len(all_related_rows)}")
    print("\nSample related queries:")
    for row in all_related_rows[:3]:
        print(row)
if __name__ == "__main__":
    main()