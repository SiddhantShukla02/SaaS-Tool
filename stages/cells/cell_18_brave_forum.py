# ─────────────────────────────────────────────────────────────
# BRAVE FORUM SEARCH COLLECTOR
# ─────────────────────────────────────────────────────────────
# PURPOSE:
#   Finds patient discussions across Quora, forums, review sites,
#   and Reddit-indexed pages using Brave Search API.
#
# INPUT:
#   - run_keywords (Postgres)
#     → keyword
#
# PROCESS:
#   - Builds source-specific Brave queries
#   - Searches Quora, health forums, and review pages
#   - Normalizes titles, snippets, URLs, and source type
#
# OUTPUT:
#   - Forum search results → Postgres via insert_forum_search_result
#
# NOTES:
#   - Uses Brave Search API
#   - Reddit native scraping is handled separately in cell_16
#   - No Google Sheets dependency
# ─────────────────────────────────────────────────────────────

import os
import time

import requests

from app.repositories.run_repo import get_run_keywords
from app.repositories.search_repo import insert_forum_search_result
from config import BRAVE_API_KEY

# ── Config ────────────────────────────────────────────────────────────
MAX_CELL = 49000

# ── Auth ──────────────────────────────────────────────────────────────

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

# ── Helpers ─────────────────────────────────────────────────────
def _trunc(v):
    s = str(v) if v is not None else ""
    return s[:MAX_CELL] + "\n[TRUNCATED]" if len(s) > MAX_CELL else s

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


    run_id_raw = os.environ.get("RUN_ID")
    if not run_id_raw:
        raise RuntimeError("RUN_ID env var is required")
    run_id = int(run_id_raw)
    records = get_run_keywords(run_id)

    collector = BraveForumSearchCollector(
        api_key=BRAVE_API_KEY,
    )


    print(f"\n💾 Saving forum search results to DB...")

    for row in records:
        keyword = str(row.get("keyword", "")).strip()
        if not keyword:
            continue

        print(f"\n  Keyword: '{keyword}'")

        results = collector.collect_from_sources(
            keyword=keyword,
            sources=["quora", "forums", "reviews"],
            results_per_source=10,
        )

        for r in results["results"]:
            insert_forum_search_result(
                run_id=run_id,
                keyword=keyword,
                source_type=r["source_type"],
                title=_trunc(r["title"]),
                snippet=_trunc(r["snippet"]),
                url=r["url"],
                display_link=r["source"],
            )

    print(f"\n{'='*65}")
    print(f"✅ BRAVE FORUM SEARCH COMPLETE")
    print(f"{'='*65}")