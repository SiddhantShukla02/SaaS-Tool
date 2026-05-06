# ─────────────────────────────────────────────────────────────
# SERP + PAA FETCHER
# ─────────────────────────────────────────────────────────────
# PURPOSE:
#   Fetches Google SERP results and People Also Ask (PAA)
#   questions for each keyword.
#
# INPUT:
#   - run_keywords (Postgres)
#     → keyword, country_code
#
# PROCESS:
#   - Calls SERP API (1 call per keyword)
#   - Extracts top organic URLs
#   - Filters unwanted domains
#   - Extracts PAA questions
#
# OUTPUT:
#   - URLs → save_serp_urls
#   - PAA   → save_paa_questions
#
# NOTES:
#   - EXCLUDED_DOMAINS currently duplicated (pending design decision)
# ─────────────────────────────────────────────────────────────

import os

import requests

from app.repositories.run_repo import get_run_keywords
from app.repositories.search_repo import save_paa_questions, save_serp_urls
from config import EXCLUDED_DOMAINS, SERP_API_KEY


EXCLUDED_DOMAINS = [
    "youtube.com", "youtu.be", "facebook.com", "instagram.com",
    "linkedin.com", "twitter.com", "wikipedia.org", "justdial.com",
    "practo.com", "healthgrades.com", "webmd.com", "mayoclinic.org",
    "clevelandclinic.org", "amazon.", "flipkart."
]



# ==============================
# SINGLE API CALL → URLs + PAA
# ==============================

def fetch_google_search(keyword, country_code):
    params = {
        "engine":  "google",
        "q":       keyword,
        "gl":      country_code.lower().strip(),
        "hl":      "en",
        "num":     10,
        "api_key": SERP_API_KEY
    }

    response = requests.get("https://serpapi.com/search", params=params)
    data     = response.json()

    if "error" in data:
        print(f"  [SERP ERROR] {data['error']}")
        return [], []

    # --- Extract Organic URLs ---
    urls = []
    for result in data.get("organic_results", []):
        link = result.get("link", "")
        if not link:
            continue
        if any(domain in link.lower() for domain in EXCLUDED_DOMAINS):
            print(f"  [SKIP] {link}")
            continue
        urls.append({"rank": len(urls) + 1, "url": link})
        if len(urls) == 10:
            break

    # --- Extract People Also Ask ---
    paa = []
    for idx, item in enumerate(data.get("related_questions", []), start=1):
        paa.append({
            "rank":       idx,
            "question":   item.get("question", ""),
            "snippet":    item.get("snippet", ""),
            "source":     item.get("source", ""),
            "source_url": item.get("link", "")
        })

    print(f"  URLs: {len(urls)} | PAA: {len(paa)}")
    return urls, paa


# ==============================
# MAIN
# ==============================

def main():
    run_id_str = os.environ.get("SAAS_RUN_ID")

    if not run_id_str:
        raise ValueError("SAAS_RUN_ID not set")

    run_id = int(run_id_str)

    keyword_data = [
        {
            "Keyword": row["keyword"],
            "Country_Code": row["country_code"],
        }
        for row in get_run_keywords(run_id)
    ]

    all_urls = []
    all_paa  = []

    for row in keyword_data:
        keyword      = str(row.get("Keyword", "")).strip()
        country_code = str(row.get("Country_Code", "")).strip()

        if not keyword or not country_code:
            print("Skipping empty row:", row)
            continue

        print(f"\n🔍 '{keyword}' | Country: {country_code}")

        urls, paa = fetch_google_search(keyword, country_code)

        for r in urls:
            all_urls.append({"keyword": keyword, "country_code": country_code, **r})
        for r in paa:
            all_paa.append({"keyword": keyword, "country_code": country_code, **r})

    save_serp_urls(run_id, all_urls)
    save_paa_questions(run_id, all_paa)
    
    print("\nURLs:")
    for row in all_urls[:3]:
        print(row)

    print("\nPAA:")
    for row in all_paa[:3]:
        print(row)

    print("\n✅ Google Search Script Completed! (1 API call per keyword → URLs + PAA collected)")

if __name__ == "__main__":
    main()