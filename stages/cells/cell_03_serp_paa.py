from app.repositories.search_repo import save_serp_urls, save_paa_questions
from config import SERP_API_KEY, EXCLUDED_DOMAINS

import requests
import time
# ==============================
# CONFIGURATION
# ==============================




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
    keyword_data = [
    {"Keyword": "heart surgery cost in india", "Country_Code": "US"},
    ]
    run_id = 1  # temporary, will replace later

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