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

import requests
import gspread
import time
from app.utils.helper import get_sheet_client
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
# GOOGLE SHEETS CONNECTION
# ==============================

def connect_google_sheet():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    client = get_sheet_client(scope)
    return client.open(SPREADSHEET_NAME)

# ==============================
# READ KEYWORDS
# ==============================

def read_keywords(sheet):
    return sheet.worksheet("keyword").get_all_records()

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
# WRITE TO SERP_URL TAB
# FIX: Use batch append_rows() instead of per-row append_row()
# ==============================

def write_urls(sheet, rows):
    if not rows:
        print("No URL results to write.")
        return
    try:
        ws = sheet.worksheet("Serp_Url")
    except:
        ws = sheet.add_worksheet(title="Serp_Url", rows="1000", cols="4")

    ws.clear()
    time.sleep(2)  # Brief pause after clear

    header = [["Keyword", "Country_Code", "Rank", "URL"]]
    batch  = [[r["keyword"], r["country_code"], r["rank"], r["url"]] for r in rows]

    ws.append_rows(header + batch, value_input_option="RAW")  # ✅ Single API call
    print(f"✅ Serp_Url tab: {len(rows)} rows written.")

# ==============================
# WRITE TO PAA TAB
# FIX: Use batch append_rows() instead of per-row append_row()
# ==============================

def write_paa(sheet, rows):
    if not rows:
        print("No PAA results to write.")
        return
    try:
        ws = sheet.worksheet("PAA")
    except:
        ws = sheet.add_worksheet(title="PAA", rows="1000", cols="7")

    ws.clear()
    time.sleep(2)  # Brief pause after clear

    header = [["Keyword", "Country_Code", "Rank", "Question", "Snippet", "Source", "Source_URL"]]
    batch  = [
        [r["keyword"], r["country_code"], r["rank"],
         r["question"], r["snippet"], r["source"], r["source_url"]]
        for r in rows
    ]

    ws.append_rows(header + batch, value_input_option="RAW")  # ✅ Single API call
    print(f"✅ PAA tab: {len(rows)} rows written.")

# ==============================
# MAIN
# ==============================

def main():
    sheet        = connect_google_sheet()
    keyword_data = read_keywords(sheet)

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

    write_urls(sheet, all_urls)

    time.sleep(5)  # ✅ Wait between writing the two tabs to avoid rate limit

    write_paa(sheet, all_paa)

    print("\n✅ Google Search Script Completed! (1 API call per keyword → Serp_Url + PAA tabs)")

if __name__ == "__main__":
    main()