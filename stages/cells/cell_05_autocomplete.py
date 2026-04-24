# """
# Google Autocomplete Fetcher
# ============================
# Reads: Keyword_n8n → "keyword" tab (columns: Keyword, Country_Code)
# Writes: Keyword_n8n → "Other_PAA" tab

# Setup:
#     pip install gspread google-auth requests
#     Place your Google service account JSON key at: service_account.json
#     Share the sheet with the service account email.
# """

import time
import urllib.parse
import requests
import gspread
from app.utils.helper import get_sheet_client
# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
from config import SPREADSHEET_NAME
                                                 # Exact name of the Google Sheet
INPUT_TAB            = "keyword"                 # Tab to read keywords from
OUTPUT_TAB           = "Other_Autocomplete"              # Tab to write results to

INTENT_MODIFIERS = [
    "",          # Base keyword
    " how",      # Informational
    " cost",     # Transactional
    " best",     # Comparison
    " why",      # Motivational
    " which",    # Choice comparison
    " vs",       # Direct comparison
]

REQUEST_DELAY = 1.5   # Seconds between API calls (avoids Google rate-limiting)

# ─────────────────────────────────────────────
# READ KEYWORDS FROM INPUT TAB
# ─────────────────────────────────────────────
def read_keywords(spreadsheet) -> list[dict]:
    """
    Reads the 'keyword' tab.
    Expects header row: Keyword | Country_Code
    Returns list of {"keyword": str, "country": str}
    """
    ws      = spreadsheet.worksheet(INPUT_TAB)
    records = ws.get_all_records()            # Converts header row → dict keys

    if not records:
        raise ValueError(f"❌ '{INPUT_TAB}' tab is empty or missing headers.")

    # Normalise column names (strip whitespace, lowercase for matching)
    keywords = []
    for i, row in enumerate(records, start=2):   # start=2 → row number in sheet
        kw      = str(row.get("Keyword", "")).strip()
        country = str(row.get("Country_Code", "")).strip().lower()

        if not kw:
            print(f"  ⚠️  Row {i}: empty Keyword — skipped")
            continue
        if not country:
            print(f"  ⚠️  Row {i}: '{kw}' has no Country_Code — skipped")
            continue

        keywords.append({"keyword": kw, "country": country})

    print(f"✅ Loaded {len(keywords)} keyword/country pairs from '{INPUT_TAB}'")
    return keywords


# ─────────────────────────────────────────────
# GOOGLE AUTOCOMPLETE FETCHER
# ─────────────────────────────────────────────
def fetch_google_autocomplete(query: str, language: str = "en", country: str = "us") -> list[str]:
    """
    Calls the Google Suggest API.
    Returns a list of autocomplete suggestion strings, or [] on failure.
    """
    try:
        encoded = urllib.parse.quote(query)
        url = (
            f"https://suggestqueries.google.com/complete/search"
            f"?client=firefox&q={encoded}&hl={language}&gl={country}"
        )
        resp = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            timeout=10,
        )
        resp.raise_for_status()
        data        = resp.json()
        suggestions = data[1] if isinstance(data, list) and len(data) > 1 else []
        return [s for s in suggestions if isinstance(s, str)]

    except Exception as e:
        print(f"    ⚠️  Autocomplete fetch failed for '{query[:50]}': {e}")
        return []


# ─────────────────────────────────────────────
# WRITE RESULTS TO OUTPUT TAB
# ─────────────────────────────────────────────
def write_results(spreadsheet, rows: list[list]):
    """
    Clears and rewrites the Other_PAA tab.
    Creates the tab if it doesn't exist.
    """
    try:
        ws = spreadsheet.worksheet(OUTPUT_TAB)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=OUTPUT_TAB, rows=1000, cols=10)
        print(f"  ℹ️  Created new tab: '{OUTPUT_TAB}'")

    ws.clear()
    ws.update(rows, value_input_option="USER_ENTERED")
    print(f"✅ Written {len(rows) - 1} data rows to '{OUTPUT_TAB}'")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    # 1. Connect to Google Sheets
    print("🔗 Connecting to Google Sheets...")
    scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
    ]
    client = get_sheet_client(scopes)
    spreadsheet = client.open(SPREADSHEET_NAME)
    print(f"   Opened: '{SPREADSHEET_NAME}'")

    # 2. Read keywords
    keyword_pairs = read_keywords(spreadsheet)
    if not keyword_pairs:
        raise ValueError("❌ No valid keyword/country pairs found — check the 'keyword' tab.")

    total_calls = len(keyword_pairs) * len(INTENT_MODIFIERS)
    print(f"\n🔎 Starting autocomplete fetch")
    print(f"   Pairs loaded      : {len(keyword_pairs)}")
    print(f"   Modifiers / pair  : {len(INTENT_MODIFIERS)}")
    print(f"   Total API calls   : {total_calls}")
    print(f"   Est. time         : ~{round(total_calls * REQUEST_DELAY / 60, 1)} min\n")

    # 3. Fetch autocomplete for every keyword × country × modifier
    all_rows   = {}   # key = (keyword, country, modifier) → [suggestions]
    unique_set = set()

    for pair in keyword_pairs:
        kw      = pair["keyword"]
        country = pair["country"]
        print(f"\n  📍 '{kw}'  [country: {country}]")

        for modifier in INTENT_MODIFIERS:
            query       = kw + modifier
            suggestions = fetch_google_autocomplete(query, country=country)
            key         = (kw, country, modifier.strip() or "base")

            if suggestions:
                all_rows[key] = suggestions
                for s in suggestions:
                    unique_set.add(s.lower().strip())
                print(f"     ✅ '{query}' → {len(suggestions)} suggestions: {suggestions[:2]}...")
            else:
                print(f"     ⚠️  No results for '{query}'")

            time.sleep(REQUEST_DELAY)

    print(f"\n   Total unique suggestions collected: {len(unique_set)}")

    # 4. Build output rows
    output_rows = [[
        "Keyword",
        "Country_Code",
        "Modifier",
        "Suggestions",
        "Suggestion_Count",
    ]]

    for (kw, country, modifier), suggestions in all_rows.items():
        output_rows.append([
            kw,
            country,
            modifier,
            " | ".join(suggestions),
            len(suggestions),
        ])

    # 5. Write to Other_PAA tab
    write_results(spreadsheet, output_rows)

    print(f"\n📊 SUMMARY")
    print(f"   Keyword/country pairs : {len(keyword_pairs)}")
    print(f"   API calls made        : {len(all_rows)}")
    print(f"   Unique suggestions    : {len(unique_set)}")
    print(f"   Rows written to sheet : {len(output_rows) - 1}")
    print(f"\n✅ Done — check '{OUTPUT_TAB}' tab in '{SPREADSHEET_NAME}'")


if __name__ == "__main__":
    main()