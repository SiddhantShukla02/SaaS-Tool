# """
# Google Autocomplete Fetcher
# ============================
# Reads: Keyword_n8n → "keyword" tab (columns: Keyword, Country_Code)
# Writes: Keyword_n8n → "Other_PAA" tab
# """
from app.repositories.search_repo import save_autocomplete_suggestions
import time
import urllib.parse
import requests
# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

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
# MAIN
# ─────────────────────────────────────────────
def main():
    run_id = 1  # temporary
    # 2. Read keywords
    keyword_pairs = [
    {"keyword": "heart surgery cost in india", "country": "us"},
    ]
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
    save_autocomplete_suggestions(run_id, all_rows)

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

    print("\nSample suggestions:")
    for k, v in list(all_rows.items())[:2]:
        print(k, v[:3])

    print(f"\n📊 SUMMARY")
    print(f"   Keyword/country pairs : {len(keyword_pairs)}")
    print(f"   API calls made        : {len(all_rows)}")
    print(f"   Unique suggestions    : {len(unique_set)}")
    print(f"   Suggestion groups     : {len(all_rows)}")
    print("\n✅ Autocomplete fetch completed")

if __name__ == "__main__":
    main()