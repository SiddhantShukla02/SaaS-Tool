# ─────────────────────────────────────────────────────────────
# ENV CONFIG
# ─────────────────────────────────────────────────────────────
# PURPOSE:
#   Loads environment variables and shared configuration.
#
# INPUT:
#   - .env (optional)
#   - config.py
#
# PROCESS:
#   - Loads environment variables
#   - Imports all runtime config (keys, limits, personas, etc.)
#   - Prints summary for visibility
#
# OUTPUT:
#   - No direct output
#   - Prepares environment for all subsequent cells
#
# NOTES:
#   - Executed at start of each stage by runner
# ─────────────────────────────────────────────────────────────

# Load .env (secrets)
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ .env loaded")
except ImportError:
    print("ℹ️  python-dotenv not installed; using system env vars only")

# Ensure project root/config.py is importable
import os
import sys
from pathlib import Path

if str(Path.cwd()) not in sys.path:
    sys.path.insert(0, str(Path.cwd()))

# ── Import shared runtime configuration ──────────────────────────────
from config import (
    # API keys (from .env via config.py's loader)
    SERP_API_KEY, GEMINI_API_KEY, FIRECRAWL_API_KEY, BRAVE_API_KEY,

    # Gemini model + limits
    GEMINI_MODEL, MAX_CELL, MAX_SCRAPE_CHARS, MAX_TOKENS, OUTLINE_TOKENS,
    TEMP_KEYWORD_EXTRACT, TEMP_OUTLINE, TEMP_BLOG_SECTION,
    TEMP_FAQ_ANSWER, TEMP_H1_META,

    # Blog generation targets
    TARGET_WORDS_PER_SECTION, HARD_CAP_WORDS, TARGET_READING_GRADE,
    MIN_STATS_PER_1000, TARGET_FAQ_COUNT, MAX_SENTENCE_WORDS,

    # Brand + business
    BRAND,

    # Partner hospitals + internal links (content quality levers)
    PARTNER_HOSPITALS, get_partner_hospitals,
    INTERNAL_LINKS, get_internal_link_suggestions,

    # Citation allowlist (YMYL safeguard)
    CITATION_ALLOWLIST, all_allowed_citations,

    # Countries + personas
    COUNTRY_MAP, get_country_name,
    COUNTRY_PERSONAS, get_persona,

    # Specialty detection
    SPECIALTY_PATTERNS, detect_specialty,

    # YMYL compliance
    YMYL_DISCLAIMERS, FORBIDDEN_MEDICAL_CLAIMS,

    # Scraping + SERP
    EXCLUDED_DOMAINS, AUTOCOMPLETE_MODIFIERS,
    REDDIT_SUBREDDITS, REDDIT_EXCLUDED_SUBREDDITS,

    # Gemini safety
    SAFETY_OFF,

    # AI-detection tells
    AI_TELL_PHRASES,
)

# ── Status summary ────────────────────────────────────────────────
partner_count = sum(len(v) for v in PARTNER_HOSPITALS.values())
def _count_citations(citation_allowlist):
    total = 0
    for v in citation_allowlist.values():
        if isinstance(v, list):
            total += len(v)
        elif isinstance(v, dict):
            total += sum(len(items) for items in v.values() if isinstance(items, list))
    return total

citation_count = _count_citations(CITATION_ALLOWLIST)


print(f"✅ config.py loaded")
print(f"   Brand              : {BRAND['name']} ({BRAND['website']})")
print(f"   Run ID             : {os.getenv('RUN_ID')}")
print(f"   Model              : {GEMINI_MODEL}")
print(f"   Countries          : {len(COUNTRY_MAP)} | Personas: {len(COUNTRY_PERSONAS)}")
print(f"   Specialties        : {len(SPECIALTY_PATTERNS)}")
print(f"   Partner hospitals  : {partner_count}")
print(f"   Internal links     : {len(INTERNAL_LINKS)}")
print(f"   Citation allowlist : {citation_count}")
print(f"   Reading grade      : Class {TARGET_READING_GRADE}")
print(f"   Word cap           : {HARD_CAP_WORDS}")

# ── Gentle nudges for things worth filling in ────────────────────
if partner_count == 0:
    print("   ⚠️  PARTNER_HOSPITALS is empty — writer will use generic names. "
          "Edit config.py Section 7.")
if len(INTERNAL_LINKS) == 0:
    print("   ⚠️  INTERNAL_LINKS is empty — no internal link suggestions. "
          "Edit config.py Section 8.")
if citation_count < 5:
    print("   ⚠️  CITATION_ALLOWLIST under-filled — writer may be over-restricted. "
          "Edit config.py Section 9.")
