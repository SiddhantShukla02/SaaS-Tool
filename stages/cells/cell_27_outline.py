# ─────────────────────────────────────────────────────────────
# BLOG OUTLINE GENERATOR
# ─────────────────────────────────────────────────────────────
# PURPOSE:
#   Generates a strategic H2/H3 blog outline using Gemini,
#   combining SERP data, competitor structure, and forum insights.
#
# INPUT:
#   - H1/meta output (R2 + Postgres)
#   - competitor_pages (H2/H3 + FAQs)
#   - paa_questions
#   - search_suggestions (autocomplete + related)
#   - forum_master_insights
#
# PROCESS:
#   - Loads unified article brief
#   - Aggregates SERP + forum intelligence
#   - Builds structured prompt with caps
#   - Generates outline via Gemini
#   - Parses structured sections
#
# OUTPUT:
#   - Markdown outline → R2 (blog/{run_id}/outline.md)
#   - Metadata → Postgres (generated_outputs)
#
# NOTES:
#   - Single-article paradigm (all keywords merged)
#   - Includes gap analysis + SEO tagging
#   - Critical for downstream blog generation
# ─────────────────────────────────────────────────────────────

import json
import os
import re
import time

from psycopg2.extras import Json
from google import genai
from google.genai import types

from app.database import fetch_all, fetch_one, execute
from app.repositories.run_repo import get_run_keywords
from app.storage import r2_get_text, r2_put_text

from stages.cells.cell_23_shared_utils import *
from config import GEMINI_API_KEY, GEMINI_MODEL, SAFETY_OFF

# ── Config ────────────────────────────────────────────────────────────

GEMINI_MODEL      = "gemini-2.5-flash"
MAX_CELL          = 49000

COUNTRY_MAP = {
    "ng": "Nigeria", "ae": "UAE", "gb": "UK", "us": "USA",
    "et": "Ethiopia", "ke": "Kenya", "za": "South Africa",
    "sa": "Saudi Arabia", "pk": "Pakistan", "bd": "Bangladesh",
    "au": "Australia", "ca": "Canada", "sg": "Singapore",
    "om": "Oman", "qa": "Qatar", "bh": "Bahrain", "kw": "Kuwait",
    "in": "India", "iq": "Iraq", "lk": "Sri Lanka", "np": "Nepal",
}

# ── Auth ──────────────────────────────────────────────────────────────
gemini_client = genai.Client(api_key=GEMINI_API_KEY)

SAFETY_OFF = [
    types.SafetySetting(category="HARM_CATEGORY_HARASSMENT",        threshold="OFF"),
    types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH",       threshold="OFF"),
    types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="OFF"),
    types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="OFF"),
    types.SafetySetting(category="HARM_CATEGORY_CIVIC_INTEGRITY",   threshold="OFF"),
]


def parse_outline_sections(raw_text):
    """
    Parse blog outline from Gemini output.
    Handles multiple formats Gemini uses:
      "H2: heading"
      "## H2: heading"
      "### H2: heading"
      "**H2: heading**"
      "## heading" (without "H2:" prefix)
    Returns dict with gap_analysis, outline, linking, word_count
    """
    text = clean_gemini_output(raw_text)

    # First, try to split by ## section headers
    # Look for Content Gap Analysis section
    gap = ""
    outline = ""
    linking = ""
    word_count = ""

    # Flexible section boundary detection
    gap_patterns = [
        r'(?:#{1,3}\s*)?Content\s*Gap\s*Analysis\s*\n(.*?)(?=(?:#{1,3}\s*)?(?:Complete\s*Blog\s*Outline|Blog\s*Outline|H1\s*:)|$)',
        r'(?:Must-Have|Gap\s*Opportunities)(.*?)(?=H1\s*:|H2\s*:|#{1,3}\s*H2)',
    ]
    for pat in gap_patterns:
        m = re.search(pat, text, re.DOTALL | re.IGNORECASE)
        if m and len(m.group(1).strip()) > 50:
            gap = m.group(1).strip()
            break

    # Find outline section — everything from first H2 line to linking/word count
    outline_patterns = [
        r'(H1\s*:.*?)(?=(?:#{1,3}\s*)?Internal\s*Linking|(?:#{1,3}\s*)?Recommended\s*Word|$)',
        r'((?:#{1,3}\s*)?H2\s*:.*?)(?=(?:#{1,3}\s*)?Internal\s*Linking|(?:#{1,3}\s*)?Recommended\s*Word|$)',
    ]
    for pat in outline_patterns:
        m = re.search(pat, text, re.DOTALL | re.IGNORECASE)
        if m and len(m.group(1).strip()) > 100:
            outline = m.group(1).strip()
            break

    # If still no outline, take everything after gap analysis
    if not outline and gap:
        gap_end = text.find(gap) + len(gap)
        remainder = text[gap_end:].strip()
        if len(remainder) > 100:
            outline = remainder

    # If STILL no outline, use the full text (minus obvious non-outline)
    if not outline:
        outline = text

    # Linking
    m = re.search(r'(?:#{1,3}\s*)?Internal\s*Linking.*?\n(.*?)(?=(?:#{1,3}\s*)?Recommended\s*Word|$)', text, re.DOTALL | re.IGNORECASE)
    if m:
        linking = m.group(1).strip()

    # Word count
    m = re.search(r'(?:#{1,3}\s*)?Recommended\s*Word\s*Count.*?\n(.*?)$', text, re.DOTALL | re.IGNORECASE)
    if m:
        word_count = m.group(1).strip()

    return {
        "gap_analysis": gap,
        "outline": outline,
        "linking": linking,
        "word_count": word_count,
    }


# ── Helpers ─────────────────────────────────────────────────────
def _trunc(v):
    s = str(v) if v is not None else ""
    return s[:MAX_CELL] + "\n[TRUNCATED]" if len(s) > MAX_CELL else s

# ── Gemini call handling (token + response fixes)
# ── Prompt size control (caps applied)
def call_gemini(prompt, max_tokens=16000):   # ← FIX 1: was 6000
    for attempt in range(3):
        try:
            resp = gemini_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    max_output_tokens=max_tokens,
                    safety_settings=SAFETY_OFF,
                ),
            )
            if resp.candidates:
                return resp.text.strip()     # ← FIX 2: was resp.candidates[0].content.parts[0].text
        except Exception as e:
            print(f"  ⚠️ Gemini attempt {attempt+1}/3: {e}")
            if attempt < 2:
                time.sleep(6 * (attempt + 1))
    return ""


# ═══════════════════════════════════════════════════════════════
# DATA LOADERS — DB + R2
# ═══════════════════════════════════════════════════════════════

def current_run_id() -> int:
    run_id_raw = os.getenv("RUN_ID")
    if not run_id_raw:
        raise RuntimeError("RUN_ID env var is required")
    return int(run_id_raw)


def extract_best_h1(h1_options_text, recommended_text, fallback_keyword):
    """Extract the best H1 from parsed sections, with multiple fallbacks."""
    if recommended_text:
        match = re.search(r'H1\s*:\s*(.+?)(?:\n|$)', recommended_text)
        if match:
            h1 = match.group(1).strip().strip('"').strip("'").strip("*")
            if len(h1) > 10:
                return h1

    if h1_options_text:
        match = re.search(r'^\s*1[\.\)]\s*(.+?)(?:\s*—|\s*$)', h1_options_text, re.MULTILINE)
        if match:
            h1 = match.group(1).strip().strip('"').strip("'").strip("*")
            if len(h1) > 10:
                return h1

    return fallback_keyword


def _extract_section(text: str, heading: str) -> str:
    pattern = rf"## {re.escape(heading)}\n(.*?)(?=\n## |\Z)"
    match = re.search(pattern, text or "", re.DOTALL | re.IGNORECASE)
    return match.group(1).strip() if match else ""


def load_article_brief(run_id: int):
    """Load article brief from H1/meta output in R2; fallback to run_keywords."""
    output = fetch_one(
        """
        SELECT r2_key, metadata_json
        FROM generated_outputs
        WHERE run_id = %s
          AND output_type = %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (run_id, "h1_meta"),
    )

    if output:
        text = r2_get_text(output["r2_key"])
        metadata = output.get("metadata_json") or {}

        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except Exception:
                metadata = {}

        primary = str(metadata.get("primary_keyword", "")).strip()
        secondary = metadata.get("secondary_keywords", []) or []
        countries = metadata.get("target_countries", []) or []

        h1_options = _extract_section(text, "H1 Options")
        recommended = _extract_section(text, "Recommended Combination")

        chosen_h1 = extract_best_h1(
            h1_options,
            recommended,
            primary,
        )

        return {
            "primary_keyword": primary,
            "secondary_keywords": secondary,
            "all_keywords": [primary] + [k for k in secondary if k],
            "target_countries": ", ".join(countries),
            "chosen_h1": chosen_h1,
        }

    print("  ⚠️ H1/meta output not found in generated_outputs. Falling back to run_keywords...")

    records = get_run_keywords(run_id)
    kws = []
    country_codes = set()

    for r in records:
        kw = str(r.get("keyword", "")).strip()
        cc = str(r.get("country_code", "")).strip().lower()

        if kw and kw not in kws:
            kws.append(kw)
        if cc:
            country_codes.add(cc)

    primary = sorted(kws, key=lambda k: len(k.split()), reverse=True)[0] if kws else ""
    countries = ", ".join(COUNTRY_MAP.get(c, c.upper()) for c in sorted(country_codes))

    return {
        "primary_keyword": primary,
        "secondary_keywords": [k for k in kws if k != primary],
        "all_keywords": kws,
        "target_countries": countries,
        "chosen_h1": primary,
    }


def load_all_competitor_h2s(run_id: int):
    """Load competitor H2/H3 structures from competitor_pages."""
    records = fetch_all(
        """
        SELECT url, h2_data, h3_data
        FROM competitor_pages
        WHERE run_id = %s
          AND status = 'success'
        ORDER BY id ASC
        """,
        (run_id,),
    )

    competitors = []

    for r in records:
        url = str(r.get("url", "") or "").strip()
        h2_raw = str(r.get("h2_data", "") or "").strip()
        h3_raw = str(r.get("h3_data", "") or "").strip()

        if h2_raw and h2_raw != "nan":
            h2s = [
                re.sub(r'^\d+\.\s*', '', line).strip()
                for line in h2_raw.split("\n")
                if line.strip() and len(line.strip()) > 3
            ]
            h3s = [
                re.sub(r'^\d+\.\s*', '', line).strip()
                for line in h3_raw.split("\n")
                if line.strip() and len(line.strip()) > 3
            ]

            competitors.append({
                "url": url,
                "h2s": h2s,
                "h3s": h3s,
            })

    return competitors


def _extract_questions_from_faq_value(faq_value):
    questions = []
    seen = set()

    if not faq_value:
        return questions

    if isinstance(faq_value, str):
        try:
            parsed = json.loads(faq_value)
        except Exception:
            parsed = faq_value
    else:
        parsed = faq_value

    if isinstance(parsed, list):
        for item in parsed:
            if isinstance(item, dict):
                q = str(item.get("question", "") or item.get("Question", "") or "").strip()
                if q and len(q) > 10 and q.lower() not in seen:
                    questions.append(q)
                    seen.add(q.lower())
            elif isinstance(item, str):
                q = item.strip()
                if q and len(q) > 10 and q.lower() not in seen:
                    questions.append(q)
                    seen.add(q.lower())

    elif isinstance(parsed, dict):
        for item in parsed.values():
            if isinstance(item, dict):
                q = str(item.get("question", "") or item.get("Question", "") or "").strip()
            else:
                q = str(item or "").strip()

            if q and len(q) > 10 and q.lower() not in seen:
                questions.append(q)
                seen.add(q.lower())

    elif isinstance(parsed, str):
        for m in re.finditer(r'Q\d+\s*:\s*(.+?)(?:\nA\d+\s*:|$)', parsed, re.DOTALL):
            q = m.group(1).strip()
            if q and len(q) > 10 and q.lower() not in seen:
                questions.append(q)
                seen.add(q.lower())

        for m in re.finditer(r'"question"\s*:\s*"([^"]+)"', parsed, re.IGNORECASE):
            q = m.group(1).strip()
            if q and len(q) > 10 and q.lower() not in seen:
                questions.append(q)
                seen.add(q.lower())

    return questions


def load_competitor_faqs(run_id: int):
    """Load competitor FAQ questions from competitor_pages.faqs_json."""
    records = fetch_all(
        """
        SELECT faqs_json
        FROM competitor_pages
        WHERE run_id = %s
          AND status = 'success'
        ORDER BY id ASC
        """,
        (run_id,),
    )

    seen = set()
    questions = []

    for r in records:
        for q in _extract_questions_from_faq_value(r.get("faqs_json")):
            if q.lower() not in seen:
                questions.append(q)
                seen.add(q.lower())

    return questions


def load_paa_questions(run_id: int):
    rows = fetch_all(
        """
        SELECT question
        FROM paa_questions
        WHERE run_id = %s
        ORDER BY position ASC
        """,
        (run_id,),
    )

    seen = set()
    out = []

    for r in rows:
        q = str(r.get("question", "") or "").strip()
        if q and q.lower() not in seen:
            out.append(q)
            seen.add(q.lower())

    return out


def load_autocomplete_suggestions(run_id: int):
    rows = fetch_all(
        """
        SELECT suggestion
        FROM search_suggestions
        WHERE run_id = %s
          AND source = 'autocomplete'
        ORDER BY position ASC
        """,
        (run_id,),
    )

    seen = set()
    out = []

    for r in rows:
        s = str(r.get("suggestion", "") or "").strip()
        if s and len(s) > 3 and s.lower() not in seen:
            out.append(s)
            seen.add(s.lower())

    return out


def load_related_searches(run_id: int):
    rows = fetch_all(
        """
        SELECT suggestion
        FROM search_suggestions
        WHERE run_id = %s
          AND source = 'related'
        ORDER BY position ASC
        """,
        (run_id,),
    )

    seen = set()
    out = []

    for r in rows:
        s = str(r.get("suggestion", "") or "").strip()
        if s and s.lower() not in seen:
            out.append(s)
            seen.add(s.lower())

    return out


def load_forum_insights_for_outline(run_id: int):
    """
    Load Content_Gap, Objection, Patient_Question rows from forum_master_insights.
    These are injected into the outline prompt as high-priority H2 candidates.
    """
    records = fetch_all(
        """
        SELECT insight_type, priority_score, clean_insight, insight_text, detected_country
        FROM forum_master_insights
        WHERE run_id = %s
          AND insight_type IN ('Content_Gap', 'Objection', 'Patient_Question')
        ORDER BY priority_score DESC
        """,
        (run_id,),
    )

    grouped = {
        "Content_Gap": [],
        "Objection": [],
        "Patient_Question": [],
    }

    for r in records:
        cat = str(r.get("insight_type", "") or "").strip()
        score = int(r.get("priority_score") or 1)
        insight = str(r.get("clean_insight", "") or r.get("insight_text", "") or "").strip()
        country = str(r.get("detected_country", "Global") or "Global").strip()

        if cat in grouped and insight:
            grouped[cat].append((score, insight[:120], country))

    if not any(grouped.values()):
        return ""

    lines = ["\nFORUM MASTER INSIGHTS (for outline):"]

    if grouped["Content_Gap"]:
        lines.append("\nCONTENT GAPS (topics NO competitor covers — highest SEO priority):")
        for score, insight, country in grouped["Content_Gap"][:10]:
            lines.append(f"  [P{score}|{country}] {insight}")

    if grouped["Objection"]:
        lines.append("\nPATIENT OBJECTIONS (each needs a dedicated H2 section):")
        for score, insight, country in grouped["Objection"][:8]:
            lines.append(f"  [P{score}|{country}] {insight}")

    if grouped["Patient_Question"]:
        lines.append("\nPATIENT QUESTIONS (validate H2/H3 headings + FAQ candidates):")
        for score, insight, country in grouped["Patient_Question"][:12]:
            lines.append(f"  [P{score}|{country}] {insight}")

    return "\n".join(lines)


def save_blog_outline_output(run_id: int, output_text: str, metadata: dict) -> str:
    r2_key = f"blog/{run_id}/outline.md"

    r2_put_text(r2_key, output_text)

    execute(
        """
        DELETE FROM generated_outputs
        WHERE run_id = %s
          AND output_type = %s
        """,
        (run_id, "blog_outline"),
    )

    execute(
        """
        INSERT INTO generated_outputs (
            run_id, output_type, r2_key, metadata_json
        )
        VALUES (%s, %s, %s, %s)
        """,
        (
            run_id,
            "blog_outline",
            r2_key,
            Json(metadata),
        ),
    )

    return r2_key


# ═══════════════════════════════════════════════════════════════
# PROMPT BUILDER
# ═══════════════════════════════════════════════════════════════

def build_outline_prompt(brief, competitor_h2s, paa_all, autocomplete_all, related_all, comp_faqs=None, forum_insights=""):
    """Build ONE outline prompt for the single article, using ALL merged data."""

    primary   = brief["primary_keyword"]
    secondary = brief["secondary_keywords"]
    countries = brief["target_countries"]
    chosen_h1 = brief["chosen_h1"]

    COMP_CAP = len(competitor_h2s)
    comp_block = ""
    print(f"  Competitors: {len(competitor_h2s)} (capped at {COMP_CAP})")
    if competitor_h2s:
        comp_block = "COMPETITOR H2 STRUCTURES:\n"
        for i, comp in enumerate(competitor_h2s[:COMP_CAP], 1):
            comp_block += f"\nURL {i}: {comp['url'][:70]}\n"
            for h2 in comp["h2s"][:25]:
                comp_block += f"  - H2: {h2}\n"
            for h3 in comp["h3s"][:25]:
                comp_block += f"    - H3: {h3}\n"
        if len(competitor_h2s) > COMP_CAP:
            comp_block += f"\n(Plus {len(competitor_h2s) - COMP_CAP} more competitor URLs — themes covered above)\n"

    # FIX #11: Cap data to prevent prompt bloat (was sending ALL 200+ items)
    PAA_CAP = 25
    paa_block = ""
    print(f"  PAA questions: {len(paa_all)} (capped at {PAA_CAP})")
    if paa_all:
        paa_block = "PAA QUESTIONS (merged across all keyword variations):\n"
        for i, q in enumerate(paa_all[:PAA_CAP], 1):
            paa_block += f"  {i}. {q}\n"
        if len(paa_all) > PAA_CAP:
            paa_block += f"  (Plus {len(paa_all) - PAA_CAP} more PAA questions on similar themes)\n"

    AC_CAP = 60
    ac_block = ""
    print(f"  Autocomplete: {len(autocomplete_all)} (capped at {AC_CAP})")
    if autocomplete_all:
        ac_block = "GOOGLE AUTOCOMPLETE SUGGESTIONS (merged):\n"
        for i, s in enumerate(autocomplete_all[:AC_CAP], 1):
            ac_block += f"  {i}. {s}\n"
        if len(autocomplete_all) > AC_CAP:
            ac_block += f"  (Plus {len(autocomplete_all) - AC_CAP} more suggestions)\n"

    RS_CAP = 40
    rs_block = ""
    print(f"  Related searches: {len(related_all)} (capped at {RS_CAP})")
    if related_all:
        rs_block = "GOOGLE RELATED SEARCHES (merged):\n"
        for i, s in enumerate(related_all[:RS_CAP], 1):
            rs_block += f"  {i}. {s}\n"
        if len(related_all) > RS_CAP:
            rs_block += f"  (Plus {len(related_all) - RS_CAP} more related searches)\n"

    comp_faq_block = ""
    if comp_faqs:
        comp_faq_block = "COMPETITOR FAQ QUESTIONS (from top-ranking pages — consider for YOUR FAQ section):\n"
        for i, q in enumerate(comp_faqs[:80], 1):
            comp_faq_block += f"  {i}. {q}\n"

    forum_insights_block = forum_insights if forum_insights else "(Run Cells A+B to generate Forum_Master_Insights — content gaps and objections will appear here)"

    return f"""Create a blog outline for Divinheal (Indian medical tourism company).

ARTICLE BRIEF:
H1: {chosen_h1}
Primary keyword: {primary}
Secondary keywords: {", ".join(secondary) if secondary else "(none)"}
Target countries: {countries}

FORUM INTELLIGENCE (high-priority — address all content gaps as H2 sections):
{forum_insights_block}

SERP DATA:
{paa_block}
{ac_block}
{rs_block}
{comp_faq_block}
{comp_block}

OUTLINE RULES (follow in order):
1. Follow Patient Decision Journey: Understanding → Cost → Safety/Success → Hospital Selection → Country Comparison → Logistics → Recovery → Divinheal Value → FAQ → Conclusion.
2. Multi-country: Include India vs {countries} cost comparison H2. Include country-specific logistics H3s. ONE unified outline, not per-country.
3. Every keyword variation ({", ".join(brief["all_keywords"])}) must be covered by at least one H2/H3.
4. Tag each H2 with SOURCE: [PAA] / [AC] / [RS] / [COMP] / [COMP_FAQ] / [GAP].
5. Tag each H2 with CONTENT NEEDS: [TABLE] / [CTA] / [STORY] / [IMAGE] / [SCHEMA] / [MANUAL DATA].
6. Include at least 3 H2s tagged [GAP] — topics NO competitor covers.
7. 10-15 H2s total (including FAQ and Conclusion). Annotate each with ~word count.
8. FAQ: 10-13 questions. Prioritize commercial intent (cost, comparison, process).
   DEDUP: PAA, autocomplete, related searches, and competitor FAQs may contain overlapping questions.
   If two questions ask the same thing in different words, keep the version with stronger commercial intent.
   Example: keep "How much does [treatment] cost in India?" over "What is the cost of [treatment]?"
9. Gap Analysis: BRIEF (under 300 words). Outline is the priority.
10. Do NOT include "Internal Linking" or "Word Count" as blog H2s — those go after the outline as metadata.
11. Heading depth: Use H2 and H3 ONLY. Do NOT create H4s under any circumstance.If a sub-point feels like it needs an H4, either fold it into the H3 body text
    or elevate it to its own H3 if it warrants standalone coverage.
    
WORD TARGETS:
- Major sections (Cost, Safety, Hospital): ~400-500 words
- Minor sections (Recovery, Logistics): ~200-300 words
- FAQ: ~350 words | Conclusion: ~130 words | Total: 3000-4500 words

OUTPUT FORMAT (use plain text, not markdown ## or **):

Content Gap Analysis:
- Must-Have (in 3+ competitors): [list]
- Unique to 1 competitor: [list]
- Gap Opportunities (in none): [list]

Blog Outline:
HEADING DEPTH CONSTRAINT: Maximum 2 levels — H2 and H3 only. No H4s anywhere in the outline.

H1: {chosen_h1}

H2: [heading] — Sources: [tags] — Tags: [tags] — ~[X] words
  H3: [sub-heading]
  H3: [sub-heading]

H2: Frequently Asked Questions — Tags: [SCHEMA] — ~350 words
  Q1: [question] — Source: [PAA/AC/RS/COMP_FAQ]
  Q2: [question] — Source: [PAA/AC/RS/COMP_FAQ]

H2: Conclusion — Tags: [CTA] — ~130 words

Internal Linking Opportunities:
- [3-5 Divinheal pages to link to]

Recommended Word Count: ~[X] words"""


# ═══════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════

print("\n" + "="*65)
print("🗂️  H2/H3 STRATEGIC OUTLINE + CONTENT GAP ANALYSIS")
print("   Mode: SINGLE ARTICLE — all keywords/data merged")
print("="*65)

run_id = current_run_id()

print("\n📋 Loading article brief...")
brief = load_article_brief(run_id)

if not brief or not brief["primary_keyword"]:
    print("❌ No article brief found. Run the H1/Meta cell first.")
else:
    print(f"  📌 Primary: {brief['primary_keyword']}")
    print(f"  📌 Secondary: {brief['secondary_keywords']}")
    print(f"  🌍 Countries: {brief['target_countries']}")
    print(f"  🏷️ Chosen H1: {brief['chosen_h1'][:70]}")

    print("\n📊 Loading ALL supporting data from DB/R2...")
    competitor_h2s = load_all_competitor_h2s(run_id)
    paa_all = load_paa_questions(run_id)
    ac_all = load_autocomplete_suggestions(run_id)
    rs_all = load_related_searches(run_id)
    comp_faqs = load_competitor_faqs(run_id)

    print(
        f"  Competitors: {len(competitor_h2s)} | "
        f"PAA: {len(paa_all)} | "
        f"AC: {len(ac_all)} | "
        f"RS: {len(rs_all)} | "
        f"Comp FAQs: {len(comp_faqs)}"
    )

    print("\n📊 Loading Forum_Master_Insights for outline...")
    forum_insights = load_forum_insights_for_outline(run_id)
    print(f"  Forum insights: {'✅ loaded' if forum_insights else '⚠️ not available'}")

    print("\n🤖 Generating unified outline...")
    prompt = build_outline_prompt(
        brief,
        competitor_h2s,
        paa_all,
        ac_all,
        rs_all,
        comp_faqs,
        forum_insights,
    )
    result = call_gemini(prompt, max_tokens=16000)

    if not result:
        print("  ⚠️ Empty response from Gemini")
    else:
        parsed = parse_outline_sections(result)
        gap_analysis = parsed["gap_analysis"]
        outline_text = parsed["outline"]
        linking = parsed["linking"]
        word_count = parsed["word_count"]

        full_outline = outline_text
        if linking:
            full_outline += f"\n\n## Internal Linking Opportunities\n{linking}"

        output_text = f"""# Blog Outline

Primary Keyword: {brief["primary_keyword"]}
Secondary Keywords: {", ".join(brief["secondary_keywords"])}
Target Countries: {brief["target_countries"]}
Chosen H1: {brief["chosen_h1"]}

## Content Gap Analysis

{gap_analysis}

## Complete Outline

{full_outline}

## Word Count Recommendation

{word_count}
"""

        metadata = {
            "primary_keyword": brief["primary_keyword"],
            "secondary_keywords": brief["secondary_keywords"],
            "target_countries": brief["target_countries"],
            "chosen_h1": brief["chosen_h1"],
            "word_count": word_count,
        }

        r2_key = save_blog_outline_output(
            run_id=run_id,
            output_text=output_text,
            metadata=metadata,
        )

        h2_count = len(re.findall(r'^H2:', full_outline, re.MULTILINE))
        faq_count = len(re.findall(r'^\s+Q\d+:', full_outline, re.MULTILINE))
        gap_count = full_outline.count("[GAP]")

        print(f"\n  ✅ Outline: {h2_count} H2s | {faq_count} FAQ questions | {gap_count} gap sections")
        print(f"  ✅ Word count: {word_count[:60] if word_count else '⚠️ empty'}")
        print(f"\n{'='*65}")
        print("✅ OUTLINE COMPLETE — single article")
        print(f"   R2 output: {r2_key}")
        print(f"{'='*65}")