# ─────────────────────────────────────────────────────────────
# SHARED UTILS (GEMINI OUTPUT PARSING)
# ─────────────────────────────────────────────────────────────
# PURPOSE:
#   Provides reusable parsing and cleaning utilities for handling
#   inconsistent Gemini outputs across pipeline stages.
#
# INPUT:
#   - Raw Gemini responses (text / JSON / markdown-like output)
#
# PROCESS:
#   - Cleans markdown artifacts (bold, headers, code blocks)
#   - Parses structured sections (H1/meta, outline, FAQs)
#   - Extracts headings, questions, and hooks
#   - Provides fallback-safe parsing logic
#
# OUTPUT:
#   - Structured Python dictionaries / lists
#   - Cleaned text ready for downstream processing
#
# NOTES:
#   - Designed to handle inconsistent Gemini formatting
#   - Used across Stage 3 and later pipeline steps
#   - No external dependencies (pure utility layer)
# ─────────────────────────────────────────────────────────────

import re

# ═══════════════════════════════════════════════════════════════
# Clean Gemini output before parsing
# ═══════════════════════════════════════════════════════════════

def clean_gemini_output(text):
    """
    Strip markdown decorations that Gemini adds inconsistently.
    This MUST run BEFORE any regex splitting/parsing.

    Gemini often:
    - Wraps headers in **bold**: **INTENT:** or **H1 OPTIONS:**
    - Uses ## markdown headers: ## META TITLE OPTIONS
    - Adds backtick code blocks around JSON
    - Adds extra newlines or trailing whitespace
    """
    if not text:
        return ""

    # Remove ```markdown or ```json wrappers
    text = re.sub(r'^```\w*\n?', '', text, flags=re.MULTILINE)
    text = re.sub(r'^```\s*$', '', text, flags=re.MULTILINE)

    # Remove **bold** around header keywords (but keep the text)
    # e.g., **INTENT:** → INTENT:
    # e.g., **H1 OPTIONS:** → H1 OPTIONS:
    text = re.sub(r'\*\*([A-Z][A-Z\s/&]+(?::|OPTIONS|COMBINATION|ANALYSIS|OUTLINE|MAP).*?)\*\*', r'\1', text)

    # Remove leading ## or ### from section header lines
    # e.g., "## H1 OPTIONS:" → "H1 OPTIONS:"
    # e.g., "### META TITLE OPTIONS:" → "META TITLE OPTIONS:"
    # BUT: preserve ## inside outline content (like "## H2 heading" in blog outline)
    text = re.sub(r'^#{1,4}\s*(INTENT|H1 OPTIONS|META TITLE|META DESCRIPTION|RECOMMENDED|CONTENT GAP|COMPLETE BLOG|INTERNAL LINKING|RECOMMENDED WORD)',
                  r'\1', text, flags=re.MULTILINE | re.IGNORECASE)

    # Normalize multiple blank lines to single
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text.strip()


# ═══════════════════════════════════════════════════════════════
# Flexible section splitter for H1/Meta output
# ═══════════════════════════════════════════════════════════════

def parse_h1_meta_sections(raw_text):
    """
    Parse H1/Meta Gemini output into sections, handling multiple formats.
    Returns dict with keys: intent, h1_options, meta_titles, meta_descriptions, recommended
    """
    text = clean_gemini_output(raw_text)

    result = {
        "intent": "",
        "h1_options": "",
        "meta_titles": "",
        "meta_descriptions": "",
        "recommended": "",
    }

    # Strategy: find each section by flexible header pattern, capture until next section
    patterns = [
        ("intent",           r'(?:^|\n)\s*INTENT\s*:\s*(.+?)(?=\n\s*H1\s*OPTIONS|\Z)', re.DOTALL | re.IGNORECASE),
        ("h1_options",       r'(?:^|\n)\s*H1\s*OPTIONS\s*:?\s*\n(.*?)(?=\n\s*META\s*TITLE|\Z)', re.DOTALL | re.IGNORECASE),
        ("meta_titles",      r'(?:^|\n)\s*META\s*TITLE\s*OPTIONS?\s*:?\s*\n(.*?)(?=\n\s*META\s*DESC|\Z)', re.DOTALL | re.IGNORECASE),
        ("meta_descriptions",r'(?:^|\n)\s*META\s*DESC\w*\s*OPTIONS?\s*:?\s*\n(.*?)(?=\n\s*RECOMMEND|\Z)', re.DOTALL | re.IGNORECASE),
        ("recommended",      r'(?:^|\n)\s*RECOMMEND\w*\s*COMBINATION\s*:?\s*\n(.*?)$', re.DOTALL | re.IGNORECASE),
    ]

    for key, pattern, flags in patterns:
        match = re.search(pattern, text, flags)
        if match:
            result[key] = match.group(1).strip()

    # Intent often has trailing newlines or is on same line
    if result["intent"]:
        result["intent"] = result["intent"].split("\n")[0].strip()

    return result


def extract_best_h1(h1_options_text, recommended_text, fallback_keyword):
    """
    Extract the best H1 from parsed sections, with multiple fallbacks.
    Priority: recommended → first H1 option → keyword
    """
    # Try recommended section
    if recommended_text:
        match = re.search(r'H1\s*:\s*(.+?)(?:\n|$)', recommended_text)
        if match:
            h1 = match.group(1).strip().strip('"').strip("'").strip("*")
            if len(h1) > 10:
                return h1

    # Try first H1 from options
    if h1_options_text:
        # Match: "1. Egg Freezing Cost..." or "1) Egg Freezing Cost..."
        match = re.search(r'^\s*1[\.\)]\s*(.+?)(?:\s*—|\s*$)', h1_options_text, re.MULTILINE)
        if match:
            h1 = match.group(1).strip().strip('"').strip("'").strip("*")
            if len(h1) > 10:
                return h1

    return fallback_keyword


# ═══════════════════════════════════════════════════════════════
# Flexible outline parser
# ═══════════════════════════════════════════════════════════════

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


def extract_h2_sections_from_outline(outline_text):
    """
    Parse H2/H3 sections from outline text, handling Gemini's varied formats.
    Supports: "H2: heading", "### H2: heading", "## heading", "**H2: heading**"
    """
    sections = []
    current = None

    for line in outline_text.split("\n"):
        line_stripped = line.strip()
        if not line_stripped:
            continue

        # Match H2 — multiple patterns
        h2 = None
        tags = ""
        intent = ""
        section_kws = ""

        # Pattern 1: "H2: heading — Sources: [...] — Tags: [...]"
        m = re.match(r'^(?:#{1,4}\s*)?H2\s*:\s*(.+?)(?:\s*—\s*Sources?:\s*(.+?))?(?:\s*—\s*Tags?:\s*(.+?))?(?:\s*—\s*Intent:\s*(.+?))?(?:\s*—\s*Keywords?:\s*(.+?))?$', line_stripped, re.IGNORECASE)
        if m:
            h2 = m.group(1).strip().strip("*")
            tags = (m.group(3) or "").strip()
            intent = (m.group(4) or "").strip()
            section_kws = (m.group(5) or "").strip()

        # Pattern 2: "## heading — Sources: [...]" (no H2: prefix)
        if not h2:
            m = re.match(r'^#{2,3}\s+(.+?)(?:\s*—\s*Sources?:\s*(.+?))?(?:\s*—\s*Tags?:\s*(.+?))?$', line_stripped)
            if m and not line_stripped.startswith("# "):  # Not H1
                h2 = m.group(1).strip().strip("*")
                tags = (m.group(3) or "").strip()

        if h2:
            if current:
                sections.append(current)
            current = {"h2": h2, "h3s": [], "tags": tags, "intent": intent, "section_keywords": section_kws}
            continue

        # Match H3
        h3 = None
        m = re.match(r'^(?:#{1,4}\s*)?(?:H3\s*:\s*)?(?:\s*-\s+)?(.+)', line_stripped)
        if current and m:
            candidate = m.group(1).strip().strip("*")
            # Must look like a sub-heading (not just any text)
            if (line_stripped.startswith("H3:") or line_stripped.startswith("###") or
                line_stripped.startswith("  -") or line_stripped.startswith("  H3") or
                line_stripped.startswith("- H3")):
                h3 = re.sub(r'^H3\s*:\s*', '', candidate).strip()
                current["h3s"].append(h3)
                continue

        # Match FAQ questions
        m = re.match(r'^\s*Q\d+\s*:\s*(.+?)(?:\s*—|$)', line_stripped)
        if m and current and ("FAQ" in current["h2"] or "Frequently" in current["h2"]):
            current["h3s"].append(m.group(1).strip())

    if current:
        sections.append(current)

    return sections


def extract_faq_questions_from_outline(outline_text, paa_fallback=None):
    """
    Extract FAQ questions from outline. Falls back to PAA if none found.
    """
    questions = []

    # Try Q1: pattern
    for m in re.finditer(r'Q\d+\s*:\s*(.+?)(?:\s*—|\s*$)', outline_text, re.MULTILINE):
        q = m.group(1).strip()
        if q and len(q) > 10:
            questions.append(q)

    # Try "- question?" pattern inside FAQ section
    if not questions:
        faq_section = re.search(r'(?:FAQ|Frequently\s*Asked).*?\n(.*?)(?=H2\s*:|##\s*Conclusion|##\s*Final|$)', outline_text, re.DOTALL | re.IGNORECASE)
        if faq_section:
            for m in re.finditer(r'(?:^|\n)\s*[-•]\s*(.+\?)', faq_section.group(1)):
                q = m.group(1).strip()
                if len(q) > 10:
                    questions.append(q)

    # Try ### heading format inside FAQ section
    if not questions:
        for m in re.finditer(r'###\s*(.+\?)', outline_text):
            questions.append(m.group(1).strip())

    # FALLBACK: Use PAA questions if outline has none
    if not questions and paa_fallback:
        questions = paa_fallback[:10]

    return questions


def extract_empathy_hook(hooks_text, section_type):
    """
    Extract empathy hook by section type, handling Gemini's varied heading formats.
    """
    if not hooks_text:
        return ""

    # Map section types to flexible patterns
    hook_patterns = {
        "intro":     [r'(?:###?\s*)?Introduction\s*Hook\s*\n(.*?)(?=(?:###?\s*)|---|\Z)'],
        "cost":      [r'(?:###?\s*)?Cost\s*(?:Section\s*)?Hook\s*\n(.*?)(?=(?:###?\s*)|---|\Z)'],
        "safety":    [r'(?:###?\s*)?Safety\s*(?:Section\s*)?Hook\s*\n(.*?)(?=(?:###?\s*)|---|\Z)'],
        "logistics": [r'(?:###?\s*)?Logistics\s*(?:Section\s*)?Hook\s*\n(.*?)(?=(?:###?\s*)|---|\Z)'],
        "cta":       [r'(?:###?\s*)?CTA[/\s]*(?:Conclusion\s*)?Hook\s*\n(.*?)(?=(?:###?\s*)|---|\Z)'],
    }

    for pattern in hook_patterns.get(section_type, []):
        match = re.search(pattern, hooks_text, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return ""


# ═══════════════════════════════════════════════════════════════
# Reddit relevance filter
# ═══════════════════════════════════════════════════════════════

# ── Relevance filter — strengthened: 3 keywords + must contain 1 medical term ─
EXCLUDED_SUBREDDITS = {
    "eatcheapandhealthy", "cooking", "food", "recipes", "mealprep",
    "coolguides", "askreddit", "tifu", "gaming", "memes", "funny",
    "aww", "pics", "todayilearned", "lifeprotips", "showerthoughts",
    "music", "eu4", "wallstreetbets", "cryptocurrency", "stocks",
    "sports", "nfl", "nba", "soccer", "movies", "television",
}

RELEVANCE_KEYWORDS = {
    "cost", "treatment", "medical tourism", "hospital", "doctor",
    "india", "medical", "patient", "procedure", "surgery",
    "clinic", "specialist", "diagnosis", "therapy", "affordable",
    "insurance", "recovery", "consultation", "accredited", "quality",
    "healthcare", "experience", "side effect", "risk", "success rate",
    "price", "expense", "savings", "abroad", "overseas",
}

# At least one of these MUST be present in the post
MUST_CONTAIN_MEDICAL = {
    "treatment", "medical","hospital", "surgery", "health", "doctor", "patient",
    "diagnosis", "therapy", "clinic",
    "transplant", "cost", "india", "affordable", "insurance",
}

def is_relevant_post(post, topic_keywords=None):
    subreddit = post.get("subreddit", "").lower()
    if subreddit in EXCLUDED_SUBREDDITS:
        return False
    text = f"{post.get('title', '')} {post.get('selftext', '')}".lower()
    # Must contain at least 1 medical/health term
    has_medical = any(term in text for term in MUST_CONTAIN_MEDICAL)
    if not has_medical:
        return False
    # Must match at least 3 relevance keywords (was 2 — too weak)
    check_words = RELEVANCE_KEYWORDS
    if topic_keywords:
        check_words = check_words | set(w.lower() for w in topic_keywords)
    return sum(1 for kw in check_words if kw in text) >= 3
    
