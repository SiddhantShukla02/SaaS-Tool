# ─── NOTE: This cell now imports keys from config.py ────────
# If you haven't set up config.py yet, see README_REVISION.md
try:
    from config import (SERP_API_KEY, GEMINI_API_KEY,
                         FIRECRAWL_API_KEY, BRAVE_API_KEY,
                         CREDS_FILE, SPREADSHEET_NAME,
                         GEMINI_MODEL, COUNTRY_MAP, SAFETY_OFF, SCOPES)
except ImportError:
    print('⚠️ config.py not found — falling back to globals from Cell 1')
# ────────────────────────────────────────────────────────────


from stages.cells.cell_23_shared_utils import *

# ══════════════════════════════════════════════════════════════════════
# LAYER B: POST-SECTION CLEANING
# ══════════════════════════════════════════════════════════════════════

def clean_section_text(text):
    """
    Strip formatting violations Gemini adds despite being told not to:
      - `backtick` keyword wrappers
      - **bold** mid-paragraph keyword wrappers (keeps ## and ### headings intact)
    """
    if not text:
        return text

    # Remove `backtick` wrappers around keywords (keep content)
    text = re.sub(r'`([^`\n]+)`', r'\1', text)

    # Remove **bold** wrappers ONLY inside paragraphs (not on heading lines)
    # Heading lines start with ## or ### — leave those alone
    lines = text.split('\n')
    cleaned = []
    for line in lines:
        # Convert #### (H4) to ### (H3) — H4 is not valid blog structure
        if re.match(r'^####\s', line):
            line = re.sub(r'^####', '###', line)
        if re.match(r'^#{1,3}\s', line):
            cleaned.append(line)  # Heading — don't touch
        else:
            # Strip **bold** from non-heading lines
            line = re.sub(r'\*\*([^*\n]+)\*\*', r'\1', line)
            # Strip *italic* from non-heading lines
            line = re.sub(r'\*([^*\n]+)\*', r'\1', line)
            cleaned.append(line)
    result = '\n'.join(cleaned)

    # FIX E: Strip debug artifacts and pipeline metadata
    # These leak from forum insights tags and hook labels into Gemini output
    result = re.sub(r'\(Emo\)', '', result)
    result = re.sub(r'\(TOFU\)|\(MOFU\)|\(BOFU\)', '', result)
    result = re.sub(r'^.*Hook:\s*$', '', result, flags=re.MULTILINE)
    result = re.sub(r'\[P\d+\|[A-Z]+\]', '', result)  # Priority/country tags

    # Strip banned phrases that Gemini still uses
    banned_replacements = {
        'comprehensive': 'full',
        'world-class': 'internationally recognized',
        'cutting-edge': 'advanced',
        'state-of-the-art': 'modern',
        'has emerged as': 'is now',
        'leading destination': 'popular choice',
        'beacon of hope': 'source of hope',
        'embark on': 'start',
        'it is worth noting': '',
        'in today\'s world': 'today',
        'delve into': 'look at',
        'furthermore': '',
        'moreover': '',
        'additionally': 'also',
        'it\'s important to note that': '',
        'it\'s worth mentioning that': '',
        'undeniable leader': 'strong option',
        'unparalleled': 'excellent',
        'plethora': 'range',
        'myriad': 'range',
        'testament to': 'reflects',
        'pivotal': 'important',
        'navigate': 'plan',
        'game-changer': 'advantage',
        'cornerstone': 'foundation',
        'ensuring': 'so that',
    }
    for phrase, replacement in banned_replacements.items():
        result = re.sub(re.escape(phrase), replacement, result, flags=re.IGNORECASE)

    # Collapse multiple blank lines
    result = re.sub(r'\n{3,}', '\n\n', result)

    return result


def make_unique_hook(hook_text, section_h2, section_idx):
    """
    Prevent every section from starting with the same empathy hook opener.

    Strategy: Replace just the FIRST SENTENCE with a section-specific alternative
    derived entirely from the H2 heading text — no treatment-specific hardcoding.
    The emotional insight of sentences 2-3 from the original hook is preserved.

    Works for any treatment: IVF, cardiac surgery, knee replacement, hair transplant,
    cancer treatment, bariatric surgery, dental work, or anything else.
    """
    if not hook_text:
        return ""

    sentences = re.split(r'(?<=[.!?])\s+', hook_text.strip())
    if len(sentences) <= 1:
        return hook_text

    h2_lower = section_h2.lower()

    # ── Cost / financial sections ──────────────────────────────────────
    if any(w in h2_lower for w in ["cost", "price", "afford", "saving", "fee", "budget", "expense", "cheap", "payment"]):
        opener = "The cost question is usually the first one — and it deserves a straight answer."

    # ── Safety / quality / accreditation sections ──────────────────────
    elif any(w in h2_lower for w in ["safe", "risk", "quality", "accredit", "standard", "certif", "nabh", "jci", "nabl"]):
        opener = "Safety is non-negotiable, and it should be your first filter when choosing where to be treated."

    # ── Hospital / clinic / doctor selection ───────────────────────────
    elif any(w in h2_lower for w in ["hospital", "clinic", "surgeon", "doctor", "specialist", "find", "choose", "best", "select", "top"]):
        opener = "The facility and team you choose shapes everything that follows — the outcome, the experience, and your confidence throughout."

    # ── Recovery / post-treatment / aftercare ─────────────────────────
    elif any(w in h2_lower for w in ["recovery", "heal", "pain", "after", "post", "rehab", "result", "outcome", "discharge", "follow"]):
        opener = "What happens after the procedure matters as much as the procedure itself."

    # ── Travel / logistics / visa / planning ──────────────────────────
    elif any(w in h2_lower for w in ["travel", "journey", "trip", "visa", "logistics", "plan", "tourism", "flight", "accommodation", "stay"]):
        opener = "Travelling abroad for treatment raises practical questions — and having clear answers before you book removes most of the stress."

    # ── Country comparison / India vs elsewhere ────────────────────────
    elif any(w in h2_lower for w in ["india", "uk", "uae", "compar", "versus", " vs ", "value", "abroad", "overseas", "international"]):
        opener = "The cost and quality gap between India and other countries is real — and one does not come at the expense of the other."

    # ── Understanding the treatment / what is / types ─────────────────
    elif any(w in h2_lower for w in ["what is", "understand", "type", "kind", "about", "overview", "introduc", "explain", "definition"]):
        opener = "Before thinking about pricing or logistics, it helps to understand exactly what this treatment involves."

    # ── Long-term / permanence / durability ───────────────────────────
    elif any(w in h2_lower for w in ["long", "permanent", "last", "durable", "future", "year", "lifespan", "duration"]):
        opener = "The long-term picture matters as much as the immediate result — here is what patients should realistically expect."

    # ── Emotional / psychological / confidence / life impact ──────────
    elif any(w in h2_lower for w in ["emotion", "psycholog", "confidence", "mental", "wellbeing", "life", "impact", "change", "transform", "worth"]):
        opener = "The effect this treatment has on how patients feel day to day is often the part that is hardest to put into words."

    # ── Success rates / statistics / data ─────────────────────────────
    elif any(w in h2_lower for w in ["success", "rate", "statistic", "data", "percent", "outcome", "result", "survival", "efficacy"]):
        opener = "Numbers only tell part of the story — but they are still the most honest place to start when evaluating any treatment."

    # ── Process / procedure / steps / how it works ────────────────────
    elif any(w in h2_lower for w in ["process", "procedure", "step", "how", "work", "method", "technique", "approach", "protocol"]):
        opener = "Knowing what actually happens during the procedure — step by step — gives patients far more confidence going in."

    # ── Divinheal / platform / how we help ────────────────────────────
    elif any(w in h2_lower for w in ["divinheal", "platform", "help", "support", "service", "assist", "coordinat", "facilit"]):
        opener = "Having the right partner on the ground changes the entire experience of seeking treatment abroad."

    # ── FAQ / frequently asked ────────────────────────────────────────
    elif any(w in h2_lower for w in ["faq", "frequent", "common question", "ask", "answer"]):
        opener = "These are the questions that come up in almost every patient conversation — answered as directly as possible."

    # ── Conclusion / next steps / get started ─────────────────────────
    elif any(w in h2_lower for w in ["conclusion", "next step", "start", "begin", "ready", "contact", "book", "consult", "get"]):
        opener = "At this point, most patients have the information they need — the next step is simply taking it."

    # ── Generic fallback — uses section_idx for variety, no procedure names ──
    else:
        openers = [
            "There is one question patients ask before any other, and this section answers it directly.",
            "This is where most patients get stuck — and where clear information makes the biggest difference.",
            "The details in this section shape every decision that follows, so it is worth understanding them carefully.",
            "Getting this right early saves time, stress, and unnecessary cost later in the process.",
            "Most patients reach this question only after doing a lot of research — here is a straight answer.",
        ]
        opener = openers[section_idx % len(openers)]

    # Preserve sentences 2+ from the original hook (the real emotional content)
    tail = ' '.join(sentences[1:]) if len(sentences) > 1 else ""
    return f"{opener} {tail}".strip()


def detect_repeated_opener(new_text, seen_openers, threshold=50):
    """
    Check if the first `threshold` chars of new_text match any seen opener.
    Returns True if it's a duplicate.
    """
    if not new_text:
        return False
    first_chars = new_text.strip()[:threshold].lower()
    for seen in seen_openers:
        if seen[:threshold].lower() == first_chars:
            return True
    return False


# ══════════════════════════════════════════════════════════════════════
# LAYER C: FINAL QA REVIEW PASS
# ══════════════════════════════════════════════════════════════════════

def run_final_review_pass(full_blog, plan):
    """
    TWO-STAGE final review:
      Stage 1: Python-based mechanical fixes (fast, deterministic)
      Stage 2: Gemini editorial review (coherency, flow, human voice)
    """
    word_count = count_words(full_blog)

    # ══════════════════════════════════════════════════════════════
    # STAGE 1: Python mechanical fixes (no Gemini call needed)
    # ══════════════════════════════════════════════════════════════

    # 1a. Run clean_section_text on the FULL blog (catches FAQ/conclusion too)
    full_blog = clean_section_text(full_blog)

    # 1b. Cap repeated conversational asides (e.g., "Honestly," ×7)
    ASIDE_PATTERNS = [
        (r'(?i)\bhonestly,', 'Honestly,'),
        (r'(?i)\bthe truth is,', 'The truth is,'),
        (r"(?i)\bhere'?s what most", "Here's what most"),
        (r'(?i)\bfair warning:', 'Fair warning:'),
    ]
    for pattern, canonical in ASIDE_PATTERNS:
        matches = list(re.finditer(pattern, full_blog))
        if len(matches) > 2:
            print(f"  🔧 '{canonical}' appears {len(matches)}× → keeping first 2")
            # Remove 3rd+ occurrences by replacing with empty
            for m in reversed(matches[2:]):
                # Remove the aside phrase (keep the rest of the sentence)
                full_blog = full_blog[:m.start()] + full_blog[m.end():]

    # 1c. Enforce banned words one more time (catches FAQ/conclusion leaks)
    banned_final = {
        'crucial': 'important', 'ensuring': 'making sure',
        'facilitate': 'help with', 'comprehensive': 'full',
        'underscore': 'highlight', 'leverage': 'use',
        'cornerstone': 'foundation', 'pivotal': 'key',
        'furthermore': '', 'moreover': '',
    }
    for phrase, replacement in banned_final.items():
        count = len(re.findall(re.escape(phrase), full_blog, flags=re.IGNORECASE))
        if count > 0:
            full_blog = re.sub(re.escape(phrase), replacement, full_blog, flags=re.IGNORECASE)
            if count > 0:
                print(f"  🔧 Replaced '{phrase}' ×{count} → '{replacement}'")

    # 1d. Fix grammar around brand replacement ("a we", "to we", etc.)
    full_blog = re.sub(r'\ba we\b', 'our', full_blog, flags=re.IGNORECASE)
    full_blog = re.sub(r'\bto we\b', 'to our team', full_blog, flags=re.IGNORECASE)
    full_blog = re.sub(r'\bwith we\b', 'with our team', full_blog, flags=re.IGNORECASE)
    full_blog = re.sub(r'\band we matches\b', 'and we match', full_blog, flags=re.IGNORECASE)
    full_blog = re.sub(r'\band we helps\b', 'and we help', full_blog, flags=re.IGNORECASE)
    full_blog = re.sub(r'\band we provides\b', 'and we provide', full_blog, flags=re.IGNORECASE)
    full_blog = re.sub(r'\bwe connects\b', 'we connect', full_blog, flags=re.IGNORECASE)

    # 1e. Collapse multiple blank lines
    full_blog = re.sub(r'\n{3,}', '\n\n', full_blog)

    stage1_wc = count_words(full_blog)
    print(f"  ✅ Stage 1 (Python fixes): {word_count} → {stage1_wc} words")

    # ══════════════════════════════════════════════════════════════
    # STAGE 2: Gemini editorial review (coherency + human voice)
    # ══════════════════════════════════════════════════════════════

    # Build diagnostic notes for Gemini
    diagnostics = []

    # Check for repeated openers
    section_openers = []
    for match in re.finditer(r'^## .+\n+(.{0,120})', full_blog, re.MULTILINE):
        opener = match.group(1).strip()
        if opener:
            section_openers.append(opener[:80])
    seen = {}
    for opener in section_openers:
        key = opener[:50].lower()
        if key in seen:
            diagnostics.append(f"REPEATED OPENER: \"{opener[:60]}...\" — rewrite to match its section topic")
        else:
            seen[key] = True

    # Check primary keyword density
    pk = plan.get('primary_keyword', '').lower()
    if pk:
        pk_count = full_blog.lower().count(pk)
        if pk_count > 6:
            diagnostics.append(f"KEYWORD STUFFING: \"{pk}\" appears {pk_count} times. Reduce to 4-5 total across the blog. Remove from sections where it doesn't fit the topic.")

    diagnostic_block = ""
    if diagnostics:
        diagnostic_block = "SPECIFIC ISSUES DETECTED (fix these first):\n" + "\n".join(f"  • {d}" for d in diagnostics)

    review_prompt = f"""You are a senior medical content editor doing a FINAL review before publication.
The blog is about: {plan['primary_keyword']}
Target audience: patients from {plan['target_countries']}

{diagnostic_block}

REVIEW CHECKLIST — fix anything that fails:

□ COHERENCY: Does each paragraph follow logically from the previous one?
  If not, add ONE bridging sentence between them. Delete any paragraph that
  repeats an idea already covered in an earlier section.

□ HUMAN VOICE: Does this sound like a real medical consultant wrote it?
  - Every section should have at least one sentence that feels personal or
    experienced (a tip, a real number, a "what patients usually tell us" detail).
  - No two sections should start the same way.
  - Sentences should vary in length: some short (5-8 words), some medium (15-20).
  - Use contractions: "it's", "you'll", "that's", "won't".

□ TRANSITIONS: Each H2 section should end with 1 sentence leading into the next topic.

□ FORMATTING: Remove any `backtick text` or **bold keywords** in paragraphs.
  Keep ## and ### headings as-is.

□ WORD ECONOMY: If the blog exceeds {stage1_wc} words, tighten paragraphs.
  Cut filler like "it's important to note that", "it should be mentioned that".
  Target: stay within ±150 words of current count.

RULES:
- Do NOT change costs, hospital names, accreditations, or table data.
- Do NOT remove or reorder H2/H3 headings.
- Do NOT remove FAQ questions.
- Return the COMPLETE blog in markdown. No preamble text.

BLOG:
{full_blog}"""

    print(f"  🔍 Stage 2: Gemini editorial review ({stage1_wc} words)...")
    revised = call_gemini(review_prompt, max_tokens=24000)  # Increased from 16K — thinking + full blog output

    if not revised or count_words(revised) < stage1_wc * 0.85:
        print(f"  ⚠️ Gemini review lost too much content ({count_words(revised)} vs {stage1_wc} words) — using Stage 1 output")
        return full_blog

    # Verify critical sections survived
    has_faq = bool(re.search(r'(?i)## Frequently Asked|## FAQ', revised))
    has_conclusion = bool(re.search(r'(?i)## Final Thoughts|## Conclusion', revised))
    if not has_faq or not has_conclusion:
        print(f"  ⚠️ Gemini review dropped FAQ={'❌' if not has_faq else '✅'} Conclusion={'❌' if not has_conclusion else '✅'} — using Stage 1 output")
        return full_blog

    # Run clean_section_text one more time on Gemini's output
    revised = clean_section_text(revised)

    print(f"  ✅ Stage 2 complete: {count_words(revised)} words (was {stage1_wc})")
    return revised



