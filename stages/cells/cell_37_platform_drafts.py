# ─── CELL B: Platform Draft Generator (NEW — Repurposing Pipeline stage 2) ─
#
# PURPOSE:
#   Reads Question_Bank and generates platform-appropriate drafts for:
#     - Quora (800-word authoritative answers)
#     - Reddit (400-word empathetic, non-promotional posts)
#     - Substack (2,000-word themed essays clustering 5-8 questions)
#
# Uses the existing pipeline's answer-hooks where available:
#   - Competitor_Answer_Ref from Url_data_ext → seeds Quora draft
#   - Forum_Master_MD voice quotes → seeds Reddit draft
#   - Blog_Output existing blog content → reference context
#
# Reads  : Keyword_n8n > Question_Bank, Forum_Master_MD, Url_data_ext,
#                        Blog_Output (if exists, for cross-reference context)
# Writes : Keyword_n8n > Quora_Drafts, Reddit_Drafts, Substack_Drafts
# ─────────────────────────────────────────────────────────────────────

from stages.cells.cell_23_shared_utils import *

import re
import time
import json
from datetime import datetime
from collections import defaultdict

import gspread
from google import genai
from app.utils.helper import get_sheet_client
from google.genai import types

from config import (
    GEMINI_API_KEY, GEMINI_MODEL, SPREADSHEET_NAME,
    SCOPES, MAX_CELL, MAX_TOKENS, SAFETY_OFF,
    BRAND, CITATION_ALLOWLIST, all_allowed_citations,
    COUNTRY_PERSONAS, get_persona, get_country_name,
    YMYL_DISCLAIMERS, FORBIDDEN_MEDICAL_CLAIMS,
    TEMP_FAQ_ANSWER, TEMP_BLOG_SECTION,
)
from config_repurpose import (
    REPURPOSE_TABS, PLATFORM_SPECS, SUBREDDIT_ALLOWLIST,
    QUORA_POLICY, REDDIT_POLICY, SUBSTACK_SETTINGS,
)

# ── Auth ─────────────────────────────────────────────────────────────
gc = get_sheet_client(SCOPES)
gemini_client = genai.Client(api_key=GEMINI_API_KEY)


# ── Sheet helpers ────────────────────────────────────────────────────
def _trunc(v):
    s = str(v) if v is not None else ""
    return s[:MAX_CELL] + "\n[TRUNCATED]" if len(s) > MAX_CELL else s


def _write_with_retry(ws, data, retries=3):
    for attempt in range(retries):
        try:
            ws.update(data, value_input_option="RAW")
            return True
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(4 * (attempt + 1))
            else:
                print(f"  ❌ Sheet write failed: {e}")
                return False


def _fmt_header(ws, n_cols):
    col_letter = chr(64 + min(n_cols, 26))
    ws.format(f"A1:{col_letter}1", {
        "textFormat": {"bold": True,
                        "foregroundColor": {"red":1,"green":1,"blue":1}},
        "backgroundColor": {"red": 0.13, "green": 0.37, "blue": 0.60},
    })


def get_or_create_tab(spreadsheet, tab_name, rows=500, cols=14):
    try:
        ws = spreadsheet.worksheet(tab_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=rows, cols=cols)
    return ws


# ── Gemini call with temperature ────────────────────────────────────
def call_gemini(prompt: str, max_tokens: int = MAX_TOKENS,
                 temperature: float = 0.6) -> str:
    for attempt in range(3):
        try:
            resp = gemini_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    max_output_tokens=max_tokens,
                    temperature=temperature,
                    safety_settings=SAFETY_OFF,
                ),
            )
            if resp.text:
                return resp.text.strip()
        except Exception as e:
            if attempt < 2:
                time.sleep(6 * (attempt + 1))
            else:
                print(f"    ⚠️ Gemini failed: {e}")
    return ""


def count_words(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text))


# ═══════════════════════════════════════════════════════════════════
# Data loaders
# ═══════════════════════════════════════════════════════════════════

def load_question_bank(sp):
    try:
        ws = sp.worksheet(REPURPOSE_TABS["question_bank"])
        records = ws.get_all_records()
    except Exception:
        print("  ❌ Question_Bank sheet not found. Run Cell A first.")
        return []
    return records


def load_forum_voice(sp, max_chars=5000):
    """Get patient-voice snippets for Reddit tone calibration."""
    try:
        ws = sp.worksheet("Forum_Master_MD")
        records = ws.get_all_records()
        if records:
            for r in records:
                if str(r.get("Category", "")).strip() == "ALL_CATEGORIES":
                    return str(r.get("MD_Content", ""))[:max_chars]
    except Exception:
        pass
    return ""


def load_existing_blog(sp, max_chars=6000):
    """Get the blog's existing markdown for cross-reference context."""
    try:
        ws = sp.worksheet("Blog_Output")
        records = ws.get_all_records()
        if records:
            return str(records[0].get("Full_Blog_Markdown", ""))[:max_chars]
    except Exception:
        pass
    return ""


# ═══════════════════════════════════════════════════════════════════
# Shared prompt blocks
# ═══════════════════════════════════════════════════════════════════

def build_citation_block() -> str:
    """Closed citation allowlist — prevents hallucinated JAMA/NEJM citations."""
    allowed = all_allowed_citations()
    if not allowed:
        return ("CITATIONS: Do not cite specific studies or bodies by name. "
                "Use 'multiple published studies' or 'international guidelines' "
                "when referring to evidence.")
    return (
        "CITATION ALLOWLIST (cite ONLY from this list — do NOT invent studies "
        "or years; for anything outside this list, write 'multiple published "
        "studies' without specifics):\n" +
        "\n".join(f"  • {s}" for s in allowed[:20])
    )


def build_ymyl_block() -> str:
    forbidden_csv = ", ".join(FORBIDDEN_MEDICAL_CLAIMS[:10])
    return (
        "YMYL RULES:\n"
        f"  - Never use: {forbidden_csv}\n"
        "  - No specific drug dosages or self-medication protocols\n"
        "  - No diagnostic claims ('if you have X you likely have Y')\n"
        "  - Use 'typically', 'often', 'in most cases' for outcomes\n"
        "  - Refer clinical decisions to the treating team\n"
    )


def build_persona_context(target_cc: str) -> str:
    if not target_cc:
        return ""
    persona = get_persona(target_cc)
    country = get_country_name(target_cc)
    return (
        f"TARGET READER: Patients from {country} considering treatment in India.\n"
        f"  Key concerns     : {persona['concerns']}\n"
        f"  Currency framing : {persona['cost_frame']} (use {persona['currency']})\n"
        f"  Trust signals    : {persona['trust_signals']}\n"
    )


# ═══════════════════════════════════════════════════════════════════
# Quora generator
# ═══════════════════════════════════════════════════════════════════

def build_quora_prompt(question: dict, forum_voice: str, blog_ref: str) -> str:
    spec = PLATFORM_SPECS["quora"]
    policy = QUORA_POLICY

    brand_line = ""
    if policy["allow_brand_mentions"]:
        brand_line = (f"Mention {BRAND['name']} ONCE naturally as your employer "
                      f"in the disclosure line. No soft-sell beyond that.")

    cta_line = ""
    if policy["allow_soft_cta"]:
        cta_line = (f"You may end with ONE short line pointing readers toward "
                    f"getting a personalised estimate (no URLs; just "
                    f"'{BRAND['name']} offers free consultations' style).")

    disclosure = policy["disclosure_text"]

    persona_block = build_persona_context(question.get("Target_Country_Code", ""))
    citation_block = build_citation_block()
    ymyl_block = build_ymyl_block()

    competitor_ref = question.get("Competitor_Answer_Ref", "")
    comp_block = ""
    if competitor_ref and len(competitor_ref) > 50:
        comp_block = (
            f"\nREFERENCE ANSWER (from a competitor — use for facts and figures "
            f"only, write in your own voice, do NOT copy):\n{competitor_ref[:800]}"
        )

    blog_block = ""
    if blog_ref:
        blog_block = (
            f"\nBACKGROUND (your company's published blog on this topic — "
            f"use as source of facts, don't quote):\n{blog_ref[:2000]}"
        )

    return f"""Write a Quora answer in the voice of a senior medical tourism
consultant at {BRAND['name']}.

THE QUESTION:
{question['Question']}

{persona_block}

{comp_block}
{blog_block}

ANSWER REQUIREMENTS:
  Target length: {spec['target_words']} words ({spec['min_words']}–{spec['max_words']})
  Tone         : {spec['tone']}
  Reading grade: Class 8 — plain English, ESL-friendly
  Format       : Short paragraphs (2-3 sentences). Use markdown headings
                  only if the answer has 3+ distinct sub-topics.

STRUCTURE:
  1. Opening (2-3 sentences): directly answer the question with a specific
     number, range, or fact. No "Great question!" or self-introduction yet.
  2. Body (3-6 short paragraphs): explain the answer with specifics — cost
     ranges, hospital names, timeline, patient mix, trade-offs. Name at
     least one hospital or accreditation for concreteness.
  3. Practical takeaway (1 paragraph): what should the reader do with this
     information? Not a pitch — genuine practical guidance.
  4. Disclosure line (1 sentence, exactly): "{disclosure}"
  {"5. Soft CTA (1 line): " + cta_line if cta_line else ""}

ENTITY NAMING: Name a real hospital, accreditation, or surgeon credential
(e.g., "Apollo Hospitals Chennai", "JCI accreditation", "FRCS-trained
surgeons"). Do NOT invent names.

{citation_block}

{ymyl_block}

ANTI-AI STYLE:
  - Use contractions (it's, you'll, don't)
  - Vary sentence length — mix short punchy sentences with longer ones
  - Never start with "Great question", "In this answer", "As a consultant"
  - Never end with "Hope this helps!" or similar
  - Specific > vague ("₹2,50,000–₹4,00,000" not "a few lakhs")

{brand_line}

Output the answer as plain markdown text. No preamble, no "Here is your answer"."""


def generate_quora_drafts(bank: list, forum_voice: str, blog_ref: str,
                            limit: int = 30) -> list:
    """Generate Quora drafts from highest-priority question-intent questions."""
    # Quora eats PAA + Patient_Questions best
    eligible = [q for q in bank
                 if q["Source"] in ("paa", "forum_question", "competitor_faq")]
    eligible.sort(key=lambda x: -float(x.get("Priority_Score", 0)))
    eligible = eligible[:limit]

    drafts = []
    for i, q in enumerate(eligible, 1):
        print(f"  🟢 [{i}/{len(eligible)}] Quora: {q['Question'][:60]}")
        prompt = build_quora_prompt(q, forum_voice, blog_ref)
        draft = call_gemini(prompt, max_tokens=4000,
                             temperature=TEMP_FAQ_ANSWER)
        if not draft:
            continue
        wc = count_words(draft)
        drafts.append({
            "Row_ID":           f"QUORA_{i:03d}",
            "Question":         q["Question"],
            "Source_Row_ID":    q.get("Row_ID", ""),
            "Target_Country":   q.get("Target_Country", ""),
            "Funnel_Stage":     q.get("Funnel_Stage", ""),
            "Priority":         q.get("Priority_Score", ""),
            "Word_Count":       wc,
            "Draft_Markdown":   _trunc(draft),
            "Review_Status":    "pending_review",
            "Posted_URL":       "",
            "Generated_At":     datetime.now().isoformat(timespec="seconds"),
        })
        time.sleep(3)
    return drafts


# ═══════════════════════════════════════════════════════════════════
# Reddit generator
# ═══════════════════════════════════════════════════════════════════

def detect_subreddit(question: dict, specialty: str) -> str:
    """Suggest a subreddit from the allowlist based on specialty + keywords."""
    candidates = SUBREDDIT_ALLOWLIST.get(specialty, []) + \
                 SUBREDDIT_ALLOWLIST.get("general", [])
    if not candidates:
        return ""
    # Without more signal we return the first one; human reviews before posting
    return candidates[0]


def build_reddit_prompt(question: dict, forum_voice: str) -> str:
    spec = PLATFORM_SPECS["reddit"]
    policy = REDDIT_POLICY

    disclosure = policy["disclosure_text"]

    voice_block = ""
    if forum_voice:
        voice_block = (
            f"\nPATIENT VOICE CALIBRATION (how real patients talk — match this "
            f"register, don't quote):\n{forum_voice[:1500]}"
        )

    persona_block = build_persona_context(question.get("Target_Country_Code", ""))
    citation_block = build_citation_block()
    ymyl_block = build_ymyl_block()

    return f"""Write a Reddit reply as someone who works in medical tourism
and is genuinely answering a question — NOT promoting anything.

THE QUESTION / CONCERN:
{question['Question']}

{persona_block}

{voice_block}

REDDIT STYLE:
  - Target length: {spec['target_words']} words ({spec['min_words']}–{spec['max_words']})
  - Tone: plain, direct, empathetic, conversational. No marketing.
  - First line: acknowledge the specific concern without being sycophantic.
  - Lower-case starts are fine. Contractions throughout. Occasional colloquial
    phrases ("honestly", "ngl", "in my experience") — but sparingly.
  - NO markdown headings. NO bullet lists unless listing 4+ discrete items.
  - NO "I hope this helps". NO signature.

CRITICAL ANTI-PROMO RULES:
  - {"FORBIDDEN: mentioning " + BRAND['name'] + " by name anywhere in the body" if policy['ban_brand_mentions'] else "brand may be named once"}
  - {"FORBIDDEN: any URL or link" if policy['ban_link_to_own_site'] else "one link allowed"}
  - {"FORBIDDEN: phrases like 'visit our website', 'DM me', 'check us out'" if policy['ban_explicit_cta'] else ""}
  - End with the disclosure line verbatim (NOT at the top — at the end):
    "{disclosure}"

STRUCTURE:
  1. Acknowledge (1-2 sentences): show you read the question, name the specific
     concern.
  2. Direct answer (2-3 sentences): the actual information the person needs,
     including specific ranges or numbers.
  3. Nuance (2-3 sentences): what it depends on, trade-offs, things nobody
     tells them.
  4. Practical next step (1-2 sentences): what they could do to get a clearer
     answer for their specific case.
  5. Disclosure line (verbatim).

{citation_block}

{ymyl_block}

ANTI-AI:
  - Never use: "comprehensive", "world-class", "cutting-edge", "game-changer",
    "navigate", "leverage", "unparalleled", "paramount", "in today's world"
  - Vary sentence starters. No two sentences in a row starting with the same word.

Output as plain text. No preamble."""


def generate_reddit_drafts(bank: list, forum_voice: str, specialty: str,
                             limit: int = 15) -> list:
    """Generate Reddit drafts from objection + patient-question sources."""
    # Reddit eats Objections + Patient_Questions best
    eligible = [q for q in bank
                 if q["Source"] in ("forum_objection", "forum_question", "paa")]
    # Filter: skip questions without emotional or objection content for Reddit
    def reddit_fit(q):
        qt = q["Question"].lower()
        funnel_match = q.get("Funnel_Stage") in ("TOFU", "MOFU")
        emotional_signal = any(w in qt for w in [
            "worried", "scared", "afraid", "safe", "risky", "trust", "scam",
            "experience", "anyone", "stories", "regret",
        ])
        return funnel_match or emotional_signal
    eligible = [q for q in eligible if reddit_fit(q)]
    eligible.sort(key=lambda x: -float(x.get("Priority_Score", 0)))
    eligible = eligible[:limit]

    drafts = []
    for i, q in enumerate(eligible, 1):
        print(f"  🟠 [{i}/{len(eligible)}] Reddit: {q['Question'][:60]}")
        prompt = build_reddit_prompt(q, forum_voice)
        draft = call_gemini(prompt, max_tokens=2500,
                             temperature=TEMP_BLOG_SECTION)
        if not draft:
            continue
        # Post-generation safety check — strip brand mentions if policy bans them
        if REDDIT_POLICY["ban_brand_mentions"] and BRAND["name"] in draft:
            draft = draft.replace(BRAND["name"], "our platform")

        wc = count_words(draft)
        subreddit = detect_subreddit(q, q.get("Specialty", "general"))

        drafts.append({
            "Row_ID":               f"REDDIT_{i:03d}",
            "Question":             q["Question"],
            "Source_Row_ID":        q.get("Row_ID", ""),
            "Target_Country":       q.get("Target_Country", ""),
            "Funnel_Stage":         q.get("Funnel_Stage", ""),
            "Priority":             q.get("Priority_Score", ""),
            "Word_Count":           wc,
            "Suggested_Subreddit":  subreddit,
            "Draft_Markdown":       _trunc(draft),
            "Review_Status":        "pending_review",
            "Posted_URL":           "",
            "Generated_At":         datetime.now().isoformat(timespec="seconds"),
        })
        time.sleep(3)
    return drafts


# ═══════════════════════════════════════════════════════════════════
# Substack generator — clusters 5-8 related questions into one essay
# ═══════════════════════════════════════════════════════════════════

def cluster_questions_for_substack(bank: list,
                                      questions_per_essay: int = 6) -> list:
    """
    Group questions by shared intent + country to form essay themes.
    Returns list of clusters, each a list of question dicts.
    """
    # Group by (funnel_stage, dominant_intent, target_cc)
    by_theme = defaultdict(list)
    for q in bank:
        intents = q.get("Intents", "").split(",")
        dominant = intents[0].strip() if intents else "general"
        key = (q.get("Funnel_Stage", "MOFU"),
               dominant,
               q.get("Target_Country_Code", ""))
        by_theme[key].append(q)

    clusters = []
    for key, qs in by_theme.items():
        if len(qs) < 3:  # need critical mass for an essay
            continue
        qs.sort(key=lambda x: -float(x.get("Priority_Score", 0)))
        clusters.append({
            "theme_key": key,
            "funnel":    key[0],
            "intent":    key[1],
            "country":   key[2],
            "questions": qs[:questions_per_essay],
        })

    # Sort clusters by total priority
    clusters.sort(
        key=lambda c: -sum(float(q.get("Priority_Score", 0))
                            for q in c["questions"])
    )
    return clusters


def build_substack_prompt(cluster: dict, blog_ref: str) -> str:
    spec = PLATFORM_SPECS["substack"]
    questions = cluster["questions"]
    country = get_country_name(cluster["country"]) if cluster["country"] else ""

    q_list = "\n".join(f"  {i+1}. {q['Question']}"
                        for i, q in enumerate(questions))

    persona_block = build_persona_context(cluster["country"])
    citation_block = build_citation_block()
    ymyl_block = build_ymyl_block()

    blog_block = ""
    if blog_ref:
        blog_block = (
            f"\nYOUR COMPANY'S PUBLISHED BLOG ON THIS TOPIC "
            f"(reference for facts; don't quote):\n{blog_ref[:3000]}"
        )

    intent_theme = {
        "cost":     "costs, pricing, and making an informed financial decision",
        "safety":   "safety, quality, and accreditation standards",
        "how_to":   "the process and what to expect step by step",
        "compare":  "comparing options and making the right choice",
        "logistics":"logistics, planning, and what nobody tells you",
        "recovery": "recovery and life after treatment",
        "procedure":"what the procedure actually involves",
        "outcome":  "outcomes, success rates, and setting realistic expectations",
    }.get(cluster["intent"], "key questions patients are asking")

    return f"""Write a Substack newsletter essay about {intent_theme} for
medical tourism to India, answering a cluster of real reader questions.

READER QUESTIONS TO ANSWER (organise the essay around these):
{q_list}

{persona_block}

{blog_block}

ESSAY REQUIREMENTS:
  Target length: {spec['target_words']} words ({spec['min_words']}–{spec['max_words']})
  Tone         : {spec['tone']}. Editorial but warmly expert. You're a
                  consultant writing to subscribers who trust you.
  Reading grade: Class 8–9
  Format       : Markdown. H2 headings for major sections. Short paragraphs.
                  One pull-quote or callout box. A comparison table if
                  appropriate for this topic.

STRUCTURE:
  1. Headline (evocative, not clickbait): 6-10 words max
  2. Subtitle (1 sentence): states the reader benefit
  3. Opening hook (100-150 words): name the real confusion/anxiety patients
     face around this topic. No "Welcome to my newsletter". Start in medias res.
  4. Main body (5-7 H2 sections): each section answers 1-2 of the questions
     above. Name specific hospitals, costs, timelines. Include a comparison
     table somewhere in the middle if relevant.
  5. The contrarian take (1 short section, 100-150 words): the thing other
     medical tourism content won't tell them. Honest trade-off or caveat.
  6. Practical next step (100 words): concrete action a reader can take.
  7. Closing CTA (1-2 sentences): direct mention of {BRAND['name']} as a
     resource. Include brand tagline if natural.

ENTITY NAMING REQUIREMENT:
  Name at least 3 real hospitals or accreditation bodies throughout.
  Name at least 2 specific cost ranges (INR + target country currency).

{citation_block}

{ymyl_block}

ANTI-AI:
  - Avoid: "in today's fast-paced world", "comprehensive", "navigate",
    "cutting-edge", "world-class", "at the forefront"
  - Use contractions
  - Active voice
  - Specific over vague always

SUBSTACK-NATIVE FORMATTING:
  - Open with the subtitle as italic text under the headline
  - Use `>` blockquote for the pull-quote
  - Use a markdown table for any comparison
  - End with "— {BRAND['name']}" signature line

Output as markdown ready to paste into Substack editor. Include the headline
as an H1 at the very top. No preamble."""


def generate_substack_drafts(bank: list, blog_ref: str,
                                max_essays: int = 6) -> list:
    """Generate Substack essays by clustering questions thematically."""
    clusters = cluster_questions_for_substack(
        bank, PLATFORM_SPECS["substack"]["questions_per_essay"],
    )
    clusters = clusters[:max_essays]

    drafts = []
    for i, cluster in enumerate(clusters, 1):
        theme = f"{cluster['funnel']}/{cluster['intent']}/{cluster['country'] or 'all'}"
        print(f"  🔵 [{i}/{len(clusters)}] Substack essay: {theme} "
              f"({len(cluster['questions'])} questions)")
        prompt = build_substack_prompt(cluster, blog_ref)
        draft = call_gemini(prompt, max_tokens=6000, temperature=0.7)
        if not draft:
            continue
        wc = count_words(draft)

        # Extract headline from first H1
        headline_match = re.search(r"^#\s+(.+)$", draft, re.MULTILINE)
        headline = headline_match.group(1).strip() if headline_match else \
                   f"{cluster['intent'].title()} essay"

        question_ids = ", ".join(q.get("Row_ID", "") for q in cluster["questions"])

        drafts.append({
            "Row_ID":            f"SUBSTACK_{i:03d}",
            "Headline":          headline,
            "Theme":             theme,
            "Funnel_Stage":      cluster["funnel"],
            "Target_Country":    get_country_name(cluster["country"]) if cluster["country"] else "All",
            "Source_Question_IDs": question_ids,
            "Questions_Covered": len(cluster["questions"]),
            "Word_Count":        wc,
            "Draft_Markdown":    _trunc(draft),
            "Review_Status":     "pending_review",
            "Published_URL":     "",
            "Generated_At":      datetime.now().isoformat(timespec="seconds"),
        })
        time.sleep(4)
    return drafts


# ═══════════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════════

print("\n" + "═" * 65)
print("  PLATFORM DRAFT GENERATOR")
print("  Generates Quora + Reddit + Substack drafts from Question_Bank")
print("═" * 65)

sp = gc.open(SPREADSHEET_NAME)

# Load question bank
print("\n  Loading Question_Bank...")
bank = load_question_bank(sp)
if not bank:
    print("❌ Question_Bank is empty. Run Cell A (Question Bank Builder) first.")
else:
    print(f"  ✅ Loaded {len(bank)} questions from Question_Bank")

    # Get specialty from first row
    specialty = bank[0].get("Specialty", "general") if bank else "general"

    # Load supporting context
    print("\n  Loading supporting context...")
    forum_voice = load_forum_voice(sp)
    blog_ref    = load_existing_blog(sp)
    print(f"    Forum voice data   : {len(forum_voice)} chars")
    print(f"    Existing blog ref  : {len(blog_ref)} chars")

    # Decide what to generate based on bank size
    bank_size = len(bank)
    quora_limit    = min(30, bank_size)
    reddit_limit   = min(15, max(5, bank_size // 4))
    substack_limit = min(6, max(2, bank_size // 20))

    # ══ QUORA ══
    print(f"\n  ─── Generating Quora drafts (target {quora_limit}) ───")
    quora_drafts = generate_quora_drafts(
        bank, forum_voice, blog_ref, limit=quora_limit,
    )
    if quora_drafts:
        HEADERS = list(quora_drafts[0].keys())
        rows = [HEADERS] + [[_trunc(d[h]) for h in HEADERS] for d in quora_drafts]
        ws = get_or_create_tab(sp, REPURPOSE_TABS["quora_drafts"],
                                rows=max(len(rows)+20, 100),
                                cols=len(HEADERS))
        _write_with_retry(ws, rows)
        _fmt_header(ws, len(HEADERS))
        print(f"  ✅ {len(quora_drafts)} Quora drafts written to "
              f"'{REPURPOSE_TABS['quora_drafts']}'")

    # ══ REDDIT ══
    print(f"\n  ─── Generating Reddit drafts (target {reddit_limit}) ───")
    reddit_drafts = generate_reddit_drafts(
        bank, forum_voice, specialty, limit=reddit_limit,
    )
    if reddit_drafts:
        HEADERS = list(reddit_drafts[0].keys())
        rows = [HEADERS] + [[_trunc(d[h]) for h in HEADERS] for d in reddit_drafts]
        ws = get_or_create_tab(sp, REPURPOSE_TABS["reddit_drafts"],
                                rows=max(len(rows)+20, 100),
                                cols=len(HEADERS))
        _write_with_retry(ws, rows)
        _fmt_header(ws, len(HEADERS))
        print(f"  ✅ {len(reddit_drafts)} Reddit drafts written to "
              f"'{REPURPOSE_TABS['reddit_drafts']}'")
        no_sub = sum(1 for d in reddit_drafts if not d["Suggested_Subreddit"])
        if no_sub:
            print(f"  ⚠️  {no_sub} drafts have no suggested subreddit "
                  f"(SUBREDDIT_ALLOWLIST in config_repurpose.py is empty)")

    # ══ SUBSTACK ══
    print(f"\n  ─── Generating Substack essays (target {substack_limit}) ───")
    substack_drafts = generate_substack_drafts(
        bank, blog_ref, max_essays=substack_limit,
    )
    if substack_drafts:
        HEADERS = list(substack_drafts[0].keys())
        rows = [HEADERS] + [[_trunc(d[h]) for h in HEADERS] for d in substack_drafts]
        ws = get_or_create_tab(sp, REPURPOSE_TABS["substack_drafts"],
                                rows=max(len(rows)+20, 50),
                                cols=len(HEADERS))
        _write_with_retry(ws, rows)
        _fmt_header(ws, len(HEADERS))
        print(f"  ✅ {len(substack_drafts)} Substack essays written to "
              f"'{REPURPOSE_TABS['substack_drafts']}'")

    # ── Summary ──
    print("\n" + "═" * 65)
    print("  REPURPOSING PIPELINE COMPLETE")
    print("═" * 65)
    print(f"  Output sheets in '{SPREADSHEET_NAME}':")
    print(f"    • {REPURPOSE_TABS['quora_drafts']}    "
          f"({len(quora_drafts) if quora_drafts else 0} drafts)")
    print(f"    • {REPURPOSE_TABS['reddit_drafts']}   "
          f"({len(reddit_drafts) if reddit_drafts else 0} drafts)")
    print(f"    • {REPURPOSE_TABS['substack_drafts']} "
          f"({len(substack_drafts) if substack_drafts else 0} essays)")
    print(f"\n  NEXT STEPS (manual):")
    print(f"    1. Review each draft in the sheet")
    print(f"    2. Edit Review_Status column: 'approved' / 'rejected' / 'edit_needed'")
    print(f"    3. Post approved Quora drafts manually (disclosure included)")
    print(f"    4. Post approved Reddit drafts manually to vetted subreddits")
    print(f"    5. Publish Substack essays via Substack editor or API")
    print(f"    6. Record posted URLs in the Posted_URL / Published_URL column")
    print("═" * 65)
