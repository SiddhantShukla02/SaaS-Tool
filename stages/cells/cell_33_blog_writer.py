# ─── CELL: Enhanced Blog Writer (REVISED) ────────────────────────────
#
# CHANGES FROM V15:
#   1. AEO/GEO rules baked into section prompt:
#      - Answer-first paragraphs (first 2 sentences answer the H2 directly)
#      - Entity-explicit naming (no "our partner hospital")
#      - Statistical density target (≥ 5 stats per 1000 words)
#      - "X is Y" definitions for first mention of medical entities
#      - Passage self-containment (no "as discussed above")
#      - Comparison tables, not prose, for any comparison
#      - Speakable-sentence flagging for dev team
#
#   2. YMYL guardrails:
#      - No specific dosages, surgical self-care, diagnostic claims
#      - Mandatory disclaimer on clinical outcome claims
#      - Citation requirement for every medical factual claim
#      - Author byline / medical reviewer placeholder
#
#   3. Locale-aware framing:
#      - Pulls persona matrix from config per target country
#      - Currency, language, trust signals applied throughout
#      - Country-specific objections addressed
#
#   4. Replaces post-hoc banned_phrase dict with positive-example prompting.
#      The v15 approach of "never use these 30 words" then regex-replacing
#      is replaced with "here's the voice we want" + 3 positive examples.
#
#   5. Reading grade target: Class 8 (was ambiguous).
#
#   6. Structured output includes Speakable candidates and citation list
#      for handoff to dev team and blog-grade-reducer skill.
#
# Reads  : Keyword_n8n > Blog_Outline, Empathy_FAQ_Output, H1_Meta_Output,
#          Keyword_data (JSON), keyword, Forum_Master_MD
# Writes : Keyword_n8n > Blog_Output, Speakable_Candidates, Citation_List
#          Google Doc   > Blog_Writeup
# ─────────────────────────────────────────────────────────────────────

import json
import time
import re
import gspread
from google import genai
from google.genai import types
from googleapiclient.discovery import build
from app.utils.helper import get_sheet_client

from config import (
    GEMINI_API_KEY, GEMINI_MODEL, SPREADSHEET_NAME,
    MAX_CELL, MAX_TOKENS, SAFETY_OFF, SCOPES,
    TARGET_WORDS_PER_SECTION, HARD_CAP_WORDS, TARGET_READING_GRADE,
    MIN_STATS_PER_1000, YMYL_DISCLAIMERS,
    get_country_name, get_persona,
)


from stages.cells.cell_23_shared_utils import *
from config import DOC_OUTPUT_TITLE


# ── Config specific to this cell ─────────────────────────────────────
OUTLINE_TAB         = "Blog_Outline"
EMPATHY_TAB         = "Empathy_FAQ_Output"
H1_META_TAB         = "H1_Meta_Output"
KEYWORD_TAB         = "keyword"
KW_DATA_TAB         = "Keyword_data"
FORUM_MD_TAB        = "Forum_Master_MD"
OUTPUT_TAB          = "Blog_Output"
SPEAKABLE_TAB       = "Speakable_Candidates"
CITATIONS_TAB       = "Citation_List"
DOC_TITLE           = DOC_OUTPUT_TITLE

# ── Auth ─────────────────────────────────────────────────────────────
gc = get_sheet_client(SCOPES)
gemini_client = genai.Client(api_key=GEMINI_API_KEY)
docs_service  = build("docs", "v1", credentials=creds)
drive_service = build("drive", "v3", credentials=creds)



def get_or_create_tab(spreadsheet, tab_name, rows=3000, cols=20):
    try:
        ws = spreadsheet.worksheet(tab_name)
        ws.clear()
        print(f"  📋 Tab '{tab_name}' cleared and ready.")
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=rows, cols=cols)
        print(f"  📋 Tab '{tab_name}' created.")
    return ws

# ════════════════════════════════════════════════════════════════════
# GOOGLE DOC HELPERS
# ════════════════════════════════════════════════════════════════════

def _get_or_create_doc(title):
    res = drive_service.files().list(
        q=f"name='{title}' and mimeType='application/vnd.google-apps.document' and trashed=false",
        fields="files(id,name)"
    ).execute()
    files = res.get("files", [])
    if files:
        doc_id = files[0]["id"]
        print(f"  📄 Doc found     : '{title}'  ID: {doc_id}")
        return doc_id
    doc    = docs_service.documents().create(body={"title": title}).execute()
    doc_id = doc["documentId"]
    drive_service.permissions().create(
        fileId=doc_id, body={"type": "anyone", "role": "writer"}
    ).execute()
    print(f"  📄 Doc created   : '{title}'  ID: {doc_id}")
    return doc_id

def _clear_doc(doc_id):
    doc          = docs_service.documents().get(documentId=doc_id).execute()
    body_content = doc.get("body", {}).get("content", [])
    if len(body_content) <= 1:
        return
    end_index = body_content[-1].get("endIndex", 1) - 1
    if end_index > 1:
        docs_service.documents().batchUpdate(
            documentId=doc_id,
            body={"requests": [{"deleteContentRange": {"range": {"startIndex": 1, "endIndex": end_index}}}]}
        ).execute()
        print(f"  🗑️  Doc cleared")

def _batch_update(doc_id, requests):
    for i in range(0, len(requests), 50):
        docs_service.documents().batchUpdate(
            documentId=doc_id, body={"requests": requests[i:i+50]}
        ).execute()
        time.sleep(0.3)

def write_markdown_to_doc(doc_id, markdown_text, doc_title):
    print(f"\n  📝 Writing blog to Google Doc '{doc_title}'...")
    _clear_doc(doc_id)

    lines    = markdown_text.splitlines()
    segments = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            segments.append({"type": "blank", "text": "\n"})
        elif stripped.startswith("### "):
            segments.append({"type": "h3", "text": stripped[4:].strip()})
        elif stripped.startswith("## "):
            segments.append({"type": "h2", "text": stripped[3:].strip()})
        elif stripped.startswith("# "):
            segments.append({"type": "h1", "text": stripped[2:].strip()})
        else:
            segments.append({"type": "p",  "text": stripped})

    while segments and segments[0]["type"]  == "blank": segments.pop(0)
    while segments and segments[-1]["type"] == "blank": segments.pop()

    STYLE_MAP = {"h1": "TITLE", "h2": "HEADING_1", "h3": "HEADING_2", "p": "NORMAL_TEXT"}
    requests  = []
    cur_idx   = 1

    for seg in segments:
        seg_type = seg["type"]
        raw_text = seg["text"]

        if seg_type == "blank":
            requests.append({"insertText": {"location": {"index": cur_idx}, "text": "\n"}})
            cur_idx += 1
            continue

        bold_pattern = re.compile(r"\*\*(.+?)\*\*")
        parts, last_end = [], 0
        for m in bold_pattern.finditer(raw_text):
            if m.start() > last_end:
                parts.append({"text": raw_text[last_end:m.start()], "bold": False})
            parts.append({"text": m.group(1), "bold": True})
            last_end = m.end()
        if last_end < len(raw_text):
            parts.append({"text": raw_text[last_end:], "bold": False})
        if not parts:
            parts = [{"text": raw_text, "bold": False}]

        parts[-1]["text"] += "\n"
        para_start = cur_idx

        for part in parts:
            if not part["text"]: continue
            requests.append({"insertText": {"location": {"index": cur_idx}, "text": part["text"]}})
            text_len = len(part["text"])
            if part["bold"]:
                requests.append({"updateTextStyle": {
                    "range": {"startIndex": cur_idx, "endIndex": cur_idx + text_len},
                    "textStyle": {"bold": True}, "fields": "bold"
                }})
            cur_idx += text_len

        named_style = STYLE_MAP.get(seg_type, "NORMAL_TEXT")
        requests.append({"updateParagraphStyle": {
            "range": {"startIndex": para_start, "endIndex": cur_idx},
            "paragraphStyle": {"namedStyleType": named_style},
            "fields": "namedStyleType"
        }})

    if requests:
        _batch_update(doc_id, requests)
        print(f"  ✅ Doc written   : {len(segments)} segments → {cur_idx} chars inserted")
    else:
        print(f"  ⚠️  No content to write to Doc")

    doc_url = f"https://docs.google.com/document/d/{doc_id}/edit"
    print(f"  🔗 {doc_url}")
    return doc_url


# ═══════════════════════════════════════════════════════════════════
# Gemini calls
# ═══════════════════════════════════════════════════════════════════

def call_gemini(prompt: str, max_tokens: int = MAX_TOKENS) -> str:
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
            if resp.text:
                return resp.text.strip()
        except Exception as e:
            print(f"  ⚠️ Gemini attempt {attempt+1}/3: {e}")
            if attempt < 2:
                time.sleep(6 * (attempt + 1))
    return ""


def count_words(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text))


# ═══════════════════════════════════════════════════════════════════
# Data loaders
# ═══════════════════════════════════════════════════════════════════

def load_blog_plan(sp):
    """
    Load all data needed for blog generation.
    Returns dict with outline, hooks, FAQ answers, keywords, forum data,
    and the target country personas assembled from config.
    """
    plan = {
        "chosen_h1": "", "primary_keyword": "", "target_countries": "",
        "target_country_codes": [], "outline": "",
        "empathy_hooks": "", "faq_answers": "",
        "keyword_pool": [], "keyword_by_category": {},
        "forum_data": "", "personas": [],
    }

    # H1 + Meta
    try:
        ws = sp.worksheet(H1_META_TAB)
        recs = ws.get_all_records()
        if recs:
            plan["chosen_h1"] = str(recs[0].get("Chosen_H1", "")).strip()
    except Exception:
        pass

    # Outline
    try:
        ws = sp.worksheet(OUTLINE_TAB)
        recs = ws.get_all_records()
        if recs:
            row = recs[0]
            plan["primary_keyword"] = str(row.get("Primary_Keyword", "")).strip()
            plan["target_countries"] = str(row.get("Target_Countries", "")).strip()
            plan["outline"] = str(row.get("Complete_Outline", "")).strip()
            if not plan["chosen_h1"]:
                plan["chosen_h1"] = str(row.get("Chosen_H1", "")).strip()
    except Exception:
        pass

    # Country codes + personas
    try:
        ws = sp.worksheet(KEYWORD_TAB)
        recs = ws.get_all_records()
        codes = list(set(str(r.get("Country_Code", "")).strip().lower()
                         for r in recs if r.get("Country_Code")))
        plan["target_country_codes"] = [c for c in codes if c]
        plan["personas"] = [
            {"code": c, "name": get_country_name(c), **get_persona(c)}
            for c in plan["target_country_codes"]
        ]
    except Exception:
        pass

    # Empathy hooks + FAQ
    try:
        ws = sp.worksheet(EMPATHY_TAB)
        recs = ws.get_all_records()
        if recs:
            plan["empathy_hooks"] = str(recs[0].get("Empathy_Hooks", "")).strip()
            plan["faq_answers"] = str(recs[0].get("FAQ_Answers", "")).strip()
    except Exception:
        pass

    # Keywords (structured JSON from revised Cell 14)
    try:
        ws = sp.worksheet(KW_DATA_TAB)
        recs = ws.get_all_records()
        all_kws = set()
        by_cat = {"procedure_types": set(), "patient_concerns": set(),
                  "safety_quality": set(), "recovery_results": set(),
                  "travel_logistics": set(), "cost_value": set(),
                  "hospital_surgeon_brand": set()}
        for r in recs:
            json_text = str(r.get("Extracted_JSON", "")).strip()
            if not json_text or json_text.startswith("("):
                continue
            try:
                data = json.loads(json_text)
                for cat in by_cat:
                    for kw in data.get(cat, []):
                        kw_clean = kw.strip().lower()
                        if len(kw_clean) > 2:
                            all_kws.add(kw_clean)
                            by_cat[cat].add(kw_clean)
            except json.JSONDecodeError:
                continue
        plan["keyword_pool"] = sorted(all_kws)
        plan["keyword_by_category"] = {k: sorted(v) for k, v in by_cat.items()}
    except Exception as e:
        print(f"  ⚠️ Keyword_data load: {e}")

    # Forum data
    try:
        ws = sp.worksheet(FORUM_MD_TAB)
        recs = ws.get_all_records()
        for r in recs:
            cat = str(r.get("Category", "")).strip()
            if cat == "ALL_CATEGORIES":
                plan["forum_data"] = str(r.get("MD_Content", "")).strip()
                break
    except Exception:
        pass

    return plan


# ═══════════════════════════════════════════════════════════════════
# Voice and persona helpers
# ═══════════════════════════════════════════════════════════════════

def build_persona_brief(personas: list) -> str:
    """Build a compact persona brief for the prompt."""
    if not personas:
        return "Target audience: international patients researching treatment in India."

    lines = ["TARGET AUDIENCE PERSONAS (apply these concerns throughout):"]
    for p in personas:
        lines.append(
            f"  {p['name']} ({p['code'].upper()}): "
            f"concerns = {p['concerns']}; cost frame = {p['cost_frame']}; "
            f"trust signals = {p['trust_signals']}; currency = {p['currency']}."
        )
    return "\n".join(lines)


def build_voice_brief() -> str:
    """Positive-example voice brief (replaces the banned-phrase dict)."""
    return """DIVINHEAL VOICE — write like this:

Voice characteristics:
- Warm but authoritative, like a consultant who has helped 500+ patients
- British English spelling (colour, organise, centre)
- Contractions used naturally (it's, you'll, don't, we're)
- Second person throughout (you, your)
- Specific over vague: "JCI-accredited" not "internationally accredited"
- Concrete insider detail per section (e.g., "most Delhi clinics include 2 nights' stay")

Good examples:
✅ "Your recovery takes 3 to 6 weeks, depending on your age and overall health."
✅ "Apollo Hospitals Chennai performs over 2,000 cornea transplants annually."
✅ "You'll save roughly 70% compared to private clinics in Melbourne or Sydney."
✅ "The 30-day mortality rate below 2% matches UK NHS outcomes."

Weak examples (avoid):
❌ "Recovery takes some time and most patients do well."
❌ "We work with world-class hospitals."
❌ "Costs vary depending on many factors."
❌ "The mortality rate is very low."

Sentence-length mix:
- Average 15-18 words per sentence
- No sentence over 30 words
- Mix short punchy sentences with longer explanatory ones
"""


def build_aeo_geo_brief() -> str:
    """AEO/GEO rules baked into every section."""
    return """AEO / GEO RULES (every section must satisfy these):

1. ANSWER-FIRST PARAGRAPH: First 1-2 sentences after the H2 must directly
   answer the H2 as if it were a question. Include the primary keyword or
   the main entity in the answer sentence.
   Example: H2 "How Much Does Cornea Transplant in India Cost?"
   → "Cornea transplant in India costs between ₹70,000 and ₹1,50,000
     ($850–$1,800), roughly 70-85% less than private UK or Australian clinics."

2. ENTITY-EXPLICIT NAMING: Name real hospitals, surgeons, accreditations.
   - "Apollo Hospitals, Chennai" not "our partner hospital"
   - "JCI-accredited" not "internationally accredited"
   - "Dr. Sanjay Mehta, FRCS" is fine; invented names are not

3. "X IS Y" DEFINITIONS: On first mention of any medical entity, define
   it clearly. Example: "Keratoconus is a condition where the cornea
   thins and bulges outward, causing blurred vision."

4. STATISTICAL DENSITY: Include ≥ 5 specific numbers, stats, or data
   points per 1000 words. Percentages, durations, costs, success rates.

5. SELF-CONTAINED PASSAGES: Each section must make sense if extracted
   alone. Never use "as discussed above" or "in the previous section".

6. COMPARISON TABLES: Any comparison (cost, outcomes, duration, criteria)
   goes in a markdown table. Never prose.

7. INLINE SOURCE ATTRIBUTION: Every medical claim gets an inline source.
   Example: "A 2023 ICMR study of 2,400 patients showed..."
   Use umbrella bodies (WHO, ICMR, NICE) for general claims; specific
   studies only when verifiable.

8. SPEAKABLE CANDIDATES: Include at least ONE short factual sentence
   per section ideal for voice extraction — under 20 words, one clear
   fact, no pronouns pointing elsewhere. Mark these with [SPEAKABLE]
   at the start of the line.
"""


def build_ymyl_brief(target_countries: str) -> str:
    """YMYL medical-content guardrails."""
    return f"""YMYL MEDICAL CONTENT RULES (mandatory):

1. NO SPECIFIC DOSAGES. Do not recommend specific drug dosages or
   self-medication protocols. "Your doctor prescribes anti-rejection
   medication" is fine; "Take 5mg Prednisolone daily" is NOT.

2. NO DIAGNOSTIC CLAIMS. Do not tell readers they have a condition.
   "Keratoconus often presents with blurred vision" is fine; "If you
   have blurred vision you likely have keratoconus" is NOT.

3. NO SURGICAL SELF-CARE INSTRUCTIONS. Do not give step-by-step
   post-surgical care. Refer to the treating team instead.

4. CITATION REQUIREMENT. Every factual medical claim requires a
   source. If you can't cite a real source, reword to general
   ("multiple studies suggest...") or omit.

5. DISCLAIMER PLACEMENT. Include this line (verbatim) at the END of
   any section that mentions specific clinical outcomes, success
   rates, or survival data:
   "{YMYL_DISCLAIMERS['clinical_outcome']}"

6. COST DISCLAIMER. Include this line at the end of the cost section:
   "{YMYL_DISCLAIMERS['cost_estimate']}"

7. NO INVENTED ENTITIES. Do not invent hospital names, doctor names,
   study titles, years, or journal names. If uncertain, use generic
   form ("leading cardiac centres in Delhi") rather than inventing
   specifics.

8. AUDIENCE REMINDER. Audience is patients from {target_countries}
   making a serious healthcare decision — not US/UK-centric readers.
"""


# ═══════════════════════════════════════════════════════════════════
# Section prompt builder — all rules in one place
# ═══════════════════════════════════════════════════════════════════

def build_section_prompt(section: dict, keywords: list, plan: dict,
                          empathy_hook: str, current_wc: int, remaining: int) -> str:
    """Build the per-section prompt with AEO/GEO/YMYL/voice rules baked in."""

    h3_block = ""
    if section["h3s"]:
        h3_block = "Sub-sections (### H3):\n" + "\n".join(
            f"  - {h}" for h in section["h3s"]
        )

    hook_block = f"\nEMPATHY HOOK — open with a version of this:\n{empathy_hook}" if empathy_hook else ""

    # Forum snippets relevant to this section
    forum_block = ""
    if plan.get("forum_data"):
        search_terms = set(section["h2"].lower().split())
        for h3 in section.get("h3s", []):
            search_terms.update(h3.lower().split())
        search_terms -= {"the", "a", "an", "in", "of", "and", "to", "for",
                          "your", "with", "is", "how", "what", "why"}

        relevant = []
        for line in plan["forum_data"].split("\n"):
            if len(line.strip()) < 20:
                continue
            matches = sum(1 for w in search_terms if w in line.lower())
            if matches >= 2:
                relevant.append(line.strip())

        if relevant:
            forum_block = "\nPATIENT CONCERNS RELEVANT TO THIS SECTION:\n" + "\n".join(relevant[:5])

    # Content-needs tags
    tag_block = ""
    tags = section.get("tags", "")
    if "[TABLE]" in tags:  tag_block += "\n- Include a markdown comparison table."
    if "[CTA]" in tags:    tag_block += "\n- End with a Divinheal call-to-action."
    if "[STORY]" in tags:  tag_block += "\n- Include a brief patient scenario (composite, with 'illustrative' noted)."
    if "[MANUAL DATA]" in tags: tag_block += "\n- Include specific data (costs, success rates, named hospitals)."

    primary_kw = plan["primary_keyword"]
    persona_brief = build_persona_brief(plan["personas"])
    voice_brief = build_voice_brief()
    aeo_brief = build_aeo_geo_brief()
    ymyl_brief = build_ymyl_brief(plan["target_countries"])

    kw_instruction = f"PRIMARY KEYWORD (use ONCE if it fits naturally): {primary_kw}"
    if keywords:
        kw_instruction += f"\nSUPPORTING TERMS (weave in naturally, skip forced ones): {', '.join(keywords[:5])}"

    return f"""Write ONE section of a medical tourism blog for Divinheal.

SECTION: ## {section['h2']}
PURPOSE: {section.get('intent', 'Inform and guide the reader through this topic')}
{h3_block}
{hook_block}
{forum_block}

{persona_brief}

{kw_instruction}

WORD COUNT: {TARGET_WORDS_PER_SECTION} words (hard limit: {TARGET_WORDS_PER_SECTION + 100}).
READING GRADE TARGET: Class {TARGET_READING_GRADE} (avg 15-18 words/sentence, plain vocabulary).

{voice_brief}

{aeo_brief}

{ymyl_brief}

STRUCTURE RULES:
- Use ## for H2 (the one above), ### for H3 subsections, NEVER #### (H4)
- Paragraphs: 3-4 sentences max
- Bullets ONLY for lists of 4+ comparable items; otherwise prose

PRICING FORMAT: INR first, then USD equivalent, and target-country currency.
Example: "₹2,50,000 – ₹5,00,000 ($3,000 – $6,000 / AUD 4,500 – 9,000)"
{tag_block}

DO NOT mention Divinheal in this section (brand appears only in the dedicated
Divinheal section and the conclusion).

Return ONLY the section markdown. No preamble, no explanation."""


# ═══════════════════════════════════════════════════════════════════
# Section classification + keyword mapping (simplified from v15)
# ═══════════════════════════════════════════════════════════════════

def classify_section_type(h2: str) -> str:
    """Categorize H2 to help with keyword mapping."""
    h2l = h2.lower()
    if any(w in h2l for w in ["cost", "price", "afford", "saving", "package"]):
        return "cost"
    if any(w in h2l for w in ["safe", "risk", "quality", "accredit", "certif"]):
        return "safety"
    if any(w in h2l for w in ["hospital", "clinic", "surgeon", "doctor", "best"]):
        return "brand"
    if any(w in h2l for w in ["recovery", "heal", "after", "post", "result"]):
        return "recovery"
    if any(w in h2l for w in ["travel", "visa", "logistics", "stay"]):
        return "logistics"
    if any(w in h2l for w in ["procedure", "type", "how", "process", "step"]):
        return "procedure"
    if any(w in h2l for w in ["symptom", "concern", "condition", "diagnosis"]):
        return "concerns"
    return "general"


def get_keywords_for_section(section_type: str, by_category: dict) -> list:
    """Pull appropriate keywords for section type from the structured JSON."""
    cat_map = {
        "cost":       "cost_value",
        "safety":     "safety_quality",
        "brand":      "hospital_surgeon_brand",
        "recovery":   "recovery_results",
        "logistics":  "travel_logistics",
        "procedure":  "procedure_types",
        "concerns":   "patient_concerns",
    }
    cat = cat_map.get(section_type)
    if cat and by_category.get(cat):
        return by_category[cat][:8]
    # General fallback: pull from procedure_types + patient_concerns
    combined = by_category.get("procedure_types", [])[:4]
    combined += by_category.get("patient_concerns", [])[:4]
    return combined


# ═══════════════════════════════════════════════════════════════════
# Post-generation: extract Speakable candidates + citations
# ═══════════════════════════════════════════════════════════════════

def extract_speakable_candidates(full_blog: str) -> list:
    """Find sentences tagged [SPEAKABLE] or matching Speakable criteria."""
    candidates = []

    # Explicit [SPEAKABLE] tags from the writer
    for m in re.finditer(r'\[SPEAKABLE\]\s*(.+?)(?:\n|$)', full_blog):
        sent = m.group(1).strip()
        if len(sent.split()) <= 20 and len(sent) > 20:
            candidates.append(sent)

    # Auto-detect: short factual sentences with numbers
    if len(candidates) < 5:
        for sent in re.split(r'(?<=[.!?])\s+', full_blog):
            sent = sent.strip()
            if sent in candidates:
                continue
            words = sent.split()
            if not (5 < len(words) <= 20):
                continue
            # Must contain a number/percentage/currency
            if not re.search(r'\d', sent):
                continue
            # Avoid pronouns pointing backward
            if re.match(r'^\s*(It|This|That|These|Those)\b', sent):
                continue
            candidates.append(sent)
            if len(candidates) >= 10:
                break

    # Strip the [SPEAKABLE] tag from blog text
    return candidates[:10]


def extract_citations(full_blog: str) -> list:
    """Find inline citations for dev team to verify."""
    citations = []
    # Pattern 1: "According to [source], year study"
    for m in re.finditer(r'(?:According to|Per)\s+(?:a\s+)?(\d{4})?\s*([A-Z][\w\s&,.]+?(?:study|report|guidelines?|data))', full_blog):
        citations.append(m.group(0).strip())
    # Pattern 2: "(Author, year)" or "(Source, year)"
    for m in re.finditer(r'\(([A-Z][\w\s]+,\s*\d{4})\)', full_blog):
        citations.append(m.group(1))
    # Pattern 3: "WHO / ICMR / NABH / JCI / FRCS" mentions with context
    for m in re.finditer(r'([A-Z]{2,4}(?:-[a-z]+)?)[^.]*?(?:study|report|accred|protocol|guidelines?)', full_blog):
        citations.append(m.group(0)[:100])
    return list(dict.fromkeys(citations))[:20]  # Dedup, cap at 20


def clean_speakable_tags(full_blog: str) -> str:
    """Remove [SPEAKABLE] tags from final blog text — they're extraction markers only."""
    return re.sub(r'\[SPEAKABLE\]\s*', '', full_blog)


# ═══════════════════════════════════════════════════════════════════
# Build FAQ section
# ═══════════════════════════════════════════════════════════════════

def build_faq_prompt(faqs, keywords, remaining_budget):
    faq_list = ""
    for i, faq in enumerate(faqs, 1):
        if isinstance(faq, dict):
            faq_list += f"Q{i}: {faq.get('question','')}\nContext: {faq.get('answer','')}\n\n"
        else:
            faq_list += f"Q{i}: {faq}\n\n"

    return f"""You are a Senior SEO Medical Content Writer specialising in medical tourism.

TASK: Write the FAQ section and Conclusion for a medical blog article.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FAQ QUESTIONS (each as ### H3 heading):
{faq_list}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
KEYWORDS: {", ".join(keywords[:60])}

WORD COUNT TARGET: {min(remaining_budget, 500)} words total

FAQ FORMAT:
- Section heading: ## Frequently Asked Questions
- Each question: ### [Question text]
- Answer: 2-3 natural prose sentences (40-70 words). Featured-snippet ready.
- No Q:/A: labels. No bullet points. No "Great question" openers.

CONCLUSION FORMAT:
- Heading: ## Final Thoughts
- 120-150 words. Warm, empathetic, action-oriented.
- End with a forward-looking statement about the patient's journey.
- Do NOT open with "In conclusion" or "To summarise".

VOICE: Vary sentence length. At least one first-person clinical observation.
Avoid: Furthermore, Moreover, Additionally, It is important to note.

Return ONLY the markdown content. No preamble."""


# ═══════════════════════════════════════════════════════════════════
# Main execution (abbreviated — full logic same as v15, with swaps)
# ═══════════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════════
# PATCHED v16.1: Outline format adapter
# Bridges v15 Cell 27's free-text outline output to revised Cell 33's
# structured-sections expectations. Without this, the revised Cell 33
# would fail to parse the outline and produce empty blog sections.
# ═══════════════════════════════════════════════════════════════════

def parse_v15_outline_to_sections(outline_text: str) -> list:
    """Parse v15 Cell 27 free-text outline into structured section dicts."""
    if not outline_text:
        return []

    sections = []
    current = None
    outline_text = re.sub(r"\*\*([^*\n]+)\*\*", r"\1", outline_text)

    H2_EXPLICIT = re.compile(
        r"^(?:#{1,4}\s*)?H2\s*:\s*(.+?)"
        r"(?:\s*[—\-]\s*Sources?\s*:\s*([^—\-\n]+?))?"
        r"(?:\s*[—\-]\s*Tags?\s*:\s*([^—\-\n]+?))?"
        r"(?:\s*[—\-]\s*~?\s*(\d+)\s*words?)?"
        r"\s*$", re.IGNORECASE)
    H2_MD = re.compile(
        r"^#{2,3}\s+(?!H3\b)(.+?)"
        r"(?:\s*[—\-]\s*Sources?\s*:\s*([^—\-\n]+?))?"
        r"(?:\s*[—\-]\s*Tags?\s*:\s*([^—\-\n]+?))?"
        r"\s*$", re.IGNORECASE)
    H3_EXPLICIT = re.compile(r"^\s*(?:#{3,4}\s*)?H3\s*:\s*(.+?)\s*$", re.IGNORECASE)
    H3_MD = re.compile(r"^\s*#{3,4}\s+(.+?)\s*$")
    H3_BULLET = re.compile(r"^\s*[-•]\s+(.+?)\s*$")
    FAQ_Q = re.compile(r"^\s*Q\d+\s*:\s*(.+?)(?:\s*[—\-]\s*Source\s*:\s*[^\n]+)?\s*$", re.IGNORECASE)

    SKIP = [
        re.compile(r"^\s*(?:#{1,3}\s*)?(?:Content\s*Gap|Internal\s*Linking|Recommended\s*Word|Must[- ]Have|Gap\s*Opportunit|Unique\s*to|Blog\s*Outline\s*$|HEADING\s*DEPTH)", re.IGNORECASE),
        re.compile(r"^\s*H1\s*:", re.IGNORECASE),
    ]

    for line in outline_text.split("\n"):
        s = line.strip()
        if not s:
            continue
        if any(p.match(s) for p in SKIP):
            continue

        m = H2_EXPLICIT.match(s) or H2_MD.match(s)
        if m:
            if current:
                sections.append(current)
            heading = m.group(1).strip().strip("*").strip()
            heading = re.sub(r"\s*[—\-]\s*(?:Sources?|Tags?|~?\d+\s*words?).*$", "",
                              heading, flags=re.IGNORECASE).strip()
            sources = (m.group(2) or "").strip() if m.lastindex and m.lastindex >= 2 else ""
            tags = (m.group(3) or "").strip() if m.lastindex and m.lastindex >= 3 else ""
            wc_text = m.group(4) if m.lastindex and m.lastindex >= 4 else ""
            try:
                word_budget = int(wc_text) if wc_text else 400
            except (ValueError, TypeError):
                word_budget = 400
            current = {
                "h2": heading, "h3s": [], "tags": tags, "intent": "",
                "sources": sources, "word_budget": word_budget, "faq_questions": [],
            }
            continue

        if current is None:
            continue

        is_faq = bool(re.search(r"faq|frequent|questions?", current["h2"], re.IGNORECASE))

        if is_faq:
            fm = FAQ_Q.match(s)
            if fm:
                current["faq_questions"].append(fm.group(1).strip())
                current["h3s"].append(fm.group(1).strip())
                continue

        hm = H3_EXPLICIT.match(s)
        if not hm and line.startswith(("  ", "\t", "   -", "- ")):
            hm = H3_BULLET.match(s)
        if not hm:
            hm = H3_MD.match(s)

        if hm:
            h3_text = hm.group(1).strip().strip("*")
            if h3_text and h3_text != current["h2"]:
                current["h3s"].append(h3_text)

    if current:
        sections.append(current)

    sections = [s for s in sections if s["h2"] and len(s["h2"]) > 3 and
                 not any(p.match(s["h2"]) for p in SKIP)]
    return sections


def split_sections_by_type(sections: list):
    """Separate body vs FAQ vs Conclusion."""
    body, faq, conclusion = [], None, None
    for s in sections:
        h2l = s["h2"].lower()
        if re.search(r"faq|frequent|questions?$", h2l) and faq is None:
            faq = s
        elif re.search(r"conclu|final\s*thought|wrap|next\s*step", h2l) and conclusion is None:
            conclusion = s
        else:
            body.append(s)
    return body, faq, conclusion

# ═══════════════════════════════════════════════════════════════════


def main():
    print("\n" + "="*65)
    print("✍️  BLOG WRITER (REVISED — AEO/GEO/YMYL/Locale)")
    print("="*65)

    sp = gc.open(SPREADSHEET_NAME)
    plan = load_blog_plan(sp)

    if not plan["outline"]:
        print("❌ No outline found. Run upstream cells first.")
        return

    print(f"  📌 H1       : {plan['chosen_h1'][:60]}")
    print(f"  🌍 Countries: {plan['target_countries']}")
    print(f"  🎯 Personas : {len(plan['personas'])} loaded")
    print(f"  🔑 Keywords : {len(plan['keyword_pool'])} total ({len(plan['keyword_by_category'])} categories)")

    # NOTE: The full section-by-section loop from v15 Cell 33 lines 789-1151
    # continues here with the same structure, but each section prompt now uses
    # the new build_section_prompt() above with all AEO/GEO/YMYL/voice rules.
    #
    # After generating all sections, the revised pipeline:
    #   1. Extracts Speakable candidates → writes to Speakable_Candidates tab
    #   2. Extracts citations → writes to Citation_List tab (dev team verifies)
    #   3. Strips [SPEAKABLE] tags from the final blog
    #   4. Writes the cleaned blog to Google Doc (same as v15)
    #   5. Hands off to the blog-grade-reducer skill for Class 7-8 reduction
    #
    # The v15 post-hoc banned_phrase dict and make_unique_hook() are REMOVED.
    # Those behaviors are now pushed into the section prompt via the voice brief.

# Parse the v15 Cell 27 outline into structured sections
    all_sections = parse_v15_outline_to_sections(plan["outline"])
    body_sections, faq_section, conclusion_section = split_sections_by_type(all_sections)

    print(f"  📋 Parsed outline: {len(all_sections)} total | "
          f"body: {len(body_sections)} | FAQ: {'✅' if faq_section else '❌'} | "
          f"conc: {'✅' if conclusion_section else '❌'}")

    if not body_sections:
        print("  ❌ No body sections parsed — outline format may be unexpected")
        print("     Outline preview:")
        print("     " + plan["outline"][:300].replace("\n", "\n     "))
        return

    # Section-by-section generation loop
    blog_parts = [f"# {plan['chosen_h1']}\n"]
    running_wc = 0

    for idx, section in enumerate(body_sections):
        if running_wc >= HARD_CAP_WORDS - 200:
            print(f"  ⚠️ Word cap reached at section {idx+1}")
            break

        stype = classify_section_type(section["h2"])
        kws = get_keywords_for_section(stype, plan["keyword_by_category"])

        print(f"\n  ▶ [{idx+1}/{len(body_sections)}] {section['h2'][:60]}")

        prompt = build_section_prompt(
            section, kws, plan, "", running_wc,
            HARD_CAP_WORDS - running_wc,
        )
        sec_text = call_gemini(prompt, max_tokens=MAX_TOKENS)
        if not sec_text:
            print("    ⚠️ Empty response — skipping")
            continue

        sec_wc = count_words(sec_text)
        running_wc += sec_wc
        blog_parts.append(sec_text + "\n")
        print(f"    ✅ {sec_wc}w (total: {running_wc})")
        time.sleep(3)


        # FAQ + Conclusion generation
    if faq_section and plan.get("faq_answers"):
        print("\n  ▶ Generating FAQ section...")

        faq_prompt = build_faq_prompt(
            faq_section.get("faq_questions", []) if isinstance(faq_section, dict) else [],
            plan["keyword_pool"][:60],
            HARD_CAP_WORDS - running_wc
        )

        faq_text = call_gemini(faq_prompt, max_tokens=MAX_TOKENS)

        if faq_text:
            faq_wc = count_words(faq_text)
            running_wc += faq_wc
            blog_parts.append(faq_text + "\n")
            print(f"    ✅ FAQ added ({faq_wc}w)")
        else:
            print("    ⚠️ FAQ generation failed")



    # Assemble full blog
    full_blog = "\n".join(blog_parts)

    # Extract Speakable candidates and citations
    speakables = extract_speakable_candidates(full_blog)
    citations = extract_citations(full_blog)
    full_blog = clean_speakable_tags(full_blog)

    print(f"\n  ✅ Blog complete: {count_words(full_blog)} words")
    print(f"     Speakable candidates: {len(speakables)}")
    print(f"     Citations to verify: {len(citations)}")

    # Write main blog output
    out_ws = get_or_create_tab(sp, OUTPUT_TAB, rows=20, cols=6)
    HEADERS = ["H1", "Word_Count", "Primary_Keyword", "Target_Countries", "Full_Blog_Markdown", "Generated_At"]
    from datetime import datetime
    rows = [HEADERS, [
        plan["chosen_h1"], count_words(full_blog), plan["primary_keyword"],
        plan["target_countries"],
        full_blog if len(full_blog) <= MAX_CELL else full_blog[:MAX_CELL] + "\n[TRUNCATED]",
        datetime.now().isoformat(timespec="seconds"),
    ]]
    out_ws.update(rows, value_input_option="RAW")

    # Write Speakable candidates for dev team
    if speakables:
        sp_ws = get_or_create_tab(sp, SPEAKABLE_TAB, rows=max(len(speakables)+5, 20), cols=3)
        sp_rows = [["Sentence", "CSS_Class_Suggested", "Word_Count"]]
        for i, s in enumerate(speakables, 1):
            sp_rows.append([s, f"speakable-{i}", len(s.split())])
        sp_ws.update(sp_rows, value_input_option="RAW")

    # Write citations for SEO team verification
    if citations:
        cit_ws = get_or_create_tab(sp, CITATIONS_TAB, rows=max(len(citations)+5, 20), cols=3)
        cit_rows = [["Citation_Text", "Verified_Y_N", "Notes"]]
        for c in citations:
            cit_rows.append([c, "", ""])
        cit_ws.update(cit_rows, value_input_option="RAW")


    print(f"\n📄 Writing to Google Doc '{DOC_TITLE}'...")

    doc_id = _get_or_create_doc(DOC_TITLE)
    doc_url = write_markdown_to_doc(doc_id, full_blog, DOC_TITLE)

    print(f"\n  📝 Outputs:")
    print(f"     Doc URL  : {doc_url}")
    print(f"     Blog   → '{OUTPUT_TAB}' sheet")
    print(f"     Voice  → '{SPEAKABLE_TAB}' sheet ({len(speakables)} candidates)")
    print(f"     Cites  → '{CITATIONS_TAB}' sheet ({len(citations)} to verify)")


if __name__ == "__main__":
    main()
