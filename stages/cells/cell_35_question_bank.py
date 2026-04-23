# ─── CELL A: Question Bank Builder (NEW — Repurposing Pipeline stage 1) ─
#
# PURPOSE:
#   Read every question-shaped asset the pipeline has already collected
#   (PAA, autocomplete, related searches, forum insights, competitor FAQs)
#   and consolidate into one deduplicated, intent-tagged, priority-scored
#   sheet: Question_Bank.
#
# This is raw material for Cell B, which generates Quora/Reddit/Substack
# drafts. Nothing is published here — just data consolidation.
#
# Reads  : Keyword_n8n >
#            PAA, Other_Autocomplete, Related_search,
#            Forum_Master_Insights, Url_data_ext (FAQs column),
#            keyword (for country codes + primary keyword)
# Writes : Keyword_n8n > Question_Bank
# ─────────────────────────────────────────────────────────────────────


from stages.cells.cell_23_shared_utils import *

import re
import time
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

# Import from the two config files
from config import (
    CREDS_FILE, SPREADSHEET_NAME, SCOPES, MAX_CELL,
    COUNTRY_MAP, get_country_name, detect_specialty,
)
from config_repurpose import (
    REPURPOSE_TABS, QUESTION_BANK_FILTERS, QUESTION_PRIORITY_WEIGHTS,
)

# ── Auth ─────────────────────────────────────────────────────────────
creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
gc    = gspread.authorize(creds)

# ── Sheet helpers (reused pattern from v16 cells) ────────────────────
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


def get_or_create_tab(spreadsheet, tab_name, rows=1000, cols=12):
    try:
        ws = spreadsheet.worksheet(tab_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=rows, cols=cols)
    return ws


# ═══════════════════════════════════════════════════════════════════
# Source loaders — each returns list of dicts
# ═══════════════════════════════════════════════════════════════════

def load_paa(sp):
    try:
        ws = sp.worksheet("PAA")
        records = ws.get_all_records()
    except Exception:
        return []
    out = []
    for r in records:
        q = str(r.get("Question", "")).strip()
        kw = str(r.get("Keyword", "")).strip()
        cc = str(r.get("Country_Code", "")).strip().lower()
        if q:
            out.append({
                "question": q, "source": "paa",
                "source_keyword": kw, "country_code": cc,
            })
    return out


def load_autocomplete(sp):
    try:
        ws = sp.worksheet("Other_Autocomplete")
        records = ws.get_all_records()
    except Exception:
        return []
    out = []
    for r in records:
        s = str(r.get("Suggestion", "")).strip()
        kw = str(r.get("Keyword", "")).strip()
        cc = str(r.get("Country_Code", "")).strip().lower()
        if s and "?" not in s and len(s.split()) >= 3:
            # Reshape to question form
            q = _reshape_to_question(s)
            if q:
                out.append({
                    "question": q, "source": "autocomplete",
                    "source_keyword": kw, "country_code": cc,
                    "original_fragment": s,
                })
    return out


def load_related(sp):
    try:
        ws = sp.worksheet("Related_search")
        records = ws.get_all_records()
    except Exception:
        return []
    out = []
    for r in records:
        q = str(r.get("Related_Query", "")).strip()
        kw = str(r.get("Keyword", "")).strip()
        cc = str(r.get("Country_Code", "")).strip().lower()
        if q:
            reshaped = _reshape_to_question(q) if "?" not in q else q
            if reshaped:
                out.append({
                    "question": reshaped, "source": "related",
                    "source_keyword": kw, "country_code": cc,
                    "original_fragment": q,
                })
    return out


def load_forum_insights(sp):
    try:
        ws = sp.worksheet("Forum_Master_Insights")
        records = ws.get_all_records()
    except Exception:
        return []
    out = []
    for r in records:
        insight_type = str(r.get("Insight_Type", "")).strip()
        text = str(r.get("Clean_Insight", r.get("Insight_Text", ""))).strip()
        priority = r.get("Priority_Score", 1)
        country = str(r.get("Detected_Country", "")).strip().lower()

        # We care about Patient_Question, Objection, and Content_Gap
        if insight_type == "Patient_Question" and text:
            if "?" not in text:
                text = text.rstrip(".") + "?"
            out.append({
                "question": text, "source": "forum_question",
                "priority_hint": priority, "country_code": country,
            })
        elif insight_type == "Objection" and text:
            q = _objection_to_question(text)
            if q:
                out.append({
                    "question": q, "source": "forum_objection",
                    "priority_hint": priority, "country_code": country,
                    "original_objection": text,
                })
        elif insight_type == "Content_Gap" and text:
            q = _gap_to_question(text)
            if q:
                out.append({
                    "question": q, "source": "forum_gap",
                    "priority_hint": priority, "country_code": country,
                    "original_gap": text,
                })
    return out


def load_competitor_faqs(sp):
    try:
        ws = sp.worksheet("Url_data_ext")
        records = ws.get_all_records()
    except Exception:
        return []
    out = []
    for r in records:
        faq_blob = str(r.get("FAQs", "")).strip()
        url = str(r.get("URL", "")).strip()
        if not faq_blob or faq_blob == "nan":
            continue
        # Parse Q/A pairs
        pairs = re.findall(
            r"Q\d+\s*:\s*(.+?)\s*\nA\d+\s*:\s*(.+?)(?=\nQ\d+\s*:|$)",
            faq_blob, re.DOTALL,
        )
        for q, a in pairs:
            q = q.strip()
            a = a.strip()[:800]  # keep answer as reference
            if q and len(q) > 15:
                if "?" not in q:
                    q = q.rstrip(".") + "?"
                out.append({
                    "question": q, "source": "competitor_faq",
                    "competitor_url": url,
                    "competitor_answer_ref": a,
                })
    return out


def load_keyword_context(sp):
    """Return primary keyword, all keywords, target countries."""
    try:
        ws = sp.worksheet("keyword")
        records = ws.get_all_records()
    except Exception:
        return {"primary": "", "all_keywords": [], "countries": []}
    kws, ccs = [], set()
    for r in records:
        k = str(r.get("Keyword", "")).strip()
        c = str(r.get("Country_Code", "")).strip().lower()
        if k:
            kws.append(k)
        if c:
            ccs.add(c)
    primary = max(kws, key=lambda k: len(k.split())) if kws else ""
    return {"primary": primary, "all_keywords": kws, "countries": sorted(ccs)}


# ═══════════════════════════════════════════════════════════════════
# Transformation helpers — shape fragments into questions
# ═══════════════════════════════════════════════════════════════════

_Q_PREFIXES = ("how ", "what ", "why ", "when ", "where ", "which ",
                "is ", "are ", "can ", "do ", "does ", "should ")


def _reshape_to_question(text: str) -> str:
    """Convert autocomplete/related fragment into question form."""
    t = text.strip().lower().rstrip(".?!")
    if not t or len(t) < 8:
        return ""
    if any(t.startswith(p) for p in _Q_PREFIXES):
        return t.capitalize() + "?"
    # common patterns
    if " vs " in t or " versus " in t:
        return f"What's the difference between {t.replace(' vs ', ' and ').replace(' versus ', ' and ')}?"
    if t.startswith("best "):
        return f"What is the {t}?"
    if "cost" in t or "price" in t:
        return f"How much does {t} cost?".replace("cost cost", "cost")\
               .replace("price price", "price")
    if t.startswith("side effects"):
        return f"What are the {t}?"
    if t.startswith("recovery"):
        return f"How long is {t}?"
    # default
    return f"What do patients need to know about {t}?"


def _objection_to_question(objection: str) -> str:
    """Turn a forum objection into a FAQ-style question."""
    o = objection.strip().lower().rstrip(".?!")
    if not o or len(o) < 15:
        return ""
    # Typical objection patterns
    if "worried" in o or "concerned" in o or "afraid" in o:
        return f"Is it safe — {o[:100]}?"
    if "expensive" in o or "afford" in o:
        return f"Is the cost justified? {o[:120]}"
    if "hidden" in o or "extra" in o:
        return f"What about hidden or extra costs? {o[:120]}"
    if "quality" in o or "standard" in o:
        return f"Is the quality comparable? {o[:120]}"
    return f"A common patient concern: {o[:140]}?"


def _gap_to_question(gap: str) -> str:
    """Turn a content gap into an answerable question."""
    g = gap.strip().rstrip(".?!")
    if not g or len(g) < 15:
        return ""
    # Gaps are usually noun-phrases ("post-op rehab accommodation", "Amharic interpreters")
    return f"What should patients know about {g.lower()}?"


# ═══════════════════════════════════════════════════════════════════
# Normalisation + dedup
# ═══════════════════════════════════════════════════════════════════

def _normalize_q(q: str) -> str:
    q = re.sub(r"\s+", " ", q.strip().lower())
    q = re.sub(r"[^\w\s]", "", q)
    return q


def _jaccard(a: str, b: str) -> float:
    sa = set(a.split())
    sb = set(b.split())
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def _is_spam(q: str) -> bool:
    """Apply the regex filters from QUESTION_BANK_FILTERS."""
    for pattern in QUESTION_BANK_FILTERS["exclude_if_matches"]:
        if re.search(pattern, q, re.IGNORECASE):
            return True
    L = len(q)
    if L < QUESTION_BANK_FILTERS["min_question_length"]:
        return True
    if L > QUESTION_BANK_FILTERS["max_question_length"]:
        return True
    return False


def dedup_questions(questions: list) -> list:
    """
    Near-dedup using Jaccard similarity. Keeps the higher-priority
    source when two questions are near-duplicates.
    """
    threshold = QUESTION_BANK_FILTERS["dedup_similarity"]
    source_rank = {
        "forum_objection": 6, "forum_question": 5, "paa": 4,
        "competitor_faq": 3, "forum_gap": 2,
        "autocomplete": 1, "related": 0,
    }
    kept = []
    seen_norms = []
    # Sort by source priority first (so dedup keeps the best version)
    questions.sort(key=lambda x: -source_rank.get(x["source"], 0))
    for q in questions:
        if _is_spam(q["question"]):
            continue
        norm = _normalize_q(q["question"])
        if not norm:
            continue
        duplicate = False
        for prev_norm in seen_norms:
            if _jaccard(norm, prev_norm) >= threshold:
                duplicate = True
                break
        if not duplicate:
            seen_norms.append(norm)
            q["_norm"] = norm
            kept.append(q)
    return kept


# ═══════════════════════════════════════════════════════════════════
# Intent classification + priority scoring
# ═══════════════════════════════════════════════════════════════════

INTENT_PATTERNS = {
    "cost":     [r"\bcost\b", r"\bprice\b", r"\bfee\b", r"\bexpens",
                  r"\bafford", r"\bbudget\b", r"\bsaving", r"\bpackage\b"],
    "safety":   [r"\bsafe\b", r"\brisk\b", r"\bsuccess\s*rate", r"\bcomplicat",
                  r"\btrust", r"\baccredit", r"\bquality\b"],
    "how_to":   [r"^how\s+(to|do|does|long|much|many|can)\b"],
    "what":     [r"^what\b", r"\bdefinition\b", r"\bmean"],
    "compare":  [r"\b(vs|versus|compared|better|best|top)\b"],
    "logistics":[r"\bvisa\b", r"\bflight", r"\btravel", r"\bstay\b",
                  r"\baccommod", r"\binterpret"],
    "recovery": [r"\brecover", r"\bhealing", r"\bpost[-\s]?op", r"\bafter\s+surgery"],
    "procedure":[r"\bprocedure\b", r"\btechnique", r"\bmethod", r"\bstep"],
    "outcome":  [r"\bsuccess", r"\boutcome", r"\bresult", r"\bperman"],
}


def classify_intent(q: str) -> list:
    """Returns list of intent tags. A question may hit multiple."""
    q_lower = q.lower()
    tags = []
    for intent, patterns in INTENT_PATTERNS.items():
        if any(re.search(p, q_lower) for p in patterns):
            tags.append(intent)
    return tags if tags else ["general"]


def funnel_stage(intents: list) -> str:
    """TOFU / MOFU / BOFU based on intent combo."""
    if "cost" in intents or "compare" in intents:
        return "BOFU"
    if "safety" in intents or "logistics" in intents or "recovery" in intents:
        return "MOFU"
    if "what" in intents or "how_to" in intents:
        return "TOFU"
    return "MOFU"


def country_from_question(q: str, hint_cc: str = "") -> str:
    """Detect target country from question text; fall back to hint."""
    q_lower = q.lower()
    # Explicit country mentions
    for code, name in COUNTRY_MAP.items():
        if name.lower() in q_lower or f" {code} " in f" {q_lower} ":
            return code
    # Currency mentions
    currency_map = {"aed": "ae", "sar": "sa", "ngn": "ng", "bdt": "bd",
                     "gbp": "gb", "£": "gb", "aud": "au", "usd": "us"}
    for cur, cc in currency_map.items():
        if cur in q_lower:
            return cc
    # Institution mentions
    if "nhs" in q_lower:
        return "gb"
    if "medicare" in q_lower and ("australia" in q_lower or "aus" in q_lower):
        return "au"
    return hint_cc or ""


def priority_score(q: dict) -> float:
    """Compute 0-10 priority score."""
    score = 1.0
    source_weight = QUESTION_PRIORITY_WEIGHTS.get(
        f"source_{q['source']}", 1.0,
    )
    score += source_weight

    # Intent modifiers
    qt = q["question"].lower()
    if any(w in qt for w in ["cost", "price", "afford"]):
        score += QUESTION_PRIORITY_WEIGHTS["contains_cost"]
    if any(w in qt for w in ["safe", "risk", "success"]):
        score += QUESTION_PRIORITY_WEIGHTS["contains_safety"]
    if q.get("country_code"):
        score += QUESTION_PRIORITY_WEIGHTS["contains_country"]
    if any(w in qt for w in ["best", "top", "vs", "compare"]):
        score += QUESTION_PRIORITY_WEIGHTS["has_commercial_terms"]

    # Source-provided priority hints (e.g. Priority_Score from Forum_Master_Insights)
    if q.get("priority_hint"):
        try:
            score += float(q["priority_hint"]) * 0.5
        except (ValueError, TypeError):
            pass

    return round(min(score, 10.0), 2)


# ═══════════════════════════════════════════════════════════════════
# Main execution
# ═══════════════════════════════════════════════════════════════════

print("\n" + "═" * 65)
print("  QUESTION BANK BUILDER")
print("  Consolidates PAA + autocomplete + related + forum + comp-FAQ")
print("═" * 65)

sp = gc.open(SPREADSHEET_NAME)

# Load context
ctx = load_keyword_context(sp)
specialty = detect_specialty(ctx["primary"]) if ctx["primary"] else "general"
print(f"\n  📌 Primary keyword : {ctx['primary'] or '(unknown)'}")
print(f"  🏥 Specialty       : {specialty}")
print(f"  🌍 Countries       : {', '.join(ctx['countries']) or '(none)'}")

# Load all sources
print("\n  Loading sources...")
src_paa         = load_paa(sp)
src_autocomp    = load_autocomplete(sp)
src_related     = load_related(sp)
src_forum       = load_forum_insights(sp)
src_comp_faqs   = load_competitor_faqs(sp)

print(f"    PAA               : {len(src_paa)}")
print(f"    Autocomplete      : {len(src_autocomp)}")
print(f"    Related           : {len(src_related)}")
print(f"    Forum insights    : {len(src_forum)}")
print(f"    Competitor FAQs   : {len(src_comp_faqs)}")

all_questions = (
    src_paa + src_autocomp + src_related + src_forum + src_comp_faqs
)
print(f"\n  Total raw questions: {len(all_questions)}")

# Dedup
print("\n  Deduplicating (Jaccard similarity)...")
deduped = dedup_questions(all_questions)
print(f"    After dedup: {len(deduped)}")

# Classify + score
print("\n  Classifying intent + scoring priority...")
for q in deduped:
    q["intents"]     = classify_intent(q["question"])
    q["funnel"]      = funnel_stage(q["intents"])
    q["target_cc"]   = country_from_question(
        q["question"], q.get("country_code", "")
    )
    q["target_country"] = get_country_name(q["target_cc"]) if q["target_cc"] else ""
    q["priority"]    = priority_score(q)

# Sort by priority descending
deduped.sort(key=lambda x: -x["priority"])

# Build output rows
HEADERS = [
    "Row_ID", "Question", "Source", "Source_Keyword", "Original_Fragment",
    "Intents", "Funnel_Stage", "Target_Country_Code", "Target_Country",
    "Priority_Score", "Competitor_Answer_Ref", "Specialty", "Created_At",
]
rows_out = [HEADERS]
for i, q in enumerate(deduped, 1):
    rows_out.append([
        f"Q{i:04d}",
        q["question"],
        q["source"],
        q.get("source_keyword", ""),
        q.get("original_fragment", q.get("original_objection", q.get("original_gap", ""))),
        ", ".join(q["intents"]),
        q["funnel"],
        q["target_cc"],
        q["target_country"],
        q["priority"],
        _trunc(q.get("competitor_answer_ref", "")),
        specialty,
        datetime.now().isoformat(timespec="seconds"),
    ])

# Write to sheet
print(f"\n  Writing to '{REPURPOSE_TABS['question_bank']}'...")
out_ws = get_or_create_tab(
    sp, REPURPOSE_TABS["question_bank"],
    rows=max(len(rows_out) + 20, 500),
    cols=len(HEADERS),
)
_write_with_retry(out_ws, rows_out)
_fmt_header(out_ws, len(HEADERS))

# Summary by source
print(f"\n  ✅ Question Bank built: {len(deduped)} unique questions")
print(f"\n  Breakdown by source:")
from collections import Counter
source_counts = Counter(q["source"] for q in deduped)
for source, count in source_counts.most_common():
    print(f"    {source:<20}: {count}")

print(f"\n  Breakdown by funnel stage:")
funnel_counts = Counter(q["funnel"] for q in deduped)
for stage, count in sorted(funnel_counts.items()):
    print(f"    {stage:<10}: {count}")

print(f"\n  Top 5 priority questions:")
for q in deduped[:5]:
    print(f"    [{q['priority']}] {q['question'][:80]}")

print("\n" + "═" * 65)
print(f"  NEXT: Run Cell B (Platform Draft Generator)")
print("═" * 65)
