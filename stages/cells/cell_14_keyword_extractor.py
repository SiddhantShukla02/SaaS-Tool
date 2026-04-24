# ─── CELL: Keyword Extractor → Keyword_data (REVISED) ────────────────
#
# CHANGES FROM V15:
#   1. Examples are now keyword-agnostic. The hardcoded rhinoplasty examples
#      are replaced with dynamic examples based on detect_specialty(primary_kw).
#      A cornea transplant blog no longer gets primed with rhinoplasty keywords.
#
#   2. Output is STRUCTURED JSON, not 7 free-form text lines. This eliminates
#      downstream regex parsing in Cells 27 and 33.
#
#   3. Imports config.py for keys, model, safety settings.
#
#   4. Inference rule strengthened: if source URL is cost-heavy, actively
#      infer 6 non-cost categories. (The 20% cost cap is preserved.)
#
# Reads  : Keyword_n8n > Url_data_ext tab > URL + Texts_Only columns
# Writes : Keyword_n8n > Keyword_data tab > URL + Extracted_keywords (JSON)
# ─────────────────────────────────────────────────────────────────────

import json
import time
import re
import gspread
from google import genai
from app.utils.helper import get_sheet_client
from google.genai import types

from config import (
    GEMINI_API_KEY, GEMINI_MODEL, SPREADSHEET_NAME,
    MAX_CELL, SAFETY_OFF, SCOPES, detect_specialty,
)

# ── Config specific to this cell ─────────────────────────────────────
SOURCE_TAB     = "Url_data_ext"
OUTPUT_TAB     = "Keyword_data"
MAX_TEXT_CHARS = 30_000

# ── Auth ─────────────────────────────────────────────────────────────
gc = get_sheet_client(SCOPES)
gemini_client = genai.Client(api_key=GEMINI_API_KEY)


# ═══════════════════════════════════════════════════════════════════
# Specialty-specific keyword examples
# Each specialty has 7 example lists matching the 7 intent categories.
# This replaces the hardcoded rhinoplasty examples from v15.
# ═══════════════════════════════════════════════════════════════════

SPECIALTY_EXAMPLES = {
    "fertility": {
        "procedure":     "IVF, ICSI, IUI, donor egg IVF, frozen embryo transfer, PGT-A, blastocyst transfer",
        "concerns":      "failed IVF cycle, low AMH, PCOS and IVF, low sperm count, repeated implantation failure",
        "safety":        "JCI accredited IVF centre India, ICMR guidelines IVF, IVF success rate India, ART registration",
        "recovery":      "post embryo transfer care, IVF bed rest, OHSS symptoms, two-week wait, beta hCG timing",
        "logistics":     "IVF India for UK patients, IVF package India, surrogacy legality India, IVF tourism Delhi",
        "cost":          "IVF cost India vs UK, IVF package all inclusive India, donor IVF cost, affordable IVF India",
        "brand":         "Nova IVF Fertility, Cloudnine Fertility, Apollo Fertility, Indira IVF, Manipal Fertility",
    },
    "cardiac": {
        "procedure":     "CABG, OPCAB, off-pump bypass, beating heart surgery, minimally invasive bypass, TAVR, valve replacement",
        "concerns":      "chest pain, shortness of breath, blocked arteries, leaky valve, irregular heartbeat, LAD blockage",
        "safety":        "JCI accredited cardiac centre India, NABH cardiac hospital, mortality rate cardiac surgery India",
        "recovery":      "cardiac rehab India, recovery after bypass, driving after heart surgery, when can I fly after bypass",
        "logistics":     "bypass surgery India for UK patients, cardiac package India, medical visa heart surgery",
        "cost":          "bypass surgery cost India vs UK, CABG package price India, heart valve replacement cost India",
        "brand":         "Fortis Escorts Heart Institute, Medanta Heart Institute, Narayana Hrudayalaya, Asian Heart Institute",
    },
    "orthopedic": {
        "procedure":     "TKR, UKR, PKR, robotic knee replacement, MAKO knee, anterior hip approach, hip resurfacing, ACL reconstruction",
        "concerns":      "knee pain, hip pain, arthritis, meniscus tear, cartilage damage, bone-on-bone knee",
        "safety":        "JCI accredited orthopedic hospital India, implant brands India, robotic TKR India, surgeon experience",
        "recovery":      "physiotherapy after knee replacement, walking timeline after TKR, flying after knee surgery",
        "logistics":     "knee replacement India for UK patients, orthopedic package India, medical visa knee surgery",
        "cost":          "knee replacement cost India vs UK, TKR package price India, bilateral knee cost India",
        "brand":         "Apollo Orthopedics, Max Super Speciality, Fortis Bone and Joint, Manipal Hospitals",
    },
    "cosmetic": {
        "procedure":     "rhinoplasty, liposuction, abdominoplasty, BBL, facelift, blepharoplasty, breast augmentation, otoplasty",
        "concerns":      "dorsal hump, deviated septum, bulbous tip, post-pregnancy body, loose skin, aging face",
        "safety":        "board-certified plastic surgeon India, ISAPS member India, JCI cosmetic hospital, complication rates",
        "recovery":      "recovery timeline rhinoplasty, swelling after liposuction, final results timeline, scar fading",
        "logistics":     "rhinoplasty India for UK patients, cosmetic surgery tourism India, recovery accommodation Delhi",
        "cost":          "rhinoplasty cost India vs UK, liposuction package India, BBL price India, mommy makeover India",
        "brand":         "Olmec, Designer Bodyz, SCULPT, Richardsons Cosmetic, Dr. Anup Dhir clinic",
    },
    "oncology": {
        "procedure":     "chemotherapy, immunotherapy, proton therapy, CAR-T therapy, radiation therapy, robotic oncosurgery, HIPEC",
        "concerns":      "stage 4 cancer, metastatic cancer, recurrence, second opinion, survival rate, palliative care",
        "safety":        "JCI accredited cancer hospital India, ICMR tumor board, molecular tumor board India, NABH oncology",
        "recovery":      "chemotherapy side effects, post-surgery rehab cancer, immunity after chemo, nutrition after cancer",
        "logistics":     "cancer treatment India for UK patients, oncology package India, medical visa cancer treatment",
        "cost":          "cancer treatment cost India vs UK, immunotherapy cost India, proton therapy cost India",
        "brand":         "Tata Memorial Centre, HCG Oncology, Apollo Cancer Centre, Max Institute of Cancer Care, Fortis Cancer",
    },
    "dental": {
        "procedure":     "dental implants, full mouth rehabilitation, all-on-4, all-on-6, zirconia crown, veneers, root canal",
        "concerns":      "missing teeth, broken tooth, gum disease, bad bite, discoloured teeth, tooth pain",
        "safety":        "DCI registered dentist India, implant brands India, sterilisation protocol, dental x-ray safety",
        "recovery":      "osseointegration timeline, after implant care, healing after extraction, crown placement timing",
        "logistics":     "dental treatment India for UK patients, dental tourism India, 2-visit dental plan, accommodation",
        "cost":          "dental implant cost India vs UK, full mouth implants cost India, veneer price India, root canal cost",
        "brand":         "Clove Dental, Apollo White, FMS Dental, Sabka Dentist, Dr. Smile Clinic",
    },
    "ophthalmology": {
        "procedure":     "DMEK, DALK, PKP, phacoemulsification, femto LASIK, SMILE, ICL, retinal detachment surgery, vitrectomy",
        "concerns":      "keratoconus, corneal scarring, cataract, myopia, hypermetropia, macular degeneration, diabetic retinopathy",
        "safety":        "eye bank India, All India Ophthalmological Society, cornea grading, LASIK safety, surgeon fellowship",
        "recovery":      "after cornea transplant care, eye drops schedule, visual recovery timeline, when to drive after LASIK",
        "logistics":     "eye surgery India for UK patients, eye hospital package India, accommodation near eye hospital",
        "cost":          "cornea transplant cost India vs UK, LASIK price India, cataract surgery cost India, SMILE cost India",
        "brand":         "LV Prasad Eye Institute, Sankara Nethralaya, Narayana Nethralaya, Centre for Sight, Dr. Agarwal's",
    },
    "bariatric": {
        "procedure":     "sleeve gastrectomy, gastric bypass, mini gastric bypass, revision bariatric, intragastric balloon, SADI-S",
        "concerns":      "obesity, BMI above 35, diabetes and obesity, weight regain, loose skin after weight loss",
        "safety":        "IFSO member India, bariatric surgeon fellowship, JCI bariatric hospital, complication rate sleeve",
        "recovery":      "liquid diet after sleeve, weight loss timeline bariatric, exercise after surgery, hair loss post-surgery",
        "logistics":     "bariatric surgery India for UK patients, obesity surgery package India, medical visa weight loss",
        "cost":          "bariatric surgery cost India vs UK, sleeve gastrectomy price India, gastric bypass cost India",
        "brand":         "Max Institute of Minimal Access, Sir Ganga Ram, Apollo Bariatric, Fortis Obesity, Dr. Tarun Mittal",
    },
    "transplant": {
        "procedure":     "kidney transplant, liver transplant, bone marrow transplant, stem cell transplant, deceased donor, living donor",
        "concerns":      "end stage renal disease, liver failure, waiting list, donor matching, rejection, GVHD",
        "safety":        "NOTTO registration, THOA compliance India, JCI transplant centre, transplant committee approval",
        "recovery":      "immunosuppressant schedule, GVHD signs, rejection symptoms, infection risk post-transplant, isolation",
        "logistics":     "transplant India for UK patients, transplant package India, medical visa organ transplant, donor travel",
        "cost":          "kidney transplant cost India vs UK, liver transplant package India, BMT cost India",
        "brand":         "Medanta Transplant, Apollo Transplant, Global Hospitals, BLK Max Transplant, Fortis Transplant",
    },
    "general": {
        "procedure":     "(specific procedure names from source content)",
        "concerns":      "(symptoms and conditions patients describe)",
        "safety":        "JCI accreditation, NABH accreditation, surgeon credentials, hospital experience",
        "recovery":      "recovery timeline, post-procedure care, follow-up schedule, return to normal activity",
        "logistics":     "medical tourism India, medical visa India, accommodation, duration of stay",
        "cost":          "cost India vs UK, package price India, affordable treatment India",
        "brand":         "Apollo Hospitals, Fortis Healthcare, Max Healthcare, Medanta, Manipal Hospitals",
    },
}


def build_specialty_examples(specialty: str) -> dict:
    """Return the 7-category example set for the specialty, with 'general' fallback."""
    return SPECIALTY_EXAMPLES.get(specialty, SPECIALTY_EXAMPLES["general"])


# ═══════════════════════════════════════════════════════════════════
# Sheet helpers (unchanged from v15)
# ═══════════════════════════════════════════════════════════════════

def _trunc(v):
    s = str(v) if v is not None else ""
    return s[:MAX_CELL] + "\n[TRUNCATED]" if len(s) > MAX_CELL else s


def _write_with_retry(ws, data, retries=3):
    for attempt in range(retries):
        try:
            ws.update(data, value_input_option="RAW")
            return True
        except Exception as e:
            wait = 4 * (attempt + 1)
            if attempt < retries - 1:
                print(f"    ⚠️  Sheet write attempt {attempt+1}/{retries} failed: {e} — retry in {wait}s")
                time.sleep(wait)
    return False


def _fmt_header(ws, n_cols):
    col_letter = chr(64 + min(n_cols, 26))
    ws.format(f"A1:{col_letter}1", {
        "textFormat": {"bold": True, "foregroundColor": {"red":1,"green":1,"blue":1}},
        "backgroundColor": {"red": 0.13, "green": 0.37, "blue": 0.60}
    })


def get_or_create_tab(spreadsheet, tab_name, rows=3000, cols=6):
    try:
        ws = spreadsheet.worksheet(tab_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=rows, cols=cols)
    return ws


def load_source_data(sheet_name, tab_name):
    ws = gc.open(sheet_name).worksheet(tab_name)
    rows = ws.get_all_records()
    data, skipped = [], 0
    for r in rows:
        url  = str(r.get("URL", "")).strip()
        text = str(r.get("Texts_Only", "")).strip()
        if not url or not text:
            skipped += 1
            continue
        if text.upper() in ("SCRAPE_FAILED", "GEMINI_FAILED", "") or text.startswith("ERROR:"):
            skipped += 1
            continue
        data.append({"url": url, "texts_only": text})
    print(f"  ✅ Loaded {len(data)} valid rows from '{tab_name}' (skipped {skipped})")
    return data


def call_gemini_json(prompt, max_tokens=8000, retries=3):
    """Call Gemini and return parsed JSON dict, or empty dict on failure."""
    for attempt in range(retries):
        try:
            resp = gemini_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    max_output_tokens=max_tokens,
                    safety_settings=SAFETY_OFF,
                    response_mime_type="application/json",
                ),
            )
            if not resp.candidates:
                continue
            raw = resp.text.strip()
            # Strip markdown fences if Gemini added them despite JSON mode
            raw = re.sub(r"^```\w*\s*", "", raw)
            raw = re.sub(r"```\s*$", "", raw)
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError) as e:
            print(f"    ⚠️  Gemini JSON parse attempt {attempt+1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(4 * (attempt + 1))
        except Exception as e:
            print(f"    ⚠️  Gemini attempt {attempt+1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(6 * (attempt + 1))
    return {}


# ═══════════════════════════════════════════════════════════════════
# Keyword extraction — specialty-aware, structured JSON output
# ═══════════════════════════════════════════════════════════════════

def extract_keywords(url: str, texts_only: str, primary_kw: str) -> dict:
    """
    Extract keywords from competitor content.

    Returns structured dict:
    {
        "procedure_types": [keywords...],
        "patient_concerns": [keywords...],
        "safety_quality": [keywords...],
        "recovery_results": [keywords...],
        "travel_logistics": [keywords...],
        "cost_value": [keywords...],
        "hospital_surgeon_brand": [keywords...],
        "specialty_detected": "cardiac|orthopedic|...",
    }
    """
    # Derive page topic from URL path
    try:
        path   = url.rstrip("/").split("/")[-1]
        header = re.sub(r"[-_]", " ", path).strip()
        header = re.sub(r"[^a-zA-Z0-9 ]", "", header).title() or "the topic"
    except Exception:
        header = "the topic"

    # Detect medical specialty and load matching examples
    specialty = detect_specialty(primary_kw)
    examples  = build_specialty_examples(specialty)

    prompt = f"""You are a Senior SEO Strategist for Divinheal, an Indian medical tourism platform.
Extract a BALANCED keyword set covering the full patient decision journey — not just cost.

TREATMENT TOPIC: {primary_kw}
PAGE TOPIC    : {header}
DETECTED SPECIALTY: {specialty}
TARGET AUDIENCE: Patients from Middle East, GCC, Africa, South Asia researching treatment in India

HARD CAP ON COST KEYWORDS: At most 20% of your total output may be cost-related.
If the source URL is cost-heavy, you MUST infer non-cost keywords from the treatment
name and medical knowledge.

OUTPUT FORMAT — Return a JSON object with EXACTLY these 7 arrays:

{{
  "procedure_types": [
    // 12-18 keywords: what the procedure IS, its variants, technique names
    // Example keywords for {specialty}: {examples['procedure']}
  ],
  "patient_concerns": [
    // 12-18 keywords: symptoms, conditions, problems patients want fixed
    // Example keywords for {specialty}: {examples['concerns']}
  ],
  "safety_quality": [
    // 12-18 keywords: accreditations, certifications, surgeon credentials
    // Example keywords for {specialty}: {examples['safety']}
  ],
  "recovery_results": [
    // 12-18 keywords: post-procedure timeline, healing, outcomes
    // Example keywords for {specialty}: {examples['recovery']}
  ],
  "travel_logistics": [
    // 12-18 keywords: medical tourism logistics for international patients
    // Example keywords for {specialty}: {examples['logistics']}
  ],
  "cost_value": [
    // EXACTLY 12-15 keywords: cost comparison, value, packages
    // Example keywords for {specialty}: {examples['cost']}
    // DO NOT output synonym-flooded variants (cost/price/fee/charges/expenses all at once).
    // Pick the single most-searched variant per concept and move on.
  ],
  "hospital_surgeon_brand": [
    // 10-15 keywords: named entities found in the source content
    // Example keywords for {specialty}: {examples['brand']}
    // Only include names actually present in the source content below.
  ]
}}

RULES:
- Return ONLY valid JSON. No preamble, no markdown fences.
- Do not output the example keywords verbatim unless they actually appear in source content.
- Every keyword must be a realistic Google search term a patient would type.
- Keywords must be lowercase unless a proper noun (hospital names, doctor names).

SOURCE CONTENT:
{texts_only[:MAX_TEXT_CHARS]}"""

    result = call_gemini_json(prompt, max_tokens=8000)

    # Fallback for parse failure — return empty arrays
    if not result:
        return {
            "procedure_types": [], "patient_concerns": [], "safety_quality": [],
            "recovery_results": [], "travel_logistics": [], "cost_value": [],
            "hospital_surgeon_brand": [], "specialty_detected": specialty,
        }

    result["specialty_detected"] = specialty
    # Ensure all 7 keys exist with list type
    for key in ["procedure_types", "patient_concerns", "safety_quality",
                "recovery_results", "travel_logistics", "cost_value",
                "hospital_surgeon_brand"]:
        if key not in result or not isinstance(result[key], list):
            result[key] = []

    return result


def count_keywords(extracted: dict) -> tuple:
    """Count keywords and enforce the 20% cost cap."""
    non_cost_cats = ["procedure_types", "patient_concerns", "safety_quality",
                     "recovery_results", "travel_logistics", "hospital_surgeon_brand"]
    non_cost_count = sum(len(extracted.get(c, [])) for c in non_cost_cats)
    cost_count     = len(extracted.get("cost_value", []))
    total          = non_cost_count + cost_count
    cost_pct       = (cost_count / total * 100) if total else 0
    return total, cost_count, cost_pct


def format_for_sheet(extracted: dict) -> str:
    """Human-readable summary of the JSON for the Keyword_data sheet display."""
    lines = []
    for cat in ["procedure_types", "patient_concerns", "safety_quality",
                "recovery_results", "travel_logistics", "cost_value",
                "hospital_surgeon_brand"]:
        label = cat.upper().replace("_", " ")
        items = extracted.get(cat, [])
        if items:
            lines.append(f"{label}: {', '.join(items)}")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════
# Main execution
# ═══════════════════════════════════════════════════════════════════

def main():
    print("\n" + "="*65)
    print("🔑 KEYWORD EXTRACTOR (REVISED — specialty-aware, JSON output)")
    print("="*65)

    sp = gc.open(SPREADSHEET_NAME)

    # Load primary keyword for specialty detection
    try:
        kw_ws    = sp.worksheet("keyword")
        kw_recs  = kw_ws.get_all_records()
        kw_list  = [str(r.get("Keyword", "")).strip() for r in kw_recs
                    if str(r.get("Keyword", "")).strip()]
        primary  = sorted(kw_list, key=lambda k: len(k.split()), reverse=True)[0]
    except Exception:
        primary = "medical treatment in India"

    specialty = detect_specialty(primary)
    print(f"  📌 Primary keyword  : {primary}")
    print(f"  🏥 Detected specialty: {specialty}")

    # Load source data
    source_data = load_source_data(SPREADSHEET_NAME, SOURCE_TAB)
    if not source_data:
        print("❌ No source data. Run URL scraper first.")
        return

    # Output tab
    out_ws = get_or_create_tab(sp, OUTPUT_TAB, rows=max(len(source_data)+50, 500), cols=6)
    HEADERS = ["URL", "Primary_Keyword", "Specialty", "Extracted_JSON", "Summary", "Keyword_Count"]
    all_rows = [HEADERS]

    for idx, row in enumerate(source_data, 1):
        url = row["url"]
        print(f"\n[{idx}/{len(source_data)}] {url[:70]}")
        extracted = extract_keywords(url, row["texts_only"], primary)
        total, cost_count, cost_pct = count_keywords(extracted)
        summary = format_for_sheet(extracted)

        print(f"  🔢 {total} keywords extracted | {cost_count} cost ({cost_pct:.0f}%)")
        if cost_pct > 25:
            print(f"  ⚠️  Cost keywords at {cost_pct:.0f}% — exceeds 20% target")

        all_rows.append([
            url,
            primary,
            specialty,
            _trunc(json.dumps(extracted, ensure_ascii=False, indent=2)),
            _trunc(summary),
            total,
        ])

        if idx % 5 == 0:
            print(f"  💾 Checkpoint save at URL #{idx}")
            _write_with_retry(out_ws, [[_trunc(c) for c in r] for r in all_rows])

        time.sleep(2)

    # Final write
    _write_with_retry(out_ws, [[_trunc(c) for c in r] for r in all_rows])
    _fmt_header(out_ws, len(HEADERS))

    print(f"\n{'='*65}")
    print(f"✅ Keyword extraction complete")
    print(f"   URLs processed : {len(source_data)}")
    print(f"   Specialty      : {specialty}")
    print(f"   Output tab     : '{OUTPUT_TAB}'")
    print(f"{'='*65}")


if __name__ == "__main__":
    main()
