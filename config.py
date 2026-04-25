"""
═════════════════════════════════════════════════════════════════════
  config.py — Divinheal Content Pipeline Configuration
═════════════════════════════════════════════════════════════════════

  EVERYTHING your pipeline needs — keys, sheet names, model settings,
  country personas, specialty examples, source allowlists, partner
  hospitals, internal links — lives here. No other file should contain
  project-specific values.

  HOW TO USE:
    1. Fill in every block marked with  # ⚠️ FILL IN
    2. Blocks marked  # ✏️ OPTIONAL  can stay as-is if defaults work
    3. Blocks marked  # 🔒 DO NOT EDIT  are technical constants
    4. Save this file as `config.py` next to your notebook
    5. Run:  python config.py    (self-test — confirms everything loads)

  SECURITY:
    API keys load from environment variables only.
    NEVER paste keys directly into this file — use .env instead:
       SERP_API_KEY=xxxxx
       GEMINI_API_KEY=xxxxx
       FIRECRAWL_API_KEY=xxxxx
       BRAVE_API_KEY=xxxxx
    Then:  pip install python-dotenv
    And at notebook start:  from dotenv import load_dotenv; load_dotenv()

═════════════════════════════════════════════════════════════════════
"""

import os
from google.genai import types  # type: ignore


# ═════════════════════════════════════════════════════════════════════
# SECTION 1 — API KEYS  (loaded from .env — do not hardcode)
# ═════════════════════════════════════════════════════════════════════
# 🔒 DO NOT EDIT this loader. Set values in .env instead.

def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}\n"
            f"Set it in your .env file or run: export {name}='your_key_here'"
        )
    return value


SERP_API_KEY      = _require_env("SERP_API_KEY")
GEMINI_API_KEY    = _require_env("GEMINI_API_KEY")
FIRECRAWL_API_KEY = _require_env("FIRECRAWL_API_KEY")
BRAVE_API_KEY     = _require_env("BRAVE_API_KEY")


# ═════════════════════════════════════════════════════════════════════
# SECTION 2 — GOOGLE SHEETS + DRIVE + DOCS
# ═════════════════════════════════════════════════════════════════════
# ⚠️ FILL IN if yours differs

SPREADSHEET_NAME = os.environ.get("SPREADSHEET_NAME_GLOBAL") # Name of main google sheet
DOC_OUTPUT_TITLE = os.environ.get("FINAL_DOC_NAME") # Name of main google doc



# 🔒 DO NOT EDIT
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents",
]


# ═════════════════════════════════════════════════════════════════════
# SECTION 3 — SHEET TAB NAMES  (one per pipeline stage)
# ═════════════════════════════════════════════════════════════════════
# ✏️ OPTIONAL — change only if you've renamed tabs in your sheet

TABS = {
    # Inputs (you populate these)
    "keyword":               "keyword",                # Keyword + Country_Code rows
    "final_url":             "Final_Url",              # Curated competitor URLs

    # SERP harvesting outputs
    "serp_url":              "Serp_Url",
    "paa":                   "PAA",
    "autocomplete":          "Other_Autocomplete",
    "related_search":        "Related_search",

    # Competitor scraping outputs
    "url_data":              "Url_data_ext",
    "keyword_data":          "Keyword_data",

    # Forum intelligence outputs
    "reddit_insights":       "Reddit_Insights",
    "reddit_insights_md":    "Reddit_Insights_MD",
    "google_forum_insights": "Google_Forum_Insights",
    "forum_master_raw":      "Forum_Master_Raw",
    "forum_master_insights": "Forum_Master_Insights",
    "forum_master_md":       "Forum_Master_MD",

    # Blog generation outputs
    "h1_meta_output":        "H1_Meta_Output",
    "blog_outline":          "Blog_Outline",
    "empathy_faq_output":    "Empathy_FAQ_Output",
    "blog_output":           "Blog_Output",
    "speakable_candidates":  "Speakable_Candidates",
    "citation_list":         "Citation_List",
}


# ═════════════════════════════════════════════════════════════════════
# SECTION 4 — GEMINI MODEL + LIMITS
# ═════════════════════════════════════════════════════════════════════
# ✏️ OPTIONAL — raise limits if you hit truncation, lower for cost

GEMINI_MODEL     = "gemini-2.5-flash"   # Default model for all calls

MAX_CELL         = 49_000   # Google Sheets cell character limit buffer
MAX_SCRAPE_CHARS = 45_000   # Max chars per scraped page sent to Gemini
MAX_TOKENS       = 20_000    # Default Gemini output tokens
OUTLINE_TOKENS   = 25_000   # Outline cell needs more (thinking mode overhead)

# Temperature by task type (lower = more deterministic)
TEMP_KEYWORD_EXTRACT  = 0.3   # Structured extraction — deterministic
TEMP_OUTLINE          = 0.4   # Outline generation — mostly deterministic
TEMP_BLOG_SECTION     = 0.6   # Creative writing with guardrails
TEMP_FAQ_ANSWER       = 0.4   # Factual with warm tone
TEMP_H1_META          = 0.7   # Headlines need some creativity


# ═════════════════════════════════════════════════════════════════════
# SECTION 5 — BLOG GENERATION TARGETS
# ═════════════════════════════════════════════════════════════════════
# ⚠️ FILL IN based on your editorial standards

TARGET_WORDS_PER_SECTION = 450      # Per H2 body section
HARD_CAP_WORDS           = 5_500    # Full blog cap
TARGET_READING_GRADE     = 8        # Class 8 = ESL-friendly
MIN_STATS_PER_1000       = 5        # GEO/AEO stat density target
TARGET_FAQ_COUNT         = 12       # Sweet spot for FAQPage schema
MAX_SENTENCE_WORDS       = 30       # Hard ceiling (soft target: 18)


# ═════════════════════════════════════════════════════════════════════
# SECTION 6 — DIVINHEAL BRAND + BUSINESS
# ═════════════════════════════════════════════════════════════════════
# ⚠️ FILL IN — these show up throughout generated content

BRAND = {
    "name":            "Divinheal",
    "website":         "https://www.divinheal.com/",
    "contact_email":   "care@divinheal.com",                                # e.g. "care@divinheal.com"
    "contact_phone":   "+91 9327923184",                                # e.g. "+91-XXXX-XXXXXX"
    "whatsapp":        "+91 9327923184",                                # e.g. "+91-XXXX-XXXXXX"
    "tagline":         "Medical tourism to India, simplified",
    "founded_year":    "2020",                                # e.g. "2023"
    "jci_partnership": True,
    "nabh_partnership": True,
    "default_cta":     "Get a free consultation with Divinheal",
    "spelling":        "british",                         # "british" | "american"
    "oxford_comma":    True,
}


# ═════════════════════════════════════════════════════════════════════
# SECTION 7 — PARTNER HOSPITALS  (used for entity-explicit naming)
# ═════════════════════════════════════════════════════════════════════
# ⚠️ FILL IN — the writer uses these instead of saying "our partner hospital"
# Leave empty dict {} if you want the model to infer from competitor content


PARTNER_HOSPITALS = {

    "cardiac": [
        ("Artemis Hospital", "Gurugram"),
        ("Lokmanya Hospitals", "Pune"),
        ("Apollo Hospitals, Indore", "Indore"),
        ("MIOT International Chennai", "Chennai"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("SP Medifort Hospital Trivandrum", "Trivandrum"),
        ("Max Hospital", "Gurgaon"),
        ("Gleneagles Hospital, Parel", "Mumbai"),
        ("Gleneagles BGS Hospital, Kengeri", "Bengaluru"),
        ("Gleneagles Hospital, Lakdi ka pul", "Hyderabad"),
        ("Gleneagles Aware Hospital, LB Nagar", "Hyderabad"),
        ("Gleneagles Hospital, Perumbakkam", "Chennai"),
        ("Gleneagles Hospital Richmond", "Bengaluru"),
    ],

    "orthopedic": [
        # SPARSH is the anchor — primarily an ortho/spine/trauma chain
        ("SS SPARSH Hospital, RR Nagar", "Bengaluru"),
        ("Sparsh Hospital Bommasandra", "Bengaluru"),
        ("SPARSH Hospital Infantry Road", "Bengaluru"),
        ("Sparsh Super Speciality Hospital Yeshwanthpur", "Bengaluru"),
        ("SPARSH Hospital, Hennur Road", "Bengaluru"),
        ("SPARSH Hospital", "Bengaluru"),
        ("SPARSH Hospital, Hassan", "Hassan"),
        ("SPARSH Hospital, Sarjapur Road", "Bengaluru"),
        ("SSIMS-SPARSH Hospital Davanagere", "Davanagere"),
        # MIOT's name literally stands for Madras Institute of Orthopaedics & Traumatology
        ("MIOT International Chennai", "Chennai"),
        ("Artemis Hospital", "Gurugram"),
        ("Lokmanya Hospitals", "Pune"),
        ("Apollo Hospitals, Indore", "Indore"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("SP Medifort Hospital Trivandrum", "Trivandrum"),
        ("Gleneagles BGS Hospital, Kengeri", "Bengaluru"),
        ("Gleneagles Hospital, Parel", "Mumbai"),
        ("Gleneagles Hospital, Lakdi ka pul", "Hyderabad"),
        ("Gleneagles Aware Hospital, LB Nagar", "Hyderabad"),
        ("Gleneagles Hospital, Perumbakkam, Sholinganallur", "Chennai"),
        ("Gleneagles Hospital Richmond", "Bengaluru"),
    ],

    "oncology": [
        # GEM is a dedicated cancer centre
        ("GEM Cancer Centre", "Chennai"),
        ("Artemis Hospital", "Gurugram"),
        ("Apollo Hospitals, Indore", "Indore"),
        ("MIOT International Chennai", "Chennai"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("SP Medifort Hospital Trivandrum", "Trivandrum"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("Gleneagles Hospital, Parel", "Mumbai"),
        ("Gleneagles BGS Hospital, Kengeri", "Bengaluru"),
        ("Gleneagles Hospital, Lakdi ka pul", "Hyderabad"),
        ("Gleneagles Aware Hospital, LB Nagar", "Hyderabad"),
        ("Gleneagles Hospital, Perumbakkam, Sholinganallur", "Chennai"),
        ("Gleneagles Hospital Richmond", "Bengaluru"),
    ],

    "hematology": [
        # GEM Cancer Centre covers blood cancers; large multi-specialty hospitals have haematology depts
        ("GEM Cancer Centre", "Chennai"),
        ("Artemis Hospital", "Gurugram"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("Gleneagles Hospitals", "Chennai"),
        ("Apollo Hospitals, Indore", "Indore"),
    ],

    "neuro": [
        # IBS Hospital is specifically brain & spine
        ("Institute of Brain and Spine (IBS Hospital)", "Bengaluru"),
        ("Artemis Hospital", "Gurugram"),
        ("Lokmanya Hospitals", "Pune"),
        ("MIOT International Chennai", "Chennai"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("SP Medifort Hospital Trivandrum", "Trivandrum"),
        ("Apollo Hospitals, Indore", "Indore"),
        ("Gleneagles Hospital, Parel", "Mumbai"),
        ("Gleneagles BGS Hospital, Kengeri", "Bengaluru"),
        ("Gleneagles Hospital, Lakdi ka pul", "Hyderabad"),
        ("Gleneagles Aware Hospital, LB Nagar", "Hyderabad"),
        ("Gleneagles Hospital, Perumbakkam, Sholinganallur", "Chennai"),
        ("Gleneagles Hospital Richmond", "Bengaluru"),
    ],

    "paediatric_neurology": [
        ("Institute of Brain and Spine (IBS Hospital)", "Bengaluru"),
        ("Artemis Hospital", "Gurugram"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("Apollo Hospitals, Indore", "Indore"),
        ("Gleneagles Hospitals", "Multiple Cities"),
    ],

    "paediatric": [
        # SPARSH Women and Children is a dedicated paediatric + women's hospital
        ("SPARSH Women and Children Hospital", "Bengaluru"),
        ("Artemis Hospital", "Gurugram"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("Apollo Hospitals, Indore", "Indore"),
        ("SP Medifort Hospital Trivandrum", "Trivandrum"),
        ("Gleneagles Hospitals", "Chennai"),
        ("Gleneagles Hospital, Parel", "Mumbai"),
    ],

    "fertility_ivf": [
        # World IVF Centre is a dedicated fertility clinic
        ("World IVF Centre", "Bengaluru"),
        ("Gleneagles Hospitals", "Chennai"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("Artemis Hospital", "Gurugram"),
        ("Pride IVF", "New Delhi"),
        ("Indira IVF", "New Delhi"),
        ("Apollo Hospitals", "Hyderabad"),
    ],

    "gynecology": [
        ("SPARSH Women and Children Hospital", "Bengaluru"),
        ("Lokmanya Hospitals", "Pune"),
        ("Apollo Hospitals, Indore", "Indore"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("SP Medifort Hospital Trivandrum", "Trivandrum"),
        ("Artemis Hospital", "Gurugram"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("Gleneagles Hospital, Parel", "Mumbai"),
        ("Gleneagles Hospital, Perumbakkam, Sholinganallur", "Chennai"),
        ("Gleneagles Hospital Richmond", "Bengaluru"),
    ],

    "obstetrics": [
        ("SPARSH Women and Children Hospital", "Bengaluru"),
        ("Lokmanya Hospitals", "Pune"),
        ("White Lotus Hospital", "Bengaluru"),   # conservative inclusion — common for women's care
        ("Apollo Hospitals, Indore", "Indore"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("SP Medifort Hospital Trivandrum", "Trivandrum"),
        ("Gleneagles Hospitals", "Multiple Cities"),
    ],

    "organ_transplant": [
        ("Artemis Hospital", "Gurugram"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("Gleneagles BGS Hospital, Kengeri", "Bengaluru"),  # Fortis network — known for transplant
        ("Gleneagles Hospital, Lakdi ka pul", "Hyderabad"),
        ("Gleneagles Hospital, Parel", "Mumbai"),
        ("SP Medifort Hospital Trivandrum", "Trivandrum"),
        ("Apollo Hospitals, Indore", "Indore"),
    ],

    "nephrology": [
        ("Artemis Hospital", "Gurugram"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("Apollo Hospitals, Indore", "Indore"),
        ("SP Medifort Hospital Trivandrum", "Trivandrum"),
        ("Gleneagles Hospitals", "Multiple Cities"),
        ("Gleneagles Hospital, Lakdi ka pul", "Hyderabad"),
        ("Gleneagles Aware Hospital, LB Nagar", "Hyderabad"),
        ("Gleneagles Hospital, Perumbakkam, Sholinganallur", "Chennai"),
    ],

    "urology": [
        ("Artemis Hospital", "Gurugram"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("MIOT International Chennai", "Chennai"),
        ("Apollo Hospitals, Indore", "Indore"),
        ("SP Medifort Hospital Trivandrum", "Trivandrum"),
        ("Lokmanya Hospitals", "Pune"),
        ("Gleneagles Hospitals", "Multiple Cities"),
        ("Gleneagles Hospital, Parel", "Mumbai"),
        ("Gleneagles BGS Hospital, Kengeri", "Bengaluru"),
    ],

    "gastroenterology": [
        ("Artemis Hospital", "Gurugram"),
        ("Lokmanya Hospitals", "Pune"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("Apollo Hospitals, Indore", "Indore"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("MIOT International Chennai", "Chennai"),
        ("SP Medifort Hospital Trivandrum", "Trivandrum"),
        ("Gleneagles Hospitals", "Multiple Cities"),
        ("Gleneagles Hospital, Parel", "Mumbai"),
        ("Gleneagles Hospital, Lakdi ka pul", "Hyderabad"),
        ("Gleneagles Aware Hospital, LB Nagar", "Hyderabad"),
        ("Gleneagles Hospital, Perumbakkam, Sholinganallur", "Chennai"),
        ("Gleneagles Hospital Richmond", "Bengaluru"),
    ],

    "ent": [
        ("Artemis Hospital", "Gurugram"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("Apollo Hospitals, Indore", "Indore"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("SP Medifort Hospital Trivandrum", "Trivandrum"),
        ("Gleneagles Hospitals", "Multiple Cities"),
        ("Gleneagles Hospital, Parel", "Mumbai"),
    ],

    "vascular_surgery": [
        ("Artemis Hospital", "Gurugram"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("MIOT International Chennai", "Chennai"),
        ("Apollo Hospitals, Indore", "Indore"),
        ("Gleneagles Hospitals", "Multiple Cities"),
        ("Gleneagles Hospital, Parel", "Mumbai"),
        ("SP Medifort Hospital Trivandrum", "Trivandrum"),
    ],

    "bariatric_metabolic": [
        ("Artemis Hospital", "Gurugram"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("Apollo Hospitals, Indore", "Indore"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("SP Medifort Hospital Trivandrum", "Trivandrum"),
        ("Gleneagles Hospitals", "Multiple Cities"),
        ("Gleneagles Hospital, Parel", "Mumbai"),
        ("Gleneagles BGS Hospital, Kengeri", "Bengaluru"),
        ("Gleneagles Hospital, Perumbakkam, Sholinganallur", "Chennai"),
    ],

    "ophthalmology": [
        # None of these hospitals are dedicated eye hospitals —
        # mapping only large multi-specialties with known ophtho depts
        ("Artemis Hospital", "Gurugram"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("Apollo Hospitals, Indore", "Indore"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("Gleneagles Hospitals", "Multiple Cities"),
        ("SP Medifort Hospital Trivandrum", "Trivandrum"),
    ],

    "rehabilitation": [
        # Rehab is common post-ortho/neuro — SPARSH and large multi-specialties
        ("SPARSH Hospital", "Bengaluru"),
        ("Sparsh Super Speciality Hospital Yeshwanthpur", "Bengaluru"),
        ("Institute of Brain and Spine (IBS Hospital)", "Bengaluru"),
        ("Artemis Hospital", "Gurugram"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("MIOT International Chennai", "Chennai"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("Gleneagles Hospitals", "Multiple Cities"),
    ],

    "radiology": [
        # Diagnostic/interventional radiology — all large multi-specialty centres
        ("Artemis Hospital", "Gurugram"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("Apollo Hospitals, Indore", "Indore"),
        ("MIOT International Chennai", "Chennai"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("Gleneagles Hospitals", "Multiple Cities"),
        ("Gleneagles Hospital, Parel", "Mumbai"),
        ("SP Medifort Hospital Trivandrum", "Trivandrum"),
    ],

    "cosmetic": [
        # Conservative — only large hospitals with known plastic/cosmetic surgery units
        ("Artemis Hospital", "Gurugram"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("Apollo Hospitals, Indore", "Indore"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("Gleneagles Hospitals", "Multiple Cities"),
    ],

    "dental": [
        # None of these are dedicated dental hospitals — mapping only large
        # multi-specialties that realistically carry dental/maxillofacial units
        ("Artemis Hospital", "Gurugram"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("Apollo Hospitals, Indore", "Indore"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
    ],

    "general": [
        # Safe fallback for any specialty not matched above
        ("Artemis Hospital", "Gurugram"),
        ("Apollo Hospitals, Indore", "Indore"),
        ("Sri Ramachandra Medical Centre (SRMC)", "Chennai"),
        ("Max Hospital, Gurgaon", "Gurgaon"),
        ("Gleneagles Hospitals", "Multiple Cities"),
        ("Gleneagles Hospital, Parel", "Mumbai"),
        ("Gleneagles BGS Hospital, Kengeri", "Bengaluru"),
        ("Gleneagles Hospital, Lakdi ka pul", "Hyderabad"),
        ("Gleneagles Aware Hospital, LB Nagar", "Hyderabad"),
        ("Gleneagles Hospital, Perumbakkam, Sholinganallur", "Chennai"),
        ("Gleneagles Hospital Richmond", "Bengaluru"),
        ("SP Medifort Hospital Trivandrum", "Trivandrum"),
        ("Lokmanya Hospitals", "Pune"),
        ("MIOT International Chennai", "Chennai"),
    ],
}

def get_partner_hospitals(specialty: str, limit: int = 3) -> list:
    """Return list of (name, city) tuples for a specialty. Falls back to general.

    Reconciles naming differences between SPECIALTY_PATTERNS (used by
    detect_specialty) and PARTNER_HOSPITALS keys. If a direct lookup misses,
    tries the aliased name, then falls back to general.
    """
    # Aliases: SPECIALTY_PATTERNS name → PARTNER_HOSPITALS name
    _ALIASES = {
        "fertility":  "fertility_ivf",
        "bariatric":  "bariatric_metabolic",
        "transplant": "organ_transplant",
        "neurology":  "neuro",
    }
    canonical = _ALIASES.get(specialty, specialty)
    hospitals = (
        PARTNER_HOSPITALS.get(canonical, [])
        or PARTNER_HOSPITALS.get(specialty, [])
        or PARTNER_HOSPITALS.get("general", [])
    )
    return hospitals[:limit]


# ═════════════════════════════════════════════════════════════════════
# SECTION 8 — INTERNAL LINK INVENTORY
# ═════════════════════════════════════════════════════════════════════
# ⚠️ FILL IN — used for internal linking suggestions in blog outputs
# Key = slug on divinheal.com, value = short description for relevance matching

INTERNAL_LINKS = {
    # "ivf-cost-india":                "IVF cost comparison India vs abroad",
    # "heart-bypass-surgery-india":    "CABG surgery and recovery in India",
    # "knee-replacement-india":        "Knee replacement procedures and costs",
    # "medical-visa-india":            "India medical visa guide",
    # "how-divinheal-works":           "Divinheal process and coordinator support",
    # "about-us":                      "About Divinheal",
}


def get_internal_link_suggestions(topic_words: list, limit: int = 5) -> list:
    """Match topic words to internal links. Returns list of (slug, description) tuples."""
    if not INTERNAL_LINKS:
        return []

    scored = []
    topic_set = set(w.lower() for w in topic_words)
    for slug, desc in INTERNAL_LINKS.items():
        slug_words = set(re.split(r"[-_/]", slug.lower()))
        desc_words = set(re.findall(r"\b\w+\b", desc.lower()))
        score = len(topic_set & (slug_words | desc_words))
        if score > 0:
            scored.append((score, slug, desc))
    scored.sort(reverse=True)
    return [(slug, desc) for _, slug, desc in scored[:limit]]


# ═════════════════════════════════════════════════════════════════════
# SECTION 9 — CITATION ALLOWLIST  (critical for YMYL compliance)
# ═════════════════════════════════════════════════════════════════════
# ⚠️ FILL IN — ONLY these sources may be cited by name in generated blogs.
# Anything outside this list gets rewritten to "multiple published studies"
# to prevent Gemini from inventing JAMA/NEJM/ICMR citations.

CITATION_ALLOWLIST = {
    # Umbrella health bodies (safe for general claims)
    "umbrella_bodies": [
        "World Health Organization (WHO)",
        "Indian Council of Medical Research (ICMR)",
        "National Institutes of Health (NIH)",
        "National Institute for Health and Care Excellence (NICE, UK)",
        "Centers for Disease Control and Prevention (CDC, US)",
        "National Health and Medical Research Council (NHMRC, Australia)",
    ],

    # Accreditation bodies
    "accreditations": [
        "Joint Commission International (JCI)",
        "National Accreditation Board for Hospitals & Healthcare Providers (NABH)",
        "National Accreditation Board for Testing and Calibration Laboratories (NABL)",
        "International Organization for Standardization (ISO)",
        "General Medical Council (GMC, UK)",
        "Royal College of Surgeons (FRCS, UK)",
        "National Medical Commission (NMC, India)",
    ],

# ─────────────────────────────────────────────────────────────────
#  INDIAN SOCIETIES & VERIFIED STUDIES — Divinheal Citation Bank
#  ⚠️  YMYL NOTE: Flagged entries must be verified before publishing.
#       HIGH   = verified, safe to cite immediately
#       VERIFY = society/study exists but confirm exact name/URL first
# ─────────────────────────────────────────────────────────────────

"indian_societies": {

    "cardiac": [
        "Cardiological Society of India (CSI)",                     # HIGH
        "Indian Association of Cardiovascular Thoracic Surgeons (IACTS)",  # HIGH
        "Indian College of Cardiology (ICC)",                       # VERIFY
    ],

    "orthopedic": [
        "Indian Orthopaedic Association (IOA)",                     # HIGH
        "Indian Arthroscopy Society (IAS)",                         # HIGH
        "Indian Shoulder & Elbow Society (ISES)",                   # VERIFY
    ],

    "oncology": [
        "Indian Cancer Society (ICS)",                              # HIGH
        "Indian Society of Oncology (ISO)",                         # HIGH
        "Indian Cooperative Oncology Network (ICON)",               # HIGH
        "Association of Radiation Oncologists of India (AROI)",     # HIGH
    ],

    "hematology": [
        "Indian Society of Haematology and Blood Transfusion (ISHBT)",  # HIGH
        "Haematology Society of India (HSI)",                       # VERIFY — may overlap with ISHBT
    ],

    "neuro": [
        "Neurological Society of India (NSI)",                      # HIGH
        "Indian Academy of Neurology (IAN)",                        # HIGH
    ],

    "paediatric_neurology": [
        "Indian Child Neurology Association (ICNA)",                 # HIGH
        "Indian Academy of Paediatrics — Neurology Chapter (IAP)",  # HIGH
    ],

    "paediatric": [
        "Indian Academy of Paediatrics (IAP)",                      # HIGH
        "National Neonatology Forum of India (NNF)",                # HIGH
    ],

    "fertility_ivf": [
        "Indian Society of Assisted Reproduction (ISAR)",           # HIGH
        "Indian Fertility Society (IFS)",                           # HIGH
    ],

    "gynecology": [
        "Federation of Obstetric and Gynaecological Societies of India (FOGSI)",  # HIGH
        "Indian Association of Gynaecological Endoscopists (IAGE)",               # HIGH
    ],

    "obstetrics": [
        "Federation of Obstetric and Gynaecological Societies of India (FOGSI)",  # HIGH
    ],

    "organ_transplant": [
        "Indian Society of Organ Transplantation (ISOT)",           # HIGH
        "National Organ and Tissue Transplant Organisation (NOTTO)", # HIGH — Govt body
    ],

    "nephrology": [
        "Indian Society of Nephrology (ISN)",                       # HIGH
        "Indian Society of Organ Transplantation (ISOT)",           # HIGH — for renal transplant
    ],

    "urology": [
        "Urological Society of India (USI)",                        # HIGH
        "Indian Urological Association (IUA)",                      # VERIFY
    ],

    "gastroenterology": [
        "Indian Society of Gastroenterology (ISG)",                 # HIGH
        "Society of Gastrointestinal Endoscopy of India (SGEI)",    # HIGH
        "Indian Society of Colorectal Surgeons (ISCS)",             # VERIFY
    ],

    "ent": [
        "Association of Otolaryngologists of India (AOI)",          # HIGH
        "Indian Society of Otology (ISO)",                          # HIGH
    ],

    "vascular_surgery": [
        "Vascular Society of India (VSI)",                          # HIGH
    ],

    "bariatric_metabolic": [
        "Obesity and Metabolic Surgery Society of India (OSSI)",    # HIGH
        "Indian Society for Bariatric and Metabolic Surgery (ISBMS)", # VERIFY — confirm vs OSSI
    ],

    "ophthalmology": [
        "All India Ophthalmological Society (AIOS)",                # HIGH
        "Vitreoretinal Society of India (VRSI)",                    # HIGH
    ],

    "rehabilitation": [
        "Indian Association of Physical Medicine and Rehabilitation (IAPMR)",  # HIGH
        "Indian Association of Physiotherapists (IAP-PT)",          # VERIFY
    ],

    "radiology": [
        "Indian Radiological and Imaging Association (IRIA)",       # HIGH
        "Indian Society of Neuroradiology (ISNR)",                  # VERIFY
        "Society of Interventional Radiology of India (SIRI)",      # VERIFY
    ],

    "cosmetic": [
        "Association of Plastic Surgeons of India (APSI)",          # HIGH
        "Indian Association of Aesthetic Plastic Surgeons (IAAPS)", # VERIFY
    ],

    "dental": [
        "Indian Dental Association (IDA)",                          # HIGH
        "Indian Orthodontic Society (IOS)",                         # HIGH
        "Indian Prosthodontic Society (IPS)",                       # HIGH
    ],
},


# ─────────────────────────────────────────────────────────────────
#  VERIFIED STUDIES & INSTITUTIONAL REPORTS
#  Format: "Title / Source — Publisher, Year"
#  ⚠️  Do NOT fabricate DOIs or authors. Link to source when citing.
# ─────────────────────────────────────────────────────────────────

"verified_studies": {

    "oncology": [
        # HIGH — ICMR NCRP publishes population-based cancer registry data
        "National Cancer Registry Programme Report — Indian Council of Medical Research (ICMR), 2023",
        "Three-year report of Population Based Cancer Registries — ICMR-NCRP, 2012–2014",
        # VERIFY before citing exact figures
        "Cancer incidence and mortality in India — The Lancet Oncology (ICMR data series)",
    ],

    "organ_transplant": [
        # HIGH — NOTTO is the official Govt of India body; annual reports are public
        "Annual Report on Organ Transplantation in India — National Organ and Tissue Transplant Organisation (NOTTO), 2022–23",
        "Transplant data and trend analysis — NOTTO, Ministry of Health and Family Welfare, GoI",
    ],

    "cardiac": [
        # HIGH — CSI publishes guidelines; ICMR publishes CVD burden data
        "Burden of cardiovascular disease in India — Indian Heart Journal / ICMR",
        # VERIFY — specific study year before citing
        "India Heart Watch Study — Cardiological Society of India (CSI)",
    ],

    "orthopedic": [
        # HIGH — well-documented public health data
        "Prevalence of musculoskeletal disorders in India — Indian Journal of Orthopaedics",
        "Burden of osteoarthritis in India — Indian Orthopaedic Association (IOA) position paper",
    ],

    "fertility_ivf": [
        # HIGH — ART Bill and registry data
        "Assisted Reproductive Technology (ART) regulation data — ICMR National ART Registry",
        # VERIFY — ISAR publishes outcome data periodically
        "IVF success rates and outcome trends in India — Indian Society of Assisted Reproduction (ISAR)",
    ],

    "nephrology": [
        # HIGH — CKD burden well documented
        "Prevalence of Chronic Kidney Disease in India — Indian Journal of Nephrology",
        "Chronic Kidney Disease of Unknown Aetiology (CKDu) in India — Indian Society of Nephrology (ISN)",
    ],

    "bariatric_metabolic": [
        # HIGH — obesity/diabetes burden in India is well-published
        "Metabolic surgery outcomes in India — Obesity and Metabolic Surgery Society of India (OSSI)",
        # VERIFY exact title
        "Rising obesity rates and bariatric surgery trends in India — Journal of Minimal Access Surgery",
    ],

    "ophthalmology": [
        # HIGH — India's blindness data is globally cited
        "National Blindness and Visual Impairment Survey India 2015–19 — Ministry of Health and Family Welfare, GoI",
        "Surgical outcomes of cataract surgery in India — Indian Journal of Ophthalmology",
    ],

    "neuro": [
        # HIGH
        "Burden of neurological disorders in India — Neurological Society of India (NSI)",
        "Stroke incidence and outcomes in India — Indian Stroke Association / Indian Academy of Neurology",
    ],

    "gastroenterology": [
        # HIGH — ISG publishes guidelines and epidemiology
        "Epidemiology of inflammatory bowel disease in India — Indian Society of Gastroenterology (ISG)",
        "Indian data on NAFLD/NASH burden — Indian Journal of Gastroenterology",
    ],

    "gynecology_obstetrics": [
        # HIGH — FOGSI and government data
        "Maternal Mortality Ratio trends in India — Sample Registration System (SRS), Registrar General of India",
        "FOGSI Good Clinical Practice Recommendations — Federation of Obstetric and Gynaecological Societies of India",
    ],

    "dental": [
        # HIGH — MoHFW publishes oral health data
        "National Oral Health Survey and Fluoride Mapping — Dental Council of India / MoHFW",
        "Oral health status of India — Indian Dental Association (IDA) position report",
    ],

    "rehabilitation": [
        # VERIFY — less publicly indexed than clinical specialties
        "Rehabilitation outcomes post-stroke in India — Indian Association of Physical Medicine and Rehabilitation (IAPMR)",
    ],

    "radiology": [
        # HIGH — IRIA publishes guidelines
        "Clinical practice guidelines for diagnostic imaging — Indian Radiological and Imaging Association (IRIA)",
    ],

},
}


def all_allowed_citations(specialty: str | None = None) -> list:
    """Flatten the citation allowlist into a list of citation strings.

    Handles both flat lists (umbrella_bodies, accreditations) and nested
    by-specialty dicts (indian_societies, verified_studies).

    Args:
        specialty: optional specialty filter (e.g. "cardiac", "oncology").
                    When provided, specialty-nested sections return only that
                    specialty's items; flat sections (umbrella_bodies,
                    accreditations) are always included.
    """
    out = []
    for cat in CITATION_ALLOWLIST.values():
        if isinstance(cat, list):
            # Flat section — always include
            out.extend(cat)
        elif isinstance(cat, dict):
            # Nested by specialty — filter if specialty passed
            if specialty:
                items = cat.get(specialty, [])
                if isinstance(items, list):
                    out.extend(items)
            else:
                for specialty_list in cat.values():
                    if isinstance(specialty_list, list):
                        out.extend(specialty_list)
    return out


# ═════════════════════════════════════════════════════════════════════
# SECTION 10 — COUNTRY MAP  (ISO alpha-2 → display name)
# ═════════════════════════════════════════════════════════════════════
# ✏️ OPTIONAL — add countries as you expand to new markets

COUNTRY_MAP = {
    "ng": "Nigeria",       "ae": "UAE",          "gb": "UK",
    "us": "USA",           "et": "Ethiopia",     "ke": "Kenya",
    "za": "South Africa",  "sa": "Saudi Arabia", "pk": "Pakistan",
    "bd": "Bangladesh",    "au": "Australia",    "ca": "Canada",
    "sg": "Singapore",     "om": "Oman",         "qa": "Qatar",
    "bh": "Bahrain",       "kw": "Kuwait",       "in": "India",
    "iq": "Iraq",          "lk": "Sri Lanka",    "np": "Nepal",
    "mm": "Myanmar",       "af": "Afghanistan",  "uz": "Uzbekistan",
    "gh": "Ghana",         "uk": "UK",
}


def get_country_name(code: str) -> str:
    """Safe country lookup — returns uppercase code if unknown."""
    return COUNTRY_MAP.get(code.lower().strip(), code.upper().strip())


# ═════════════════════════════════════════════════════════════════════
# SECTION 11 — COUNTRY PERSONAS  (drives locale-specific blog framing)
# ═════════════════════════════════════════════════════════════════════
# ✏️ OPTIONAL — edit concerns/trust_signals based on real customer research
# Each persona shows up in the section writer prompt for that country

COUNTRY_PERSONAS = {
    "ae": {
        "concerns": "premium quality, English+Arabic support, Dubai proximity",
        "cost_frame": "AED vs INR, premium hospital emphasis",
        "language": "English standard; Arabic is a plus",
        "trust_signals": "JCI-accredited hospitals, Arabic-speaking coordinators",
        "currency": "AED",
        "currency_symbol": "AED",
        "flight_hours_to_india": "~3 hours",
    },
    "sa": {
        "concerns": "religious compatibility, Arabic support, family accommodation",
        "cost_frame": "SAR vs INR, significant savings vs local private",
        "language": "Arabic preferred",
        "trust_signals": "JCI accreditation, halal food, prayer facilities",
        "currency": "SAR",
        "currency_symbol": "SAR",
        "flight_hours_to_india": "~4 hours",
    },
    "ng": {
        "concerns": "cost savings, visa logistics, capability gap locally",
        "cost_frame": "NGN vs INR, 60–80% savings",
        "language": "English",
        "trust_signals": "Step-by-step visa and logistics support",
        "currency": "NGN",
        "currency_symbol": "₦",
        "flight_hours_to_india": "~12 hours (often via Middle East)",
    },
    "bd": {
        "concerns": "cost sensitivity, proximity, cultural familiarity",
        "cost_frame": "BDT vs INR, 40–60% savings",
        "language": "Bengali-friendly hospitals preferred",
        "trust_signals": "Bengali interpreters, short direct flights, familiar food",
        "currency": "BDT",
        "currency_symbol": "৳",
        "flight_hours_to_india": "~2 hours",
    },
    "lk": {
        "concerns": "proximity, Tamil/Sinhala speakers, short travel",
        "cost_frame": "LKR vs INR, 30–50% savings",
        "language": "Tamil or Sinhala preferred",
        "trust_signals": "Direct flights from Colombo, Tamil-speaking staff",
        "currency": "LKR",
        "currency_symbol": "Rs.",
        "flight_hours_to_india": "~3 hours",
    },
    "au": {
        "concerns": "Medicare wait-list escape, quality parity with AU private",
        "cost_frame": "AUD vs INR, 75–90% savings vs private AU",
        "language": "English",
        "trust_signals": "JCI accreditation, FRCS/Western-trained surgeons",
        "currency": "AUD",
        "currency_symbol": "AUD",
        "flight_hours_to_india": "~12 hours",
    },
    "gb": {
        "concerns": "NHS wait-list escape, quality parity with UK private",
        "cost_frame": "GBP vs INR, 70–85% savings vs private UK",
        "language": "English",
        "trust_signals": "GMC/FRCS-trained surgeons, continuity post-return",
        "currency": "GBP",
        "currency_symbol": "£",
        "flight_hours_to_india": "~9 hours",
    },
    "et": {
        "concerns": "capability gap, cost, Amharic support",
        "cost_frame": "ETB vs INR, 50–70% savings",
        "language": "Amharic helpful",
        "trust_signals": "Amharic coordinator, visa support",
        "currency": "ETB",
        "currency_symbol": "Br",
        "flight_hours_to_india": "~6 hours",
    },
    "ke": {
        "concerns": "capability gap, cost, English communication",
        "cost_frame": "KES vs INR, 50–70% savings",
        "language": "English + Swahili",
        "trust_signals": "Visa support, hospital coordination",
        "currency": "KES",
        "currency_symbol": "KSh",
        "flight_hours_to_india": "~7 hours",
    },
    "om": {
        "concerns": "quality care, Arabic support, GCC proximity",
        "cost_frame": "OMR vs INR, 40–60% savings vs local private",
        "language": "Arabic preferred",
        "trust_signals": "JCI accreditation, Arabic coordinators",
        "currency": "OMR",
        "currency_symbol": "OMR",
        "flight_hours_to_india": "~3 hours",
    },
    "qa": {
        "concerns": "premium quality, Arabic support, family accommodation",
        "cost_frame": "QAR vs INR, 50–70% savings",
        "language": "Arabic preferred",
        "trust_signals": "JCI, halal food, Arabic coordinators",
        "currency": "QAR",
        "currency_symbol": "QAR",
        "flight_hours_to_india": "~3.5 hours",
    },
    "kw": {
        "concerns": "premium quality, Arabic support, GCC proximity",
        "cost_frame": "KWD vs INR, 50–70% savings",
        "language": "Arabic preferred",
        "trust_signals": "JCI, Arabic coordinators, halal food",
        "currency": "KWD",
        "currency_symbol": "KD",
        "flight_hours_to_india": "~4 hours",
    },
    "bh": {
        "concerns": "quality care, Arabic support, GCC proximity",
        "cost_frame": "BHD vs INR, 50–70% savings",
        "language": "Arabic preferred",
        "trust_signals": "JCI, Arabic coordinators",
        "currency": "BHD",
        "currency_symbol": "BD",
        "flight_hours_to_india": "~3.5 hours",
    },
}


def get_persona(code: str) -> dict:
    """Persona dict for a country code, with safe default for unknown codes."""
    return COUNTRY_PERSONAS.get(code.lower().strip(), {
        "concerns": "quality care, cost savings, logistics",
        "cost_frame": "local currency vs INR, significant savings",
        "language": "English",
        "trust_signals": "JCI-accredited hospitals, experienced surgeons",
        "currency": "USD",
        "currency_symbol": "$",
        "flight_hours_to_india": "varies",
    })


# ═════════════════════════════════════════════════════════════════════
# SECTION 12 — MEDICAL SPECIALTY DETECTION
# ═════════════════════════════════════════════════════════════════════
# ✏️ OPTIONAL — add specialties if you're expanding into new verticals
# Used by the keyword extractor to pick specialty-appropriate examples

SPECIALTY_PATTERNS = {

    "fertility": [
        # Core procedures (from CSV)
        "ivf", "icsi", "iui", "intracytoplasmic sperm injection",
        "intrauterine insemination", "in vitro fertilization",
        "assisted reproduction", "egg freezing", "oocyte cryopreservation",
        "embryo freezing", "embryo vitrification", "blastocyst transfer",
        "embryo biopsy", "embryo transfer", "frozen embryo transfer",
        "donor egg", "donor sperm", "egg donation",
        "preimplantation genetic testing", "pgt", "pgt-a", "pgt-m",
        "tese", "micro-tese", "sperm freezing", "sperm retrieval",
        "tubal recanalization", "tubal reversal", "reversal of sterilization",
        "ovarian drilling", "reproductive surgery",
        # Broader keywords
        "fertility", "infertility", "surrogacy", "ovulation induction",
        "ovarian stimulation", "semen analysis", "azoospermia",
        "recurrent miscarriage", "implantation failure",
    ],

    "cardiac": [
        # Interventional / structural (from CSV)
        "angioplasty", "coronary angioplasty", "carotid angioplasty",
        "angiogram", "angiography", "ct coronary angiography",
        "pci", "heart stent", "stent",
        "bypass surgery", "cabg", "off pump bypass", "mics cabg",
        "minimally invasive cabg", "midcab",
        "valve replacement", "valve repair", "heart valve surgery",
        "mitral valve surgery", "aortic valve surgery",
        "tavi", "tavr", "transcatheter aortic valve replacement",
        "mitraclip", "structural heart",
        "laac", "left atrial appendage closure",
        "aortic aneurysm repair",
        # Electrophysiology (from CSV)
        "pacemaker", "icd", "cardiac ablation", "ablation",
        "atrial fibrillation ablation", "electrophysiology",
        "cardiac electrophysiology study", "eps",
        "arrhythmia", "ventricular assist device",
        # Surgery & advanced (from CSV)
        "robotic cardiac surgery", "congenital heart surgery",
        "heart failure surgery", "heart failure program",
        "interventional cardiology", "cardiac surgery",
        "ecmo", "cardiac mri",
        # Broader keywords
        "heart", "cardiac", "cardiomyopathy",
        "coronary artery disease", "cad", "heart attack", "myocardial infarction",
    ],

    "orthopedic": [
        # Replacement surgeries (from CSV)
        "total knee replacement", "tkr",
        "total hip replacement", "thr",
        "revision knee replacement", "revision hip replacement",
        "hip resurfacing", "shoulder replacement",
        "robotic knee replacement", "robotic knee",
        # Spine (from CSV)
        "spinal fusion", "cervical fusion", "lumbar fusion",
        "spine decompression", "discectomy", "laminectomy",
        "scoliosis correction", "minimally invasive spine surgery",
        "slipped disc", "disc herniation",
        # Sports / arthroscopy (from CSV)
        "acl reconstruction", "acl", "pcl", "mcl",
        "arthroscopic meniscus repair", "meniscectomy", "meniscus",
        "rotator cuff repair", "arthroscopy",
        "sports injury surgery", "multi-ligament",
        # Trauma (from CSV)
        "trauma", "fracture fixation", "orif", "fracture",
        # Broader keywords
        "knee", "hip", "joint", "orthopedic", "orthopaedic",
        "joint replacement", "bone", "tendon", "ligament",
        "osteoarthritis", "cartilage",
    ],

    "cosmetic": [
        # Face (from CSV)
        "rhinoplasty", "nose reshaping", "nose surgery", "septorhinoplasty",
        "facelift", "anti-aging", "facial reanimation",
        "blepharoplasty", "eyelid surgery",
        "neck lift", "thread lift",
        "craniofacial surgery",
        "cleft repair", "cleft lip surgery", "cleft palate surgery",
        # Body (from CSV)
        "liposuction", "tummy tuck", "abdominoplasty",
        "body contouring", "brazilian butt lift", "bbl",
        "breast augmentation", "breast implants", "breast reduction",
        "breast lift", "male breast reduction", "gynecomastia surgery",
        "burn reconstruction", "scar revision",
        # Non-surgical (from CSV)
        "botox", "botox treatment",
        "lip fillers", "dermal fillers", "filler",
        "prp treatment", "laser skin resurfacing",
        "thread lift",
        # Hair (from CSV)
        "hair transplant", "fue hair transplant", "fut hair transplant",
        "fue", "fut",
        # Broader keywords
        "plastic surgery", "cosmetic surgery", "aesthetic", "reconstruction",
    ],

    "oncology": [
        # Surgical oncology (from CSV)
        "cancer surgery", "surgical oncology", "oncologic reconstruction",
        "thyroid cancer surgery", "bladder cancer surgery",
        "colorectal cancer surgery", "lung cancer surgery",
        "kidney cancer surgery", "prostate cancer surgery",
        "uterine cancer surgery", "liver cancer surgery",
        "pancreatic cancer surgery", "oral cancer surgery",
        "stomach cancer surgery", "colon cancer surgery",
        "head and neck cancer surgery", "mastectomy surgery",
        "breast cancer surgery", "breast cancer treatment",
        "melanoma surgery", "sarcoma surgery",
        "whipple surgery", "debulking surgery",
        "palliative cancer surgery",
        # Radiation / radiosurgery (from CSV)
        "radiation therapy", "imrt", "brachytherapy",
        "cyberknife", "cyberknife radiosurgery", "srs", "sbrt",
        "stereotactic radiosurgery", "gamma knife", "gamma knife radiosurgery",
        "proton therapy",
        "hipec", "heated intraperitoneal chemotherapy",
        # Interventional / ablative (from CSV)
        "chemoembolization", "tace",
        "radiofrequency ablation", "rfa",
        "theranostics", "y-90",
        "pet-ct guided cancer staging",
        # Systemic therapies (from CSV)
        "chemotherapy", "chemotherapy for cancer",
        "immunotherapy", "car t cell therapy", "car-t",
        "cellular therapy", "monoclonal antibody therapy",
        "targeted therapy", "targeted molecular therapy",
        "precision oncology",
        # Stem cell / transplant in oncology context (from CSV)
        "stem cell transplant cancer",
        # Broader keywords
        "cancer", "tumor", "tumour", "oncology", "oncology treatment",
        "lymphoma", "leukemia", "leukaemia", "sarcoma", "carcinoma",
        "malignancy", "metastasis", "robotic cancer surgery",
    ],

    "dental": [
        # Implants (from CSV)
        "dental implant", "single tooth dental implant",
        "multiple dental implants", "full mouth dental implants",
        "all-on-4 implants", "all-on-4",
        "all-on-6 implants", "all-on-6",
        "zygomatic implants", "immediate load implants",
        "teeth in a day", "implant-supported dentures",
        # Crowns, bridges, veneers (from CSV)
        "dental crowns", "zirconia crown", "e-max crown", "pfm crown",
        "full mouth crowns", "crowns & bridges", "dental bridges",
        "porcelain veneers", "composite veneers",
        "veneers", "smile makeover", "hollywood smile",
        "digital smile designing", "dsd",
        # Orthodontics (from CSV)
        "braces", "metal braces", "ceramic braces", "lingual braces",
        "orthodontic braces", "clear aligners", "invisalign",
        "retainers", "space maintainers",
        "bite correction", "occlusal rehabilitation",
        # Root canal & endodontics (from CSV)
        "root canal", "re-rct", "root canal retreatment",
        "pulpectomy", "pediatric root canal",
        "apicoectomy", "root-end surgery",
        # Gum / periodontics (from CSV)
        "gum grafting", "gum surgery", "gum contouring", "gum reshaping",
        "laser gum surgery", "laser gum treatment",
        "scaling & root planing", "deep cleaning", "periodontitis",
        # Extraction / surgery (from CSV)
        "wisdom tooth removal", "wisdom tooth extraction",
        "impacted wisdom tooth", "surgical extraction", "simple extraction",
        "jaw fracture treatment", "jaw cyst removal",
        "bone grafting", "ridge augmentation", "sinus lift",
        "benign oral tumor excision",
        # Diagnostics & other (from CSV)
        "cbct scan", "3d dental scan", "opg", "panoramic x-ray",
        "dental checkup", "fluoride treatment",
        "complete dentures", "flexible dentures", "removable partial dentures",
        "full mouth rehabilitation", "full mouth restoration",
        "sedation dentistry", "general anesthesia for dental",
        "teeth whitening", "dental abscess",
        "pediatric fillings", "pediatric crowns",
        # Broader keywords
        "dental", "implant", "orthodontic", "teeth", "tooth",
        "cavity", "cavity filling", "veneer", "denture",
    ],

    "ophthalmology": [
        # Refractive (from CSV)
        "lasik", "smile", "prk", "refractive surgery",
        "keratoconus", "cxl", "corneal cross-linking", "intacs rings",
        # Cataract (from CSV)
        "cataract surgery", "phacoemulsification", "phaco", "iol",
        "intraocular lens",
        # Glaucoma (from CSV)
        "glaucoma surgery", "trabeculectomy", "migs",
        "minimally invasive glaucoma surgery",
        # Retina (from CSV)
        "vitrectomy", "retinal detachment repair",
        "diabetic retinopathy", "macular degeneration",
        "retinal laser",
        # Cornea / surface (from CSV)
        "corneal transplant", "keratoplasty",
        "pterygium surgery",
        # Paediatric / oculoplastic (from CSV)
        "pediatric squint surgery", "squint", "strabismus",
        "oculoplastic surgery",
        # Broader keywords
        "cornea", "cataract", "glaucoma", "retina", "eye surgery",
        "vision", "vision correction", "eye", "ophthalmology",
    ],

    "bariatric": [
        # Procedures (from CSV)
        "sleeve gastrectomy", "gastric sleeve",
        "roux-en-y gastric bypass", "roux en y",
        "mini gastric bypass", "oagb", "one anastomosis gastric bypass",
        "revision bariatric surgery",
        "adjustable gastric banding", "gastric banding",
        "intragastric balloon", "gastric balloon",
        # Broader keywords
        "bariatric", "bariatric surgery",
        "weight loss surgery", "obesity surgery",
        "metabolic surgery", "metabolic surgery",
        "morbid obesity", "bmi reduction",
    ],

    "transplant": [
        # Organ-specific (from CSV)
        "kidney transplant", "robotic kidney transplant",
        "liver transplant", "living donor liver transplant", "ldlt",
        "heart transplant",
        "lung transplant",
        "pancreas transplant",
        "deceased donor organ transplant",
        # Process / evaluation packages (from CSV)
        "pre-transplant evaluation", "post-transplant follow-up",
        # Bone marrow / stem cell in transplant context (from CSV)
        "bone marrow transplant", "bmt",
        "autologous bone marrow transplant", "allogeneic bone marrow transplant",
        "stem cell transplant", "haploidentical transplant",
        # Broader keywords
        "transplant", "organ transplant", "living donor",
        "cadaveric transplant", "rejection management",
    ],

    "urology": [
        # Prostate (from CSV)
        "holep", "laser prostate surgery",
        "turp", "transurethral resection of prostate",
        "robotic prostatectomy",
        # Kidney stones (from CSV)
        "ureteroscopy", "urs", "pcnl", "percutaneous nephrolithotomy",
        "eswl", "shock wave lithotripsy",
        "rirs", "laser treatment for kidney stones",
        # Bladder (from CSV)
        "cystoscopy", "turbt", "bladder tumor resection",
        # Stricture (from CSV)
        "urethral stricture surgery", "urethroplasty",
        # Broader keywords
        "urology", "prostate", "kidney stone", "bladder", "renal stone",
        "circumcision", "vasectomy", "incontinence urology",
        "urinary tract", "uti recurrent", "overactive bladder",
    ],

    "neurology": [
        # Brain surgery (from CSV)
        "brain surgery", "neurosurgery",
        "endoscopic brain surgery", "minimally invasive brain tumor surgery",
        "brain tumor treatment", "brain tumor surgery",
        "awake craniotomy", "skull base surgery",
        "pituitary surgery", "pituitary tumor surgery",
        "transsphenoidal surgery",
        "brain aneurysm surgery", "brain aneurysm treatment",
        "aneurysm clipping", "brain coiling surgery",
        "avm brain surgery", "arteriovenous malformation",
        "decompressive craniectomy",
        # Spine (neurology context) (from CSV)
        "spinal cord stimulation", "spinal cord tumor surgery",
        "slipped disc surgery", "disc herniation surgery",
        "spinal surgery", "spinal fusion surgery",
        "laminectomy surgery",
        # Movement / functional (from CSV)
        "deep brain stimulation", "dbs",
        "parkinson surgery", "parkinson disease treatment",
        "movement disorder treatment",
        "vagus nerve stimulation", "vns implant",
        "neuromodulation therapy",
        # Epilepsy (from CSV)
        "epilepsy treatment", "epilepsy surgery",
        "pediatric epilepsy surgery",
        # Stroke (from CSV)
        "stroke treatment",
        "endovascular thrombectomy",
        "thrombectomy",
        # Hydrocephalus / CSF (from CSV)
        "hydrocephalus surgery", "vp shunt surgery",
        "endoscopic third ventriculostomy",
        # Peripheral nerve (from CSV)
        "peripheral nerve surgery", "carpal tunnel",
        "ulnar nerve",
        # Rehabilitation / other (from CSV)
        "neurorehabilitation", "neuro rehabilitation program",
        # Broader keywords
        "brain", "neuro", "neurological",
        "stroke", "epilepsy", "seizure",
        "alzheimer", "dementia",
        "multiple sclerosis", "ms",
        "meningitis", "nerve",
    ],

    "gastroenterology": [
        # Endoscopic (from CSV)
        "upper gi endoscopy", "gastroscopy",
        "colonoscopy", "colonoscopy with polypectomy",
        "capsule endoscopy",
        "ercp", "endoscopic retrograde cholangiopancreatography",
        "endoscopic ultrasound", "eus",
        # Hepatology (from CSV)
        "liver fibroscan", "liver biopsy",
        "hepatitis management", "cirrhosis management",
        # Surgery (from CSV)
        "gallbladder surgery", "laparoscopic cholecystectomy",
        "hernia repair", "laparoscopic hernia",
        # Broader keywords
        "gallbladder", "hernia", "colon", "liver",
        "gastric", "endoscopy", "digestive",
        "ibs", "irritable bowel", "crohn", "ulcerative colitis",
        "pancreatitis", "cholecystitis", "jaundice",
    ],

    "hematology": [
        # Blood cancers (from CSV)
        "leukemia treatment", "blood cancer treatment",
        "lymphoma treatment",
        "multiple myeloma treatment",
        "hematologic malignancy",
        "chemotherapy for blood cancer",
        # Transplant & advanced (from CSV)
        "stem cell transplant", "bmt treatment",
        "autologous bone marrow transplant",
        "allogeneic bone marrow transplant",
        "haploidentical transplant",
        "gvhd treatment", "graft versus host disease",
        "aplastic anemia treatment",
        "hemophilia care",
        # Diagnostics (from CSV)
        "flow cytometry", "bone marrow biopsy",
        # Broader keywords
        "hematology", "hematology treatment",
        "blood disorder", "anemia", "thalassemia",
        "platelet disorder", "itp", "sickle cell",
        "bleeding disorder",
    ],

    "gynecology": [
        # Hysterectomy types (from CSV)
        "hysterectomy", "laparoscopic hysterectomy",
        "vaginal hysterectomy", "radical hysterectomy",
        "robotic hysterectomy",
        # Gynecologic oncology (from CSV)
        "ovarian cancer surgery", "endometrial cancer surgery",
        "cervical cancer surgery", "uterine cancer surgery",
        # Endoscopic gynecology (from CSV)
        "hysteroscopic surgery", "hysteroscopy",
        "laparoscopic gynecology surgery",
        # Fibroid / prolapse (from CSV)
        "uterine fibroid treatment", "fibroid surgery", "myomectomy",
        "uterine artery embolization", "uae",
        "pelvic organ prolapse surgery", "pelvic floor repair",
        "incontinence surgery",
        # Cyst / endometriosis (from CSV)
        "ovarian cystectomy", "ovarian cyst surgery",
        "endometriosis surgery",
        # Other (from CSV)
        "ectopic pregnancy surgery",
        "tubal reversal surgery",
        "cervical cancer screening", "pap smear", "hpv test", "colposcopy",
        # Broader keywords
        "gynecology", "gynecology treatment",
        "uterus", "ovary", "fallopian tube",
        "menstrual disorder", "pcod", "pcos",
        "vaginal", "cervical",
    ],

    "obstetrics": [
        # High-risk / maternal-fetal (from CSV)
        "perinatology", "maternal fetal medicine",
        "high risk pregnancy treatment",
        "placenta accreta treatment",
        # Broader keywords
        "obstetrics", "pregnancy", "antenatal",
        "prenatal", "labour", "delivery",
        "c-section", "caesarean", "normal delivery",
        "preeclampsia", "gestational diabetes",
        "miscarriage management", "premature birth",
    ],

    "ent": [
        # Ear (from CSV)
        "tympanoplasty", "eardrum repair",
        "cochlear implant surgery", "cochlear implant",
        "stapedectomy", "stapedotomy",
        "myringotomy", "grommet",
        # Nose / sinus (from CSV)
        "septoplasty", "endoscopic sinus surgery", "fess",
        # Throat / voice (from CSV)
        "tonsillectomy", "adenoidectomy",
        "microlaryngeal surgery", "voice surgery",
        # Sleep (from CSV)
        "sleep apnea surgery", "uppp", "sleep study",
        # Broader keywords
        "ent", "ear", "nose", "throat",
        "sinusitis", "hearing loss", "tinnitus",
        "deviated septum", "nasal polyp",
        "laryngoscopy", "otitis media",
    ],

    "vascular_surgery": [
        # Procedures (from CSV)
        "varicose veins treatment", "evla", "sclerotherapy",
        "peripheral angioplasty", "peripheral stenting",
        "carotid endarterectomy",
        "aortic aneurysm repair", "endovascular aneurysm repair", "evar",
        # Broader keywords
        "vascular surgery", "vascular",
        "deep vein thrombosis", "dvt",
        "peripheral artery disease", "pad",
        "arterial occlusion", "limb ischemia",
        "varicose", "venous ulcer", "aneurysm",
    ],

    "rehabilitation": [
        # Types (from CSV)
        "stroke rehabilitation",
        "spine rehabilitation",
        "post-operative physiotherapy", "post-op physiotherapy",
        "neurorehabilitation", "neuro rehabilitation",
        # Broader keywords
        "rehabilitation", "physiotherapy", "physical therapy",
        "occupational therapy", "speech therapy",
        "cardiac rehabilitation", "ortho rehabilitation",
        "gait training", "motor recovery",
    ],

    "radiology": [
        # Interventional (from CSV)
        "interventional radiology",
        "ufe", "uterine fibroid embolization",
        "tace", "y-90", "liver tumor embolization",
        "chemoembolization",
        # Diagnostic imaging (from CSV)
        "digital mammography", "mammography",
        "dexa scan", "bone density scan",
        "ct scan", "contrast ct", "non-contrast ct",
        "pet-ct scan", "pet scan",
        "3 tesla mri", "mri scan",
        "4d ultrasound", "ultrasound",
        # Broader keywords
        "radiology", "imaging",
        "x-ray", "fluoroscopy",
        "biopsy guided", "image guided",
    ],

    "paediatric": [
        # Specific (from CSV)
        "neonatal intensive care", "nicu",
        "pediatric laparoscopic surgery",
        "pediatric kidney transplant",
        "pediatric cardiac surgery",
        "pediatric neurosurgery",
        # Broader keywords
        "paediatric", "pediatric", "children", "child", "newborn",
        "neonatal", "infant",
        "pediatric surgery", "kids surgery",
    ],

    "paediatric_neurology": [
        # Specific (from CSV)
        "pediatric stroke treatment", "pediatric stroke",
        "cerebral palsy",
        "pediatric epilepsy surgery",
        "brain tumor surgery in children",
        # Broader keywords
        "paediatric neurology", "pediatric neurology",
        "child brain", "child epilepsy", "child seizure",
        "childhood stroke", "developmental delay",
        "pediatric brain tumor",
    ],

    "nephrology": [
        # Procedures (from CSV)
        "kidney biopsy",
        "peritoneal dialysis",
        "hemodialysis",
        "av fistula creation", "fistula for dialysis",
        # Broader keywords
        "nephrology", "chronic kidney disease", "ckd",
        "kidney failure", "renal failure", "dialysis",
        "renal", "glomerulonephritis", "nephrotic syndrome",
        "polycystic kidney", "kidney transplant evaluation",
    ],

}


def detect_specialty(keyword: str) -> str:
    """Detect medical specialty from keyword. Returns 'general' if unclear."""
    kw = keyword.lower()
    for specialty, patterns in SPECIALTY_PATTERNS.items():
        if any(p in kw for p in patterns):
            return specialty
    return "general"


# ═════════════════════════════════════════════════════════════════════
# SECTION 13 — YMYL COMPLIANCE  (legal/medical safety guardrails)
# ═════════════════════════════════════════════════════════════════════
# ⚠️ FILL IN — review these with your legal/compliance team

YMYL_DISCLAIMERS = {
    "clinical_outcome": "Individual outcomes vary. Consult your specialist for personalised advice.",
    "cost_estimate":    "Costs are approximate and depend on hospital, surgeon seniority, and case complexity.",
    "recovery":         "Recovery timelines vary by patient age, health, and procedure complexity.",
    "medical_data":     "Cited figures reflect published ranges as of 2026; verify with your treating team.",
    "patient_story":    "Composite example based on typical patient outcomes. Names changed for privacy.",
}

# Phrases the model should never emit (medical/legal risk)
FORBIDDEN_MEDICAL_CLAIMS = [
    "guaranteed results",
    "cure",
    "100% success",
    "risk-free",
    "painless",                   # never claim absolute
    "you will definitely",
    "permanent solution",
    "we promise",
    "no complications",
    "miracle",
]


# ═════════════════════════════════════════════════════════════════════
# SECTION 14 — SCRAPING + SERP BEHAVIOR
# ═════════════════════════════════════════════════════════════════════
# ✏️ OPTIONAL — tune for your domain-scraping strategy

# Domains to exclude from competitor analysis (aggregators, Q&A, reference)
EXCLUDED_DOMAINS = [
    "youtube.com", "youtu.be", "facebook.com", "instagram.com",
    "linkedin.com", "twitter.com", "x.com", "wikipedia.org",
    "justdial.com", "practo.com", "healthgrades.com", "webmd.com",
    "mayoclinic.org", "clevelandclinic.org", "amazon.", "flipkart.",
    "quora.com",        # handled separately via Brave forum search
    "reddit.com",       # handled separately via Reddit cell
]

# Autocomplete intent modifiers (expanded from v15 to cover medical-tourism intents)
AUTOCOMPLETE_MODIFIERS = [
    "",              # base keyword
    " how",          # informational
    " cost",         # transactional
    " best",         # comparison
    " why",          # motivational
    " which",        # choice
    " vs",           # direct comparison
    " recovery",     # post-procedure
    " side effects", # safety
    " safe",         # safety
    " success rate", # outcomes
    " risks",        # safety
    " for",          # country-targeted (e.g., "for UK patients")
]

# Reddit subreddits to search (prefer specific over generic)
REDDIT_SUBREDDITS = [
    "medicaltourism", "IndianHealthTourism",
    # Generic ones — lower signal, keep if they've helped
    "AskDocs", "medical", "Health",
    # Add specialty-specific as you expand:
    # "infertility", "heart", "cancer", "weightlosssurgery",
]

# Reddit subreddits to exclude (high noise)
REDDIT_EXCLUDED_SUBREDDITS = [
    "eatcheapandhealthy", "cooking", "food", "AskReddit",
    "gaming", "memes", "funny", "aww", "pics", "todayilearned",
    "wallstreetbets", "cryptocurrency", "stocks",
]


# ═════════════════════════════════════════════════════════════════════
# SECTION 15 — GEMINI SAFETY SETTINGS
# ═════════════════════════════════════════════════════════════════════
# 🔒 DO NOT EDIT — medical content triggers false positives; safety off is required
# (Medical discussions of surgery, anatomy, and outcomes trip default filters)

SAFETY_OFF = [
    types.SafetySetting(category="HARM_CATEGORY_HARASSMENT",        threshold="OFF"),
    types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH",       threshold="OFF"),
    types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="OFF"),
    types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="OFF"),
    types.SafetySetting(category="HARM_CATEGORY_CIVIC_INTEGRITY",   threshold="OFF"),
]


# ═════════════════════════════════════════════════════════════════════
# SECTION 16 — AI-DETECTION PHRASES TO AVOID
# ═════════════════════════════════════════════════════════════════════
# ⚠️ FILL IN / ADJUST — update as new LLM tells emerge

# Phrases that flag as AI-generated (2025–2026 tells)
AI_TELL_PHRASES = [
    # Classic LLM slop
    "delve into", "dive into", "navigate the journey", "at the forefront",
    "groundbreaking", "revolutionary", "cutting-edge", "state-of-the-art",
    "world-class", "game-changer", "transformative", "pivotal",
    "robust", "seamless", "seamlessly", "leverage", "unparalleled",
    "tapestry", "realm of", "in the realm of", "testament to",
    "stands as", "paradigm shift", "unlock the potential",

    # Transition word overuse
    "furthermore", "moreover", "additionally", "notwithstanding",
    "consequently", "accordingly", "henceforth",

    # Template openers
    "in today's world", "in today's fast-paced world", "in an era",
    "when it comes to", "that being said", "at the end of the day",
    "the fact of the matter is", "it is important to note",
    "it should be noted that", "it is worth mentioning that",

    # Template middle/close
    "something worth knowing:", "what surprised most of our patients:",
    "a practical tip —", "a quick reality check",
    "understanding these ... helps you ...",

    # Medical-AI tells
    "your journey to wellness", "empowering you to make informed decisions",
    "highly experienced team of doctors", "renowned surgeons",
    "comprehensive care", "holistic approach",
]


# ═════════════════════════════════════════════════════════════════════
# SECTION 17 — SELF-TEST  (run `python config.py` to validate)
# ═════════════════════════════════════════════════════════════════════
# 🔒 DO NOT EDIT

import re  # needed for get_internal_link_suggestions()

GOOGLE_CREDS_ENV_VARS = [
    "type",
    "project_id",
    "private_key_id",
    "private_key",
    "client_email",
    "client_id",
    "auth_uri",
    "token_uri",
    "auth_provider_x509_cert_url",
    "client_x509_cert_url",
]



def _count_citations():
    total = 0
    for v in CITATION_ALLOWLIST.values():
        if isinstance(v, list):
            total += len(v)
        elif isinstance(v, dict):
            total += sum(len(lst) for lst in v.values())
    return total



def _validate():
    issues = []
    if not BRAND.get("name"):
        issues.append("BRAND.name is empty")
    if not SPREADSHEET_NAME:
        issues.append("SPREADSHEET_NAME is empty")
    partner_total = sum(len(v) for v in PARTNER_HOSPITALS.values())
    if partner_total == 0:
        issues.append("⚠️  No partner hospitals configured — writer will fall back to generic names")
    if not INTERNAL_LINKS:
        issues.append("⚠️  No internal links configured — blog outputs won't suggest internal linking")

    citation_total = _count_citations()
    if citation_total < 5:
        issues.append(f"⚠️  Only {citation_total} citations in allowlist — writer may be over-restricted")

    missing_google_creds = [
        name for name in GOOGLE_CREDS_ENV_VARS
        if not os.environ.get(name, "").strip()
    ]

    if missing_google_creds:
        issues.append(
            "Missing Google service account env vars: "
            + ", ".join(missing_google_creds)
        )

    private_key = os.environ.get("private_key", "")
    if private_key and "\\n" not in private_key:
        issues.append(
            "Google private_key may be incorrectly formatted. "
            "It should contain escaped \\n characters."
        )
    
    return issues


if __name__ == "__main__":
    print("═" * 60)
    print("  config.py — self-test")
    print("═" * 60)
    print(f"  API keys           : all 4 loaded from env")
    print(f"  Spreadsheet        : {SPREADSHEET_NAME}")
    print(f"  Brand              : {BRAND['name']} ({BRAND['website']})")
    print(f"  Model              : {GEMINI_MODEL}")
    print(f"  Countries          : {len(COUNTRY_MAP)}")
    print(f"  Personas           : {len(COUNTRY_PERSONAS)}")
    print(f"  Specialties        : {len(SPECIALTY_PATTERNS)}")
    print(f"  Partner hospitals  : {sum(len(v) for v in PARTNER_HOSPITALS.values())}")
    print(f"  Internal links     : {len(INTERNAL_LINKS)}")
    print(f"  Citation allowlist : {sum(len(v) for v in CITATION_ALLOWLIST.values())}")
    print(f"  Reading grade      : Class {TARGET_READING_GRADE}")
    print(f"  Word cap           : {HARD_CAP_WORDS}")
    print(f"  Target FAQs        : {TARGET_FAQ_COUNT}")
    print()

    issues = _validate()
    if issues:
        print("  ⚠️  Things to fill in for best output quality:")
        for issue in issues:
            print(f"     • {issue}")
    else:
        print("  ✅ All checks passed")
    print("═" * 60)
