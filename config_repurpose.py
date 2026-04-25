"""
═══════════════════════════════════════════════════════════════════════
config_repurpose.py — Additional config for the Quora/Reddit/Substack
                     repurposing pipeline.

Import this alongside config.py:

    from config import *
    from config_repurpose import *
═══════════════════════════════════════════════════════════════════════
"""

# ═══════════════════════════════════════════════════════════════════════
# NEW SHEET TABS — written by the repurposing pipeline
# ═══════════════════════════════════════════════════════════════════════
REPURPOSE_TABS = {
    "question_bank":   "Question_Bank",        # unified, dedup'd question pool
    "quora_drafts":    "Quora_Drafts",         # ready-to-review Quora answers
    "reddit_drafts":   "Reddit_Drafts",        # ready-to-review Reddit posts/comments
    "substack_drafts": "Substack_Drafts",      # ready-to-publish Substack essays
    "publish_log":     "Repurpose_Publish_Log",  # tracks what's been posted where
}

# ═══════════════════════════════════════════════════════════════════════
# PLATFORM — word-count targets by platform + intent
# ═══════════════════════════════════════════════════════════════════════
PLATFORM_SPECS = {
    "quora": {
        "target_words":        800,
        "min_words":           500,
        "max_words":           1200,
        "tone":                "authoritative, first-person consultant",
        "include_cta":         True,
        "cta_style":           "soft",       # "soft" | "none" | "direct"
        "citation_density":    2,             # cite 2 authorities per answer
        "include_disclaimer":  True,
        "title_style":         "question",    # repeat the question as title
    },
    "reddit": {
        "target_words":        400,
        "min_words":           200,
        "max_words":           700,
        "tone":                "plain, empathetic, non-promotional",
        "include_cta":         False,         # Reddit anti-promo rules
        "cta_style":           "none",
        "citation_density":    1,
        "include_disclaimer":  False,         # feels corporate on Reddit
        "title_style":         "conversational",
    },
    "substack": {
        "target_words":        2000,
        "min_words":           1500,
        "max_words":           3000,
        "tone":                "editorial, narrative, warmly expert",
        "include_cta":         True,
        "cta_style":           "direct",
        "citation_density":    4,
        "include_disclaimer":  True,
        "title_style":         "editorial",   # evocative headline
        "questions_per_essay": 6,             # cluster N questions into one essay
    },
}

# ═══════════════════════════════════════════════════════════════════════
# SUBREDDIT ALLOWLIST — where we'd actually post, by topic
# Edit this carefully. Most subreddits ban self-promotion entirely.
# Fill in only subreddits you've read and confirmed allow helpful, clearly-
# identified answers from an industry person. Format: subreddit_name (no r/)
# ═══════════════════════════════════════════════════════════════════════
SUBREDDIT_ALLOWLIST = {
    # by specialty — subreddits where answering medical-tourism questions
    # from a known industry account is welcome
    "fertility":       [],  # e.g. "infertility", "IVF"
    "cardiac":         [],  # research each before enabling
    "orthopedic":      [],
    "oncology":        [],
    "ophthalmology":   [],
    "dental":          [],
    "bariatric":       [],
    "cosmetic":        [],
    "transplant":      [],
    "Urology":         [],
    "Gastroenterology":[],
    "Gynecology":      [],
    "Hematology":     [],
    "Neuro":         [],
    "ENT":         [],
    "Vascular":         [],
    "Rehabilitation":         [],
    "Radiology":         [],
    "Paediatric":         [],
    "Orthopedics":         [],
    "Organ Transplant":         [],
    "Obstetrics":         [],
    "Ophthalmology":         [],
    "Nephrology":         [],
    "Bariatric Surgery":         [],
    "Metabolic Surgery":         [],
    "general":         [],  # e.g. "MedicalTourism"
}

# ═══════════════════════════════════════════════════════════════════════
# REDDIT POSTING POLICY — enforced per response
# ═══════════════════════════════════════════════════════════════════════
REDDIT_POLICY = {
    "require_disclosure":    True,  # add "I work in medical tourism..." disclosure
    "disclosure_text":       "Disclosure: I work in medical tourism and help "
                              "patients navigate treatment abroad. Happy to "
                              "answer follow-ups without promoting anyone.",
    "ban_brand_mentions":    True,   # no "Divinheal" in Reddit drafts
    "ban_explicit_cta":      True,   # no "visit our website"
    "ban_link_to_own_site":  True,   # conservative default; override per post
    "require_human_review":  True,   # pipeline flags, never auto-posts
}

# ═══════════════════════════════════════════════════════════════════════
# QUORA POSTING POLICY
# ═══════════════════════════════════════════════════════════════════════
QUORA_POLICY = {
    "require_disclosure":    True,
    "disclosure_text":       "(I work at Divinheal, a medical tourism platform. "
                              "Answer is based on our patient experience and "
                              "published data — not a paid placement.)",
    "allow_brand_mentions":  True,   # Quora is friendlier to brand mention
    "allow_soft_cta":        True,
    "max_cta_lines":         1,
    "require_human_review":  True,
}

# ═══════════════════════════════════════════════════════════════════════
# SUBSTACK SETTINGS (for future API auto-publish)
# ═══════════════════════════════════════════════════════════════════════
SUBSTACK_SETTINGS = {
    "publication_url":       "",   # e.g. "https://divinheal.substack.com"
    "default_category":      "Medical tourism",
    "default_tags":          ["medical tourism", "india", "healthcare costs"],
    "auto_publish":          False,  # keep False until API integration complete
    "scheduled_publish":     True,   # prefer scheduling over immediate
}

# ═══════════════════════════════════════════════════════════════════════
# QUESTION BANK — filters and priority weights
# ═══════════════════════════════════════════════════════════════════════
QUESTION_BANK_FILTERS = {
    "min_question_length":      20,      # chars — filter out 1-word fragments
    "max_question_length":      200,
    "dedup_similarity":         0.75,    # Jaccard threshold for near-duplicates
    "exclude_if_matches": [              # regex patterns to drop
        r"^\s*\?+\s*$",                   # just punctuation
        r"^\s*\d+\s*$",                   # just numbers
        r"\b(viagra|porn|crypto)\b",      # spam indicators
    ],
}

# Priority score weights for question ranking
QUESTION_PRIORITY_WEIGHTS = {
    "source_paa":              3.0,   # PAA = proven Google demand
    "source_autocomplete":     2.0,   # autocomplete = real search
    "source_related":          1.5,
    "source_forum_question":   2.5,   # high intent
    "source_forum_objection":  2.8,   # highest commercial intent
    "source_competitor_faq":   2.0,
    "contains_cost":           1.2,   # commercial boost
    "contains_safety":         1.3,   # YMYL-important
    "contains_country":        1.4,   # localised intent
    "has_commercial_terms":    1.2,   # best/top/vs/compare
}

# ═══════════════════════════════════════════════════════════════════════
# Self-test
# ═══════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("✅ config_repurpose.py loaded")
    print(f"   New sheet tabs      : {len(REPURPOSE_TABS)}")
    print(f"   Platforms configured: {', '.join(PLATFORM_SPECS.keys())}")
    allowlisted = sum(len(v) for v in SUBREDDIT_ALLOWLIST.values())
    print(f"   Subreddits allowed  : {allowlisted}")
    if allowlisted == 0:
        print("   ⚠️  SUBREDDIT_ALLOWLIST is empty — Reddit drafts will generate "
               "but the 'suggested_subreddit' field will be blank until you fill "
               "in subreddits your team has vetted.")
