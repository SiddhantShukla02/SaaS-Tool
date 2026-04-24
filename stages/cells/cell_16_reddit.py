# ─── NOTE: This cell now imports keys from config.py ────────
# If you haven't set up config.py yet, see README_REVISION.md
try:
    from config import (SERP_API_KEY, GEMINI_API_KEY,
                         FIRECRAWL_API_KEY, BRAVE_API_KEY,
                         SPREADSHEET_NAME,
                         GEMINI_MODEL, COUNTRY_MAP, SAFETY_OFF, SCOPES)
except ImportError:
    print('⚠️ config.py not found — falling back to globals from Cell 1')
# ────────────────────────────────────────────────────────────

"""
Reddit Medical Tourism Insights Collector
==========================================
Collects patient discussions from Reddit for SEO content enrichment.
Uses Reddit's public JSON endpoints (no authentication required).

Reads  : Keyword_n8n → "keyword" tab (Keyword + Country_Code)
Writes : Keyword_n8n → "Reddit_Insights" tab
Also   : Exports prompt-ready markdown to "Reddit_Insights_MD" tab

Usage in notebook: just run this cell after Cells 1-5.
"""

import requests
import json
import time
import re
import gspread
from datetime import datetime
from app.utils.helper import get_sheet_client

# ── Config ────────────────────────────────────────────────────────────
SHEET_NAME        = SPREADSHEET_NAME
INPUT_TAB         = "keyword"
OUTPUT_TAB        = "Reddit_Insights"
OUTPUT_MD_TAB     = "Reddit_Insights_MD"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ── Auth ──────────────────────────────────────────────────────────────
gc = get_sheet_client(SCOPES)
# ── Reddit Collector Class ────────────────────────────────────────────

class RedditInsightsCollector:
    """Collects and processes Reddit discussions for medical tourism SEO content."""

    BASE_URL = "https://www.reddit.com"
    HEADERS  = {
        "User-Agent": "DivinhealSEOResearch/1.0 (Medical Tourism Content Research)"
    }

    DEFAULT_SUBREDDITS = [
        "medicaltourism", "health", "india", "askdocs", "medical",
        "healthcare", "personalfinance", "travel", "iwantout",
    ]

    EMOTION_MARKERS = {
        "fear": [
            "scared", "terrified", "afraid", "worried", "nervous",
            "anxious", "fear", "nightmare", "panic", "dread",
        ],
        "financial_stress": [
            "can't afford", "expensive", "cost", "price", "insurance",
            "bankrupt", "debt", "saving", "financial", "budget",
            "loan", "money",
        ],
        "trust_deficit": [
            "scam", "trust", "legit", "reliable", "reviews", "fake",
            "fraud", "shady", "suspicious", "rip off", "cheat",
        ],
        "urgency": [
            "urgent", "emergency", "dying", "critical", "immediately",
            "time running", "can't wait", "asap", "desperate",
        ],
        "hope": [
            "hope", "miracle", "chance", "success", "recovered",
            "saved", "worked", "grateful", "blessed", "amazing",
        ],
        "overwhelm": [
            "confused", "overwhelmed", "don't know", "help me",
            "where to start", "lost", "complicated", "too much",
        ],
        "quality_concern": [
            "quality", "hygiene", "clean", "standard", "accredited",
            "safe", "infection", "complication", "risk", "death rate",
        ],
        "logistics_worry": [
            "visa", "travel", "language", "alone", "companion",
            "accommodation", "food", "transport", "airport", "hotel",
            "stay", "recovery abroad",
        ],
    }

    def __init__(self, rate_limit_seconds=2.0):
        self.rate_limit = rate_limit_seconds
        self.last_request_time = 0

    def _rate_limited_get(self, url, params=None):
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        try:
            response = requests.get(
                url, headers=self.HEADERS, params=params, timeout=20
            )
            self.last_request_time = time.time()
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                print("  [RATE LIMITED] Waiting 60s...")
                time.sleep(60)
                return self._rate_limited_get(url, params)
            else:
                print(f"  [ERROR] Status {response.status_code} for {url[:80]}")
                return None
        except Exception as e:
            print(f"  [ERROR] Request failed: {e}")
            return None

    def search_subreddit(self, subreddit, keyword, limit=25, sort="relevance"):
        url = f"{self.BASE_URL}/r/{subreddit}/search.json"
        params = {
            "q": keyword, "restrict_sr": "on", "sort": sort,
            "limit": min(limit, 100), "t": "all",
        }
        data = self._rate_limited_get(url, params)
        if not data or "data" not in data:
            return []
        posts = []
        for child in data["data"].get("children", []):
            post = child.get("data", {})
            posts.append({
                "subreddit": subreddit,
                "title": post.get("title", ""),
                "selftext": post.get("selftext", ""),
                "score": post.get("score", 0),
                "num_comments": post.get("num_comments", 0),
                "url": f"https://www.reddit.com{post.get('permalink', '')}",
                "created_utc": post.get("created_utc", 0),
                "author": post.get("author", "[deleted]"),
            })
        return posts

    def search_all_reddit(self, keyword, limit=50, sort="relevance"):
        url = f"{self.BASE_URL}/search.json"
        params = {"q": keyword, "sort": sort, "limit": min(limit, 100), "t": "all"}
        data = self._rate_limited_get(url, params)
        if not data or "data" not in data:
            return []
        posts = []
        for child in data["data"].get("children", []):
            post = child.get("data", {})
            posts.append({
                "subreddit": post.get("subreddit", ""),
                "title": post.get("title", ""),
                "selftext": post.get("selftext", ""),
                "score": post.get("score", 0),
                "num_comments": post.get("num_comments", 0),
                "url": f"https://www.reddit.com{post.get('permalink', '')}",
                "created_utc": post.get("created_utc", 0),
                "author": post.get("author", "[deleted]"),
            })
        return posts

    def get_post_comments(self, post_url, limit=50):
        json_url = post_url.rstrip("/") + ".json"
        params = {"limit": limit, "sort": "best"}
        data = self._rate_limited_get(json_url, params)
        if not data or len(data) < 2:
            return []
        comments = []
        self._extract_comments(
            data[1].get("data", {}).get("children", []), comments, 0
        )
        return comments

    def _extract_comments(self, children, comments, depth):
        for child in children:
            if child.get("kind") != "t1":
                continue
            cd = child.get("data", {})
            body = cd.get("body", "")
            if body and body not in ("[deleted]", "[removed]"):
                comments.append({
                    "body": body,
                    "score": cd.get("score", 0),
                    "author": cd.get("author", "[deleted]"),
                    "depth": depth,
                })
                replies = cd.get("replies", "")
                if isinstance(replies, dict):
                    self._extract_comments(
                        replies.get("data", {}).get("children", []),
                        comments, depth + 1,
                    )

    def detect_emotions(self, text):
        text_lower = text.lower()
        detected = []
        for emotion, markers in self.EMOTION_MARKERS.items():
            if any(m in text_lower for m in markers):
                detected.append(emotion)
        return detected

    def collect_insights(self, keyword, subreddits=None, posts_per_subreddit=10,
                         also_search_all=True, fetch_comments=True, max_comment_posts=5):
        if subreddits is None:
            subreddits = self.DEFAULT_SUBREDDITS

        all_posts = []
        all_comments = []

        print(f"\n{'='*60}")
        print(f"Collecting Reddit insights for: '{keyword}'")
        print(f"{'='*60}")

        # Step 1: Search each subreddit
        for sub in subreddits:
            print(f"\n  Searching r/{sub}...")
            posts = self.search_subreddit(sub, keyword, limit=posts_per_subreddit)
            all_posts.extend(posts)
            print(f"  Found {len(posts)} posts")

        # Step 2: Search all of Reddit
        if also_search_all:
            print(f"\n  Searching all of Reddit...")
            global_posts = self.search_all_reddit(keyword, limit=30)
            existing_urls = {p["url"] for p in all_posts}
            new_posts = [p for p in global_posts if p["url"] not in existing_urls]
            all_posts.extend(new_posts)
            print(f"  Found {len(new_posts)} additional posts")

        # Step 3: Fetch comments for top posts
        if fetch_comments and all_posts:
            top_posts = sorted(
                all_posts, key=lambda p: p["score"] + p["num_comments"], reverse=True
            )[:max_comment_posts]
            print(f"\n  Fetching comments for top {len(top_posts)} posts...")
            for post in top_posts:
                comments = self.get_post_comments(post["url"], limit=30)
                for c in comments:
                    c["source_post_title"] = post["title"]
                    c["source_post_url"] = post["url"]
                all_comments.extend(comments)
                print(f"  {post['title'][:60]}... -> {len(comments)} comments")

        # Step 4: Emotion analysis
        print(f"\n  Analyzing emotions...")
        emotion_counts = {e: 0 for e in self.EMOTION_MARKERS}
        emotion_examples = {e: [] for e in self.EMOTION_MARKERS}

        for post in all_posts:
            text = f"{post['title']} {post['selftext']}"
            emotions = self.detect_emotions(text)
            for e in emotions:
                emotion_counts[e] += 1
                if len(emotion_examples[e]) < 3:
                    emotion_examples[e].append({
                        "text": (text[:200] + "...") if len(text) > 200 else text,
                        "url": post["url"],
                    })

        for comment in all_comments:
            emotions = self.detect_emotions(comment["body"])
            for e in emotions:
                emotion_counts[e] += 1
                if len(emotion_examples[e]) < 3:
                    emotion_examples[e].append({
                        "text": (comment["body"][:200] + "...") if len(comment["body"]) > 200 else comment["body"],
                        "url": comment.get("source_post_url", ""),
                    })

        # Step 5: Extract questions
        questions = []
        question_pattern = re.compile(r'[^.!?]*\?')
        for post in all_posts:
            found = question_pattern.findall(f"{post['title']} {post['selftext']}")
            for q in found:
                q = q.strip()
                if 15 < len(q) < 300:
                    questions.append({"question": q, "source": post["url"]})
        for comment in all_comments:
            found = question_pattern.findall(comment["body"])
            for q in found:
                q = q.strip()
                if 15 < len(q) < 300:
                    questions.append({"question": q, "source": comment.get("source_post_url", "")})

        results = {
            "keyword": keyword,
            "collection_date": datetime.now().isoformat(),
            "total_posts": len(all_posts),
            "total_comments": len(all_comments),
            "posts": all_posts,
            "comments": all_comments,
            "emotion_analysis": {
                "counts": emotion_counts,
                "top_emotions": sorted(emotion_counts.items(), key=lambda x: x[1], reverse=True),
                "examples": emotion_examples,
            },
            "extracted_questions": questions[:50],
            "subreddits_with_results": list(set(p["subreddit"] for p in all_posts)),
        }

        print(f"\n{'='*60}")
        print(f"COLLECTION COMPLETE")
        print(f"  Posts: {len(all_posts)}")
        print(f"  Comments: {len(all_comments)}")
        print(f"  Questions extracted: {len(questions)}")
        top_3 = results['emotion_analysis']['top_emotions'][:3]
        print(f"  Top emotions: {top_3}")
        print(f"{'='*60}")
        return results

    def generate_prompt_ready_markdown(self, results):
        """Generate markdown output ready for pasting into content writer prompts."""
        lines = []
        lines.append(f"# Forum/Reddit Insights: {results['keyword']}")
        lines.append(f"*Collected: {results['collection_date']}*")
        lines.append(f"*Posts: {results['total_posts']} | Comments: {results['total_comments']}*")
        lines.append("")

        # Emotion Map
        lines.append("## Emotion Map")
        lines.append("")
        lines.append("| Emotion | Frequency | Example |")
        lines.append("|---------|-----------|---------|")
        for emotion, count in results["emotion_analysis"]["top_emotions"]:
            if count > 0:
                examples = results["emotion_analysis"]["examples"].get(emotion, [])
                ex_text = examples[0]["text"][:100].replace("|", "-").replace("\n", " ") + "..." if examples else "-"
                lines.append(f"| {emotion} | {count} mentions | {ex_text} |")
        lines.append("")

        # Patient Questions
        lines.append("## Real Patient Questions")
        lines.append("")
        for i, q in enumerate(results["extracted_questions"][:20], 1):
            lines.append(f"{i}. {q['question'].replace(chr(10), ' ').strip()}")
        lines.append("")

        # Top Post Snippets
        lines.append("## Top Forum Post Snippets (for content writer prompt)")
        lines.append("")
        top_posts = sorted(results["posts"], key=lambda p: p["score"] + p["num_comments"], reverse=True)[:15]
        for post in top_posts:
            title = post["title"].replace("\n", " ")
            body = post["selftext"][:300].replace("\n", " ") if post["selftext"] else "(no body)"
            lines.append(f"- **r/{post['subreddit']}** (score: {post['score']}, {post['num_comments']} comments)")
            lines.append(f'  Title: "{title}"')
            lines.append(f'  Body: "{body}"')
            lines.append(f"  URL: {post['url']}")
            lines.append("")

        # Top Comments
        lines.append("## Top Comment Snippets (richest patient insights)")
        lines.append("")
        top_comments = sorted(results["comments"], key=lambda c: c["score"], reverse=True)[:15]
        for comment in top_comments:
            body = comment["body"][:300].replace("\n", " ")
            emotions = self.detect_emotions(comment["body"])
            emotion_tags = ", ".join(emotions) if emotions else "neutral"
            lines.append(f"- (score: {comment['score']}, emotions: {emotion_tags})")
            lines.append(f'  "{body}"')
            lines.append(f"  Source: {comment.get('source_post_url', 'N/A')}")
            lines.append("")

        return "\n".join(lines)


# ── Sheet helpers ─────────────────────────────────────────────────────
MAX_CELL = 49000

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
                print(f"  ⚠️ Retry {attempt+1}/{retries}: {e}")
                time.sleep(wait)
            else:
                print(f"  ❌ Sheet write failed: {e}")
                return False

def _fmt_header(ws, n_cols):
    col_letter = chr(64 + min(n_cols, 26))
    ws.format(f"A1:{col_letter}1", {
        "textFormat": {"bold": True, "foregroundColor": {"red":1,"green":1,"blue":1}},
        "backgroundColor": {"red": 0.13, "green": 0.37, "blue": 0.60}
    })

def get_or_create_tab(spreadsheet, tab_name, rows=3000, cols=10):
    try:
        ws = spreadsheet.worksheet(tab_name)
        ws.clear()
        print(f"  📋 Tab '{tab_name}' cleared.")
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=rows, cols=cols)
        print(f"  📋 Tab '{tab_name}' created.")
    return ws


# ═══════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════

print("\n" + "="*65)
print("🔍 REDDIT INSIGHTS COLLECTOR")
print("="*65)

sp = gc.open(SHEET_NAME)

# Read keywords
ws_kw   = sp.worksheet(INPUT_TAB)
records = ws_kw.get_all_records()

collector = RedditInsightsCollector()

all_results = []

for row in records:
    keyword = str(row.get("Keyword", "")).strip()
    country = str(row.get("Country_Code", "")).strip()
    if not keyword:
        continue

    # Search with keyword variations
    keyword_variations = [keyword]
    if country:
        keyword_variations.append(f"{keyword} from {country}")
        # Add treatment-focused variation
        first_word = keyword.split()[0] if keyword.split() else keyword
        keyword_variations.append(f"medical tourism India {first_word}")

    combined_posts = []
    combined_comments = []
    seen_urls = set()

    for kw_var in keyword_variations:
        results = collector.collect_insights(
            keyword=kw_var,
            posts_per_subreddit=8,
            fetch_comments=True,
            max_comment_posts=3,
        )
        for post in results["posts"]:
            if post["url"] not in seen_urls:
                combined_posts.append(post)
                seen_urls.add(post["url"])
        combined_comments.extend(results["comments"])

    # Re-run emotion analysis on combined data
    emotion_counts = {e: 0 for e in collector.EMOTION_MARKERS}
    emotion_examples = {e: [] for e in collector.EMOTION_MARKERS}

    for post in combined_posts:
        text = f"{post['title']} {post['selftext']}"
        emotions = collector.detect_emotions(text)
        for e in emotions:
            emotion_counts[e] += 1
            if len(emotion_examples[e]) < 3:
                emotion_examples[e].append({
                    "text": (text[:200] + "...") if len(text) > 200 else text,
                    "url": post["url"],
                })

    for comment in combined_comments:
        emotions = collector.detect_emotions(comment["body"])
        for e in emotions:
            emotion_counts[e] += 1
            if len(emotion_examples[e]) < 3:
                emotion_examples[e].append({
                    "text": (comment["body"][:200] + "...") if len(comment["body"]) > 200 else comment["body"],
                    "url": comment.get("source_post_url", ""),
                })

    # Extract questions
    questions = []
    qp = re.compile(r'[^.!?]*\?')
    for post in combined_posts:
        for q in qp.findall(f"{post['title']} {post['selftext']}"):
            q = q.strip()
            if 15 < len(q) < 300:
                questions.append({"question": q, "source": post["url"]})
    for comment in combined_comments:
        for q in qp.findall(comment["body"]):
            q = q.strip()
            if 15 < len(q) < 300:
                questions.append({"question": q, "source": comment.get("source_post_url", "")})

    final_results = {
        "keyword": keyword,
        "country": country,
        "collection_date": datetime.now().isoformat(),
        "total_posts": len(combined_posts),
        "total_comments": len(combined_comments),
        "posts": combined_posts,
        "comments": combined_comments,
        "emotion_analysis": {
            "counts": emotion_counts,
            "top_emotions": sorted(emotion_counts.items(), key=lambda x: x[1], reverse=True),
            "examples": emotion_examples,
        },
        "extracted_questions": questions[:50],
        "subreddits_with_results": list(set(p["subreddit"] for p in combined_posts)),
    }
    all_results.append(final_results)


# ── Relevance filter function ─────────────────────────────────────────
EXCLUDED_SUBREDDITS = {
    "eatcheapandhealthy", "cooking", "food", "recipes", "mealprep",
    "coolguides", "askreddit", "tifu", "gaming", "memes", "funny",
    "aww", "pics", "todayilearned", "lifeprotips", "showerthoughts",
    "music", "eu4", "wallstreetbets", "cryptocurrency", "stocks",
    "sports", "nfl", "nba", "soccer", "movies", "television",
}

# Universal medical terms — match ANY treatment type
UNIVERSAL_MEDICAL_TERMS = {
    "cost", "treatment", "medical tourism", "hospital", "doctor",
    "india", "medical", "patient", "procedure", "surgery",
    "clinic", "specialist", "diagnosis", "therapy", "affordable",
    "insurance", "recovery", "consultation", "accredited", "quality",
    "healthcare", "experience", "side effect", "risk", "success rate",
    "price", "expense", "savings", "abroad", "overseas",
}

def build_topic_keywords(records_list):
    """
    Build topic-specific relevance keywords from the keyword records
    already loaded in memory (no redundant sheet read).
    
    '[keyword] cost in india' → {'[treatment-word]'}
    '[keyword] cost in india' → {'[treatment-word1]', '[treatment-word2]'}
    (words are extracted dynamically from whatever keyword is in the sheet)
    
    Strips generic words (cost, india, in, the) — those are in UNIVERSAL already.
    Keeps ONLY the treatment-specific words that distinguish this topic.
    """
    GENERIC_WORDS = {
        "the", "and", "for", "in", "of", "to", "a", "an", "is", "it",
        "cost", "india", "price", "from", "best", "top", "how", "much",
        "what", "does", "with", "treatment", "medical", "vs", "or",
    }
    topic_words = set()
    for r in records_list:
        kw = str(r.get("Keyword", "")).strip().lower()
        if not kw:
            continue
        # Add the full phrase (for multi-word matching like "egg freezing")
        topic_words.add(kw)
        # Add individual treatment-specific words only
        for w in kw.split():
            if len(w) > 2 and w not in GENERIC_WORDS:
                topic_words.add(w)
    return topic_words

# Build topic keywords from records already in memory
TOPIC_KEYWORDS = build_topic_keywords(records)
print(f"  🔑 Topic-specific filter words: {TOPIC_KEYWORDS}")


def is_relevant_post(post, topic_keywords=None):
    """
    Two-gate relevance filter:
      Gate 1: Post must contain at least 1 TOPIC-SPECIFIC term
              (treatment-specific words extracted from keyword tab)
      Gate 2: Post must ALSO contain at least 2 UNIVERSAL medical terms
              (cost, hospital, treatment, india, etc.)
    
    This ensures:
      ✅ "[topic] cost India hospital" passes (1 topic + 3 universal)
      ❌ "unrelated cost India hospital" fails when topic doesn't match (0 topic match)
      ❌ "[topic] recipe cooking" fails (1 topic but 0 universal)
      ❌ r/gaming post about anything fails (excluded subreddit)
    """
    subreddit = post.get("subreddit", "").lower()
    if subreddit in EXCLUDED_SUBREDDITS:
        return False

    text = f"{post.get('title', '')} {post.get('selftext', '')}".lower()

    # Build the topic-specific check set
    topic_check = set(TOPIC_KEYWORDS)  # from keyword tab
    if topic_keywords:
        for kw in topic_keywords:
            topic_check.add(kw.lower())
            for w in kw.lower().split():
                if len(w) > 2:
                    topic_check.add(w)

    # Gate 1: At least 1 topic-specific term must be present
    topic_matches = sum(1 for term in topic_check if term in text)
    if topic_matches == 0:
        return False

    # Gate 2: At least 2 universal medical terms must also be present
    universal_matches = sum(1 for term in UNIVERSAL_MEDICAL_TERMS if term in text)
    if universal_matches < 2:
        return False

    return True


# ── Filter irrelevant posts (e.g., cooking "egg" matches) ────────────
print(f"\n  🔍 Filtering irrelevant posts...")
for res in all_results:
    before_posts = len(res["posts"])
    before_comments = len(res["comments"])
    res["posts"] = [p for p in res["posts"] if is_relevant_post(p, [res["keyword"]])]
    # Filter comments from irrelevant posts
    relevant_urls = {p["url"] for p in res["posts"]}
    res["comments"] = [c for c in res["comments"] if c.get("source_post_url", "") in relevant_urls or is_relevant_post({"title": "", "selftext": c.get("body", ""), "subreddit": ""}, [res["keyword"]])]
    print(f"  {res['keyword']}: {before_posts}→{len(res['posts'])} posts, {before_comments}→{len(res['comments'])} comments")

# ── Write to Google Sheets ────────────────────────────────────────────

# Tab 1: Raw insights data
out_ws = get_or_create_tab(sp, OUTPUT_TAB, rows=2000, cols=8)
HEADERS = ["Keyword", "Country", "Type", "Subreddit", "Title_or_Body", "Score", "Emotions", "URL"]
rows_out = [HEADERS]

for res in all_results:
    for post in sorted(res["posts"], key=lambda p: p["score"], reverse=True)[:30]:
        text = f"{post['title']} | {post['selftext'][:200]}"
        emotions = collector.detect_emotions(text)
        rows_out.append([
            res["keyword"], res["country"], "post", post["subreddit"],
            _trunc(text), post["score"], "; ".join(emotions), post["url"]
        ])
    for comment in sorted(res["comments"], key=lambda c: c["score"], reverse=True)[:20]:
        emotions = collector.detect_emotions(comment["body"])
        rows_out.append([
            res["keyword"], res["country"], "comment", "",
            _trunc(comment["body"][:300]), comment["score"],
            "; ".join(emotions), comment.get("source_post_url", "")
        ])

_write_with_retry(out_ws, rows_out)
_fmt_header(out_ws, len(HEADERS))
print(f"\n✅ Reddit_Insights tab: {len(rows_out)-1} rows written")

# Tab 2: Prompt-ready markdown
md_ws = get_or_create_tab(sp, OUTPUT_MD_TAB, rows=200, cols=2)
md_rows = [["Keyword", "Prompt_Ready_Markdown"]]

for res in all_results:
    md_text = collector.generate_prompt_ready_markdown(res)
    md_rows.append([res["keyword"], _trunc(md_text)])

_write_with_retry(md_ws, md_rows)
_fmt_header(md_ws, 2)
print(f"✅ Reddit_Insights_MD tab: {len(md_rows)-1} keyword(s) exported")

print(f"\n{'='*65}")
print("✅ REDDIT INSIGHTS COLLECTION COMPLETE")
print(f"{'='*65}")
