from app.database import insert_many
from psycopg2.extras import Json


def save_serp_urls(run_id: int, rows: list[dict]) -> None:
    if not rows:
        return

    values = [
        (
            run_id,
            row["keyword"],
            row["country_code"],
            row["rank"],
            row["url"],
        )
        for row in rows
    ]

    sql = """
        INSERT INTO serp_results
            (run_id, keyword, country_code, rank, url)
        VALUES %s
    """

    insert_many(sql, values)

def save_paa_questions(run_id: int, rows: list[dict]) -> None:
    if not rows:
        return

    values = [
        (
            run_id,
            row["keyword"],
            row["country_code"],
            row["rank"],
            row["question"],
            row["snippet"],
            row["source"],
            row["source_url"],
        )
        for row in rows
    ]

    sql = """
        INSERT INTO paa_questions
            (run_id, keyword, country_code, position, question, snippet, source, source_url)
        VALUES %s
    """
    #postion here simple means the ordering of the questions
    insert_many(sql, values)


def save_autocomplete_suggestions(run_id: int, rows: dict) -> None:
    if not rows:
        return

    values = []

    for (keyword, country_code, modifier), suggestions in rows.items():
        for position, suggestion in enumerate(suggestions, start=1):
            values.append(
                (
                    run_id,
                    keyword,
                    country_code,
                    "autocomplete",
                    modifier,
                    position,
                    suggestion,
                )
            )

    sql = """
        INSERT INTO search_suggestions
            (run_id, keyword, country_code, source, modifier, position, suggestion)
        VALUES %s
    """

    insert_many(sql, values)


def save_related_searches(run_id: int, rows: list[dict]) -> None:
    if not rows:
        return

    values = [
        (
            run_id,
            row["keyword"],
            row["country_code"],
            "related",
            row["type"],
            row["position"],
            row["query"],
            row["source"],
        )
        for row in rows
    ]

    sql = """
        INSERT INTO search_suggestions
            (run_id, keyword, country_code, source, modifier, position, suggestion, metadata_source)
        VALUES %s
    """

    insert_many(sql, values)

def save_selected_urls(run_id: int, urls: list[str]) -> None:
    if not urls:
        return

    values = [
        (
            run_id,
            url,
            "manual",
            position,
        )
        for position, url in enumerate(urls, start=1)
    ]

    sql = """
        INSERT INTO selected_urls
            (run_id, url, source, position)
        VALUES %s
    """

    insert_many(sql, values)


def get_serp_urls_for_run(run_id: int) -> list[dict]:
    from app.database import fetch_all

    return fetch_all(
        """
        SELECT id, keyword, country_code, rank, url
        FROM serp_results
        WHERE run_id = %s
        ORDER BY keyword ASC, rank ASC
        """,
        (run_id,),
    )

def get_selected_urls(run_id: int) -> list[str]:
    from app.database import fetch_all

    rows = fetch_all(
        """
        SELECT url
        FROM selected_urls
        WHERE run_id = %s
        ORDER BY position ASC
        """,
        (run_id,),
    )

    return [r["url"] for r in rows]


def insert_competitor_page(
    run_id: int,
    url: str,
    scrape_method: str,
    raw_r2_key: str,
    clean_r2_key: str,
    h1_data: str,
    h2_data: str,
    h3_data: str,
    faqs_json,
    others_json,
    meta_title: str,
    meta_title_source: str,
    status: str = "success",
    error_message: str = None,
):
    from app.database import execute

    execute(
        """
        INSERT INTO competitor_pages (
            run_id, url, scrape_method,
            raw_r2_key, clean_r2_key,
            h1_data, h2_data, h3_data,
            faqs_json, others_json,
            meta_title, meta_title_source,
            status, error_message
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id, url, scrape_method,
            raw_r2_key, clean_r2_key,
            h1_data, h2_data, h3_data,
            Json(faqs_json), Json(others_json),
            meta_title, meta_title_source,
            status, error_message,
        ),
    )


def insert_competitor_keywords(
    run_id: int,
    url: str,
    primary_keyword: str,
    specialty: str,
    extracted_json,
    summary: str,
    keyword_count: int,
):
    from app.database import execute

    execute(
        """
        INSERT INTO competitor_keywords (
            run_id, url, primary_keyword,
            specialty, extracted_json,
            summary, keyword_count
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id, url, primary_keyword,
            specialty, Json(extracted_json),
            summary, keyword_count,
        ),
    )


def insert_reddit_insight(
    run_id: int,
    keyword: str,
    country: str,
    item_type: str,
    subreddit: str,
    title_or_body: str,
    score: int,
    emotions: str,
    url: str,
):
    from app.database import execute

    execute(
        """
        INSERT INTO reddit_insights (
            run_id, keyword, country_code, item_type, subreddit,
            title_or_body, score, emotions, url
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id, keyword, country, item_type, subreddit,
            title_or_body, score, emotions, url,
        ),
    )


def insert_reddit_markdown(
    run_id: int,
    keyword: str,
    r2_key: str,
):
    from app.database import execute

    execute(
        """
        INSERT INTO reddit_insights_md (
            run_id, keyword, r2_key
        )
        VALUES (%s, %s, %s)
        """,
        (
            run_id,
            keyword,
            r2_key,
        ),
    )


def insert_forum_search_result(
    run_id: int,
    keyword: str,
    source_type: str,
    title: str,
    snippet: str,
    url: str,
    display_link: str,
):
    from app.database import execute

    execute(
        """
        INSERT INTO forum_search_results (
            run_id, keyword, source_type,
            title, snippet, url, display_link
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            keyword,
            source_type,
            title,
            snippet,
            url,
            display_link,
        ),
    )

def insert_forum_master_row(
    run_id: int,
    source: str,
    detected_country: str,
    insight_text: str,
    emotion_tags: str,
    upvotes: int,
    url: str,
    raw_title: str,
):
    from app.database import execute

    execute(
        """
        INSERT INTO forum_master_raw (
            run_id, source, detected_country,
            insight_text, emotion_tags,
            upvotes, url, raw_title
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            source,
            detected_country,
            insight_text,
            emotion_tags,
            upvotes,
            url,
            raw_title,
        ),
    )


def insert_forum_master_insight(
    run_id: int,
    source: str,
    detected_country: str,
    insight_type: str,
    journey_stage: str,
    clean_insight: str,
    emotion_tags: str,
    priority_score: int,
    insight_text: str,
    upvotes: int,
    url: str,
    raw_title: str,
):
    from app.database import execute

    execute(
        """
        INSERT INTO forum_master_insights (
            run_id, source, detected_country,
            insight_type, journey_stage, clean_insight,
            emotion_tags, priority_score, insight_text,
            upvotes, url, raw_title
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            source,
            detected_country,
            insight_type,
            journey_stage,
            clean_insight,
            emotion_tags,
            priority_score,
            insight_text,
            upvotes,
            url,
            raw_title,
        ),
    )


def insert_forum_master_md(
    run_id: int,
    insight_type: str,
    r2_key: str,
):
    from app.database import execute

    execute(
        """
        INSERT INTO forum_master_md (
            run_id, insight_type, r2_key
        )
        VALUES (%s, %s, %s)
        """,
        (
            run_id,
            insight_type,
            r2_key,
        ),
    )

