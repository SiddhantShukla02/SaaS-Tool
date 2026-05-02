from app.database import insert_many


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