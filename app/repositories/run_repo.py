from app.database import fetch_all, fetch_one, insert_many

def create_run(primary_keyword: str, created_by: str = "anonymous") -> int:
    row = fetch_one(
        """
        INSERT INTO runs (primary_keyword, created_by)
        VALUES (%s, %s)
        RETURNING id
        """,
        (primary_keyword, created_by),
    )

    return row["id"]

def insert_run_keywords(run_id: int, keyword_rows: list[dict]) -> None:
    if not keyword_rows:
        return

    values = [
        (
            run_id,
            row["keyword"],
            row["country_code"],
        )
        for row in keyword_rows
    ]

    sql = """
        INSERT INTO run_keywords
            (run_id, keyword, country_code)
        VALUES %s
    """

    insert_many(sql, values)

def get_run_keywords(run_id: int) -> list[dict]:
    return fetch_all(
        """
        SELECT keyword, country_code
        FROM run_keywords
        WHERE run_id = %s
        ORDER BY id ASC
        """,
        (run_id,),
    )

def get_run_country_codes(run_id: int) -> list[str]:
    rows = fetch_all(
        """
        SELECT DISTINCT country_code
        FROM run_keywords
        WHERE run_id = %s
        ORDER BY country_code ASC
        """,
        (run_id,),
    )

    return [row["country_code"] for row in rows]


def get_country_codes_for_runs(run_ids: list[int]) -> dict[int, list[str]]:
    if not run_ids:
        return {}

    rows = fetch_all(
        """
        SELECT run_id, country_code
        FROM (
            SELECT DISTINCT run_id, country_code
            FROM run_keywords
            WHERE run_id = ANY(%s::int[])
        ) unique_run_countries
        ORDER BY run_id ASC, country_code ASC
        """,
        (run_ids,),
    )

    country_codes_by_run_id: dict[int, list[str]] = {}

    for row in rows:
        country_codes_by_run_id.setdefault(row["run_id"], []).append(row["country_code"])

    return country_codes_by_run_id