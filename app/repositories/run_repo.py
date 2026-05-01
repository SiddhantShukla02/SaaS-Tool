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