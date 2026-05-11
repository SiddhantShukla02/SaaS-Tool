from app.database import fetch_all, fetch_one


def get_generated_output(run_id: int, output_type: str) -> dict | None:
    return fetch_one(
        """
        SELECT output_type, r2_key, metadata_json
        FROM generated_outputs
        WHERE run_id = %s
          AND output_type = %s
        ORDER BY id DESC
        LIMIT 1
        """,
        (run_id, output_type),
    )


def get_generated_outputs(run_id: int, output_types: list[str]) -> list[dict]:
    if not output_types:
        return []

    placeholders = ", ".join(["%s"] * len(output_types))

    return fetch_all(
        f"""
        SELECT output_type, r2_key, metadata_json
        FROM generated_outputs
        WHERE run_id = %s
          AND output_type IN ({placeholders})
        ORDER BY id ASC
        """,
        (run_id, *output_types),
    )


def get_question_bank_output(run_id: int) -> dict | None:
    return get_generated_output(run_id, "question_bank")


def get_platform_draft_output(run_id: int, platform: str) -> dict | None:
    output_type_by_platform = {
        "quora": "quora_drafts",
        "reddit": "reddit_drafts",
        "substack": "substack_drafts",
    }

    output_type = output_type_by_platform.get(platform)

    if not output_type:
        raise ValueError(f"Unknown platform: {platform}")

    return get_generated_output(run_id, output_type)