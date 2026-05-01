import os
import hashlib
import boto3
from botocore.config import Config

_r2_client = None


def require_env(name:str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def get_r2_client():
    global _r2_client

    if _r2_client is None:
        account_id = require_env("R2_ACCOUNT_ID")

        _r2_client = boto3.client(
            "s3",
            endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=require_env("R2_ACCESS_KEY_ID"),
            aws_secret_access_key=require_env("R2_SECRET_ACCESS_KEY"),
            config=Config(signature_version="s3v4"),
            region_name="auto",
        )

    return _r2_client


def r2_put_text(key: str, content: str, content_type: str = "text/plain") -> str:
    get_r2_client().put_object(
        Bucket=require_env("R2_BUCKET_NAME"),
        Key=key,
        Body=content.encode("utf-8"),
        ContentType=content_type,
    )
    return key


def r2_get_text(key: str) -> str:
    obj = get_r2_client().get_object(
        Bucket=require_env("R2_BUCKET_NAME"),
        Key=key,
    )
    return obj["Body"].read().decode("utf-8")


def hash_url(url: str) -> str:
    return hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]


def blog_final_key(run_id: int) -> str:
    return f"blog/{run_id}/final.md"


def blog_section_key(run_id: int, section_index: int) -> str:
    return f"blog/{run_id}/sections/section_{section_index:02d}.md"


def scrape_raw_key(run_id: int, url: str) -> str:
    return f"scrapes/{run_id}/{hash_url(url)}/raw.txt"


def scrape_clean_key(run_id: int, url: str) -> str:
    return f"scrapes/{run_id}/{hash_url(url)}/clean.txt"