#!/usr/bin/env python3
"""
Daily exporter: ClickHouse -> S3 (CSV per day, per-agent avg and 90th percentile)

Environment variables:
  CLICKHOUSE_URL (required)
  CLICKHOUSE_USER
  CLICKHOUSE_PASSWORD
  CLICKHOUSE_DATABASE
  S3_BUCKET (required)
  S3_KEY_PREFIX (optional)
  AWS_REGION
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  EXPORT_DATE (optional YYYY-MM-DD, default = yesterday UTC)
"""
import os
import sys
import datetime
import logging
from pathlib import Path
import argparse

# Ensure we can import from src when executed from repo root
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from config import ExportConfig  # type: ignore
from services.clickhouse_client import ClickHouseClient  # type: ignore
from services.s3_client import S3Client, S3Config  # type: ignore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("export_daily_agent_stats")


def iso_yesterday_utc(override: str | None) -> str:
    if override:
        # validate YYYY-MM-DD
        datetime.datetime.strptime(override, "%Y-%m-%d")
        return override
    today = datetime.datetime.utcnow().date()
    yesterday = today - datetime.timedelta(days=1)
    return yesterday.isoformat()


def build_sql(date_str: str) -> str:
    # Filter by date using toDate(call_start) to avoid timezone/DST boundary issues.
    # Optionally, users can set CLICKHOUSE_TZ to coerce call_start into a timezone before taking the date,
    # e.g. toDate(toTimeZone(call_start, 'UTC')). If CLICKHOUSE_TZ is not set, we use toDate(call_start).
    sql = f"""
SELECT
    agent_id,
    avg(call_duration_sec) AS avg_call_length_sec,
    quantileExact(0.9)(call_duration_sec) AS p90_call_length_sec
FROM conversations
WHERE {{date_expr}} = toDate('{date_str}')
GROUP BY agent_id
ORDER BY agent_id
FORMAT CSVWithNames
"""
    return sql
    return sql


def main() -> int:
    env = os.environ
    parser = argparse.ArgumentParser(description="Export ClickHouse agent stats to S3 or local file")
    parser.add_argument("--no-upload", action="store_true", help="Only query ClickHouse and write local CSV, do not upload to S3")
    args = parser.parse_args()
    clickhouse_url = env.get("CLICKHOUSE_URL")
    s3_bucket = env.get("S3_BUCKET")
    if not clickhouse_url:
        logger.error("CLICKHOUSE_URL must be set as an environment variable")
        return 2
    if not args.no_upload and not s3_bucket:
        logger.error("S3_BUCKET must be set as an environment variable unless --no-upload is used")
        return 2

    cfg = ExportConfig(
        clickhouse_url=clickhouse_url,
        clickhouse_user=env.get("CLICKHOUSE_USER"),
        clickhouse_password=env.get("CLICKHOUSE_PASSWORD"),
        clickhouse_database=env.get("CLICKHOUSE_DATABASE"),
        s3_bucket_name=s3_bucket,
        s3_key_prefix=env.get("S3_KEY_PREFIX"),
        aws_region=env.get("AWS_REGION"),
        aws_access_key_id=env.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=env.get("AWS_SECRET_ACCESS_KEY"),
        utc_date=env.get("EXPORT_DATE"),
    )

    target_date = iso_yesterday_utc(cfg.utc_date)

    # Build SQL using timezone-aware date extraction if CLICKHOUSE_TZ is set
    clickhouse_tz = os.environ.get("CLICKHOUSE_TZ")
    if clickhouse_tz:
        # Use toDate(toTimeZone(call_start, '<tz>')) to compute date in the specified timezone
        date_expr = f"toDate(toTimeZone(call_start, '{clickhouse_tz}'))"
    else:
        # Default: use server's call_start date (avoids time-of-day boundaries/DST issues)
        date_expr = "toDate(call_start)"

    sql_template = build_sql(target_date)
    sql = sql_template.replace("{date_expr}", date_expr)
    logger.info("Querying ClickHouse for date %s", target_date)

    ch = ClickHouseClient(cfg.clickhouse_url, cfg.clickhouse_user, cfg.clickhouse_password, cfg.clickhouse_database)
    try:
        csv_bytes = ch.query_csv(sql)
    except Exception:
        logger.exception("ClickHouse query failed")
        return 3

    filename = f"agent_stats_{target_date}.csv"
    key = f"{cfg.s3_key_prefix.strip('/')}/{filename}" if cfg.s3_key_prefix else filename

    s3_cfg = S3Config(
        aws_access_key_id=cfg.aws_access_key_id,
        aws_secret_access_key=cfg.aws_secret_access_key,
        aws_region=cfg.aws_region,
        s3_bucket_name=cfg.s3_bucket_name,
    )
    s3 = S3Client(s3_cfg)

    # write local copy for CI artifact / debugging
    out_dir = REPO_ROOT / "exports"
    out_dir.mkdir(parents=True, exist_ok=True)
    local_path = out_dir / filename
    try:
        local_path.write_bytes(csv_bytes)
    except Exception:
        logger.exception("Failed to write local copy of CSV (non-fatal)")

    if not args.no_upload:
        # Avoid uploading empty reports (header-only CSV) which can happen due to timezone/DST issues
        try:
            text = csv_bytes.decode("utf-8", errors="replace")
            lines = [l for l in text.splitlines() if l.strip() != ""]
        except Exception:
            lines = []

        if len(lines) <= 1:
            logger.warning("CSV contains no data rows (only header or empty). Skipping S3 upload for %s", filename)
            return 0

        # Try uploading with simple retry/backoff to handle transient network errors
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info("Uploading %s to s3://%s/%s (attempt %d)", filename, s3_cfg.s3_bucket_name, key, attempt)
                s3.upload_bytes(key, csv_bytes, content_type="text/csv")
                logger.info("S3 upload successful")
                break
            except Exception:
                logger.exception("S3 upload attempt %d failed", attempt)
                if attempt == max_attempts:
                    logger.error("All S3 upload attempts failed")
                    return 4
                sleep_seconds = 2 ** attempt
                logger.info("Retrying in %s seconds...", sleep_seconds)
                import time

                time.sleep(sleep_seconds)

    logger.info("Export complete: s3://%s/%s", s3_cfg.s3_bucket_name, key)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
