#!/usr/bin/env python3
"""
Daily exporter: ClickHouse -> S3 (CSV per day, per-agent avg and 90th percentile)

Environment variables:
  CLICKHOUSE_URL (required)
  CLICKHOUSE_USER
  CLICKHOUSE_PASSWORD
  CLICKHOUSE_DATABASE
  S3_BUCKET (required unless --no-upload)
  S3_KEY_PREFIX (optional)
  AWS_REGION
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  EXPORT_DATE (optional YYYY-MM-DD; if not set we compute "yesterday" in CLICKHOUSE_TZ if provided, else UTC)
  CLICKHOUSE_TZ (optional IANA TZ, e.g. America/Edmonton; used for date extraction and to compute "yesterday")
"""
import os
import sys
import datetime as dt
import logging
from pathlib import Path
import argparse
from zoneinfo import ZoneInfo  # Python 3.11 stdlib

# Ensure we can import from src when executed from repo root
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from config import ExportConfig  # type: ignore
from services.clickhouse_client import ClickHouseClient  # type: ignore
from services.s3_client import S3Client, S3Config  # type: ignore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("export_daily_agent_stats")


def resolve_export_date(override: str | None, tz_name: str | None) -> str:
    """
    Returns YYYY-MM-DD.
    If override provided -> validate & return.
    Else compute 'yesterday' using tz_name (if set) or UTC.
    Uses calendar-day arithmetic (date - 1 day) rather than subtracting 24 hours.
    """
    if override:
        dt.datetime.strptime(override, "%Y-%m-%d")
        return override
    if tz_name:
        try:
            now_local = dt.datetime.now(ZoneInfo(tz_name))
            return (now_local.date() - dt.timedelta(days=1)).isoformat()
        except Exception:
            logger.warning("Invalid CLICKHOUSE_TZ '%s'; falling back to UTC for date math", tz_name)
    return (dt.datetime.utcnow().date() - dt.timedelta(days=1)).isoformat()


def make_utc_window_for_local_date(date_str: str, tz_name: str | None) -> tuple[str, str]:
    """
    Returns (start_utc_str, end_utc_str) where each is formatted as
    'YYYY-MM-DD HH:MM:SS' in UTC. The window corresponds to the full
    calendar day in the provided local timezone (midnight..next midnight).

    This avoids DST-related nonexistent/ambiguous instants by doing
    calendar-day arithmetic in the local zone and then converting to UTC.
    """
    # parse date
    target_date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    if tz_name:
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            logger.warning("Invalid CLICKHOUSE_TZ '%s'; falling back to UTC for window computation", tz_name)
            tz = dt.timezone.utc
    else:
        tz = dt.timezone.utc

    # local midnight (start of day) and next local midnight
    start_local = dt.datetime.combine(target_date, dt.time(0, 0, 0), tzinfo=tz)
    end_local = start_local + dt.timedelta(days=1)

    # convert to UTC
    start_utc = start_local.astimezone(dt.timezone.utc)
    end_utc = end_local.astimezone(dt.timezone.utc)

    # ClickHouse toDateTime(..., 'UTC') expects 'YYYY-MM-DD HH:MM:SS'
    start_utc_str = start_utc.strftime("%Y-%m-%d %H:%M:%S")
    end_utc_str = end_utc.strftime("%Y-%m-%d %H:%M:%S")
    logger.debug("Time window for %s in tz=%s -> utc %s .. %s", date_str, tz_name or "UTC", start_utc_str, end_utc_str)
    return start_utc_str, end_utc_str


def build_sql_template() -> str:
    """
    Template with a placeholder {date_expr} so we can switch between:
      - toDate(call_start)                      (server/session TZ)
      - toDate(toTimeZone(call_start, '<tz>'))  (explicit TZ)
    """
    return """
SELECT
    agent_id,
    avg(call_duration_sec) AS avg_call_length_sec,
    quantileExact(0.9)(call_duration_sec) AS p90_call_length_sec
FROM conversations
WHERE {date_expr} = toDate('{date_str}')
GROUP BY agent_id
ORDER BY agent_id
FORMAT CSVWithNames
""".strip()


def build_count_sql(date_expr: str, date_str: str) -> str:
    return f"SELECT count() AS n FROM conversations WHERE {date_expr} = toDate('{date_str}')"


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

    # Build the UTC start/end window for the local calendar day we'll export
    clickhouse_tz = env.get("CLICKHOUSE_TZ")
    target_date = resolve_export_date(cfg.utc_date, clickhouse_tz)
    logger.info("Export date (YYYY-MM-DD): %s (CLICKHOUSE_TZ=%s)", target_date, clickhouse_tz or "UTC")
    start_utc_str, end_utc_str = make_utc_window_for_local_date(target_date, clickhouse_tz)

    ch = ClickHouseClient(cfg.clickhouse_url, cfg.clickhouse_user, cfg.clickhouse_password, cfg.clickhouse_database)

    # Pre-flight: probe count so we don't silently write an empty CSV
    count_sql = (
        "SELECT count() AS n FROM conversations "
        f"WHERE call_start >= toDateTime('{start_utc_str}','UTC') AND call_start < toDateTime('{end_utc_str}','UTC')"
    )
    try:
        count_bytes = ch.query_csv(count_sql + " FORMAT CSV")
        row = count_bytes.decode("utf-8", errors="replace").strip().splitlines()[-1]
        count = int(row or "0")
        logger.info("Row count for %s (utc-window %s..%s): %d", target_date, start_utc_str, end_utc_str, count)
    except Exception:
        logger.exception("Count probe failed")
        return 3

    if count == 0:
        logger.error(
            "No rows for %s using utc window %s..%s. Likely date/TZ mismatch or no data on that day. Aborting export.",
            target_date,
            start_utc_str,
            end_utc_str,
        )
        # Write an EMPTY flag to make it obvious in CI artifacts
        out_dir = REPO_ROOT / "exports"
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / f"agent_stats_{target_date}_EMPTY.flag").write_text("empty")
        return 5

    # Build and run the real aggregation using an explicit UTC time window
    sql = (
        build_sql_template()
        + "\nWHERE call_start >= toDateTime('{start}','UTC') AND call_start < toDateTime('{end}','UTC')"
    ).format(start=start_utc_str, end=end_utc_str)
    logger.info("Querying ClickHouse for aggregated stats...")

    try:
        csv_bytes = ch.query_csv(sql)
    except Exception:
        logger.exception("ClickHouse query failed")
        return 3

    filename = f"agent_stats_{target_date}.csv"
    key = f"{cfg.s3_key_prefix.strip('/')}/{filename}" if cfg.s3_key_prefix else filename

    # write local copy for CI artifact / debugging
    out_dir = REPO_ROOT / "exports"
    out_dir.mkdir(parents=True, exist_ok=True)
    local_path = out_dir / filename
    try:
        local_path.write_bytes(csv_bytes)
    except Exception:
        logger.exception("Failed to write local copy of CSV (non-fatal)")

    # Optionally skip upload if header-only (defensive)
    try:
        text = csv_bytes.decode("utf-8", errors="replace")
        lines = [l for l in text.splitlines() if l.strip()]
    except Exception:
        lines = []

    if not args.no_upload:
        if len(lines) <= 1:
            logger.warning("CSV appears empty/header-only. Skipping S3 upload for %s", filename)
            return 0
        s3_cfg = S3Config(
            aws_access_key_id=cfg.aws_access_key_id,
            aws_secret_access_key=cfg.aws_secret_access_key,
            aws_region=cfg.aws_region,
            s3_bucket_name=cfg.s3_bucket_name,
        )
        s3 = S3Client(s3_cfg)
        try:
            logger.info("Uploading %s to s3://%s/%s", filename, s3_cfg.s3_bucket_name, key)
            s3.upload_bytes(key, csv_bytes, content_type="text/csv")
        except Exception:
            logger.exception("S3 upload failed")
            return 4

    logger.info("Export complete: s3://%s/%s", s3_bucket, key)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
