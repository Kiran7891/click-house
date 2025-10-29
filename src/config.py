from dataclasses import dataclass
from typing import Optional


@dataclass
class ExportConfig:
    # ClickHouse
    clickhouse_url: str
    clickhouse_user: Optional[str] = None
    clickhouse_password: Optional[str] = None
    clickhouse_database: Optional[str] = None

    # S3 / AWS
    s3_bucket_name: str = ""
    s3_key_prefix: Optional[str] = None
    aws_region: Optional[str] = None
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None

    # runtime
    utc_date: Optional[str] = None  # optional override YYYY-MM-DD
