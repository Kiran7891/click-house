import logging
from dataclasses import dataclass
from typing import Optional
import boto3
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger(__name__)


@dataclass
class S3Config:
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_region: Optional[str] = None
    s3_bucket_name: Optional[str] = None


class S3Client:
    def __init__(self, config: S3Config):
        session_kwargs = {}
        if config.aws_access_key_id and config.aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = config.aws_access_key_id
            session_kwargs["aws_secret_access_key"] = config.aws_secret_access_key
        if config.aws_region:
            session_kwargs["region_name"] = config.aws_region

        # boto3 will fall back to environment/IAM role when creds not provided
        self._s3 = boto3.client("s3", **session_kwargs) if session_kwargs else boto3.client("s3")
        self._bucket = config.s3_bucket_name

    def upload_bytes(self, key: str, data: bytes, content_type: str = "text/csv") -> None:
        if not self._bucket:
            raise ValueError("S3 bucket name is not set in config")
        try:
            self._s3.put_object(Bucket=self._bucket, Key=key, Body=data, ContentType=content_type)
            logger.info("Uploaded bytes to s3://%s/%s", self._bucket, key)
        except (BotoCoreError, ClientError):
            logger.exception("Failed to upload bytes to S3")
            raise

    def upload_file(self, key: str, file_path: str, extra_args: Optional[dict] = None) -> None:
        if not self._bucket:
            raise ValueError("S3 bucket name is not set in config")
        try:
            self._s3.upload_file(Filename=file_path, Bucket=self._bucket, Key=key, ExtraArgs=extra_args or {"ContentType": "text/csv"})
            logger.info("Uploaded file to s3://%s/%s", self._bucket, key)
        except (BotoCoreError, ClientError):
            logger.exception("Failed to upload file to S3")
            raise
