import logging
from typing import Optional
import requests

logger = logging.getLogger(__name__)


class ClickHouseClient:
    def __init__(self, base_url: str, user: Optional[str] = None, password: Optional[str] = None, database: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        # only set auth if provided
        self.auth = (user, password) if user or password else None
        self.database = database

    def query_csv(self, sql: str, timeout: int = 300) -> bytes:
        """
        Runs a ClickHouse SQL query and returns CSV bytes.
        Caller should use FORMAT CSVWithNames in sql.
        """
        params = {}
        if self.database:
            params["database"] = self.database

        headers = {"Content-Type": "text/plain; charset=utf-8"}
        logger.debug("ClickHouse query: %s", sql.replace("\n", " "))
        resp = requests.post(self.base_url, params=params, data=sql.encode("utf-8"), auth=self.auth, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.content
