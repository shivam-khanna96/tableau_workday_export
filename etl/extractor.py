"""
Workday RaaS extractor.
Authenticates with HTTP Basic Auth (ISU profile credentials) and fetches the
report as JSON, CSV, or XML — returning a pandas DataFrame.
Retries up to 3× on transient 5xx errors with exponential backoff.
"""
from __future__ import annotations

import io

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from loguru import logger

from etl.config import WorkdaySettings


class WorkdayExtractor:
    def __init__(self, settings: WorkdaySettings) -> None:
        self.settings = settings
        self.session = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.auth = (self.settings.username, self.settings.password)
        # Retry on transient server errors with exponential backoff (2s, 4s, 8s)
        retry = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,  # let raise_for_status() handle it below
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def extract(self) -> pd.DataFrame:
        fmt = self.settings.format
        logger.info(f"Fetching Workday report [{fmt.upper()}]: {self.settings.url}")

        response = self.session.get(self.settings.url, timeout=120)
        response.raise_for_status()

        logger.debug(f"Workday response: HTTP {response.status_code}, "
                     f"content-length={len(response.content)} bytes")

        if fmt == "json":
            return self._parse_json(response.json())
        elif fmt == "csv":
            return self._parse_csv(response.text)
        elif fmt == "xml":
            return self._parse_xml(response.content)
        else:
            raise ValueError(f"Unsupported WORKDAY_FORMAT: '{fmt}'. Use json, csv, or xml.")

    def _parse_json(self, data: dict | list) -> pd.DataFrame:
        # Workday RaaS JSON wraps records under "Report_Entry"; fall back for flat arrays
        if isinstance(data, dict):
            records = data.get("Report_Entry", data)
        else:
            records = data
        if not records:
            logger.warning("Workday report returned 0 records.")
            return pd.DataFrame()
        df = pd.json_normalize(records)
        logger.info(f"Parsed {len(df)} rows from Workday JSON response.")
        return df

    def _parse_csv(self, text: str) -> pd.DataFrame:
        df = pd.read_csv(io.StringIO(text))
        logger.info(f"Parsed {len(df)} rows from Workday CSV response.")
        return df

    def _parse_xml(self, content: bytes) -> pd.DataFrame:
        # Workday XML uses the wd: namespace; lxml handles this transparently
        df = pd.read_xml(io.BytesIO(content))
        logger.info(f"Parsed {len(df)} rows from Workday XML response.")
        return df
