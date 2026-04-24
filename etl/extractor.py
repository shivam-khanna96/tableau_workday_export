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
import pyodbc
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import warnings

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

        response.encoding = 'utf-8'

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


class PowerCampusExtractor:
    def __init__(self, server: str, database: str):
        """
        Initializes the SQL Server connection parameters.
        Using Windows Authentication (Trusted_Connection=yes).
        """
        self.server = server
        self.database = database
        # Using the standard SQL Server driver identified earlier
        self.conn_str = (
            f"DRIVER={{SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            "Trusted_Connection=yes;"
        )

    def extract(self) -> pd.DataFrame:
        """Executes the SQL query and returns a raw pandas DataFrame."""
        logger.info(f"Connecting to PowerCampus Database: {self.database} on {self.server}")
        
        # The exact columns needed for the mappings.yaml rules and term calculations
        sql_query = """
        SELECT DISTINCT 
        [ACADEMIC].[ACADEMIC_YEAR],
        [ACADEMIC].[ACADEMIC_TERM],
        [ACADEMIC].[APPLICATION_DATE],
        [ACADEMIC].[PEOPLE_CODE_ID],
        [PEOPLE].[PREVIOUS_ID],
        [PEOPLE].[LAST_NAME],
        [PEOPLE].[FIRST_NAME],
        [ACADEMIC].[CURRICULUM],
        [ACADEMIC].[DEGREE],
        [ACADEMIC].[ACADEMIC_SESSION],
        [ACADEMIC].[ACADEMIC_FLAG],
        [ACADEMIC].[PROGRAM] as [PC_PROGRAM],
        [ACADEMIC].[CLASS_LEVEL],
        [ACADEMIC].[ENROLL_SEPARATION],
        [ACADEMIC].[APP_STATUS],
        [CODE_APPSTATUS].[MEDIUM_DESC] as [APP_STATUS_DESCRIPTION],
        [ACADEMIC].[APP_STATUS_DATE],
        [ACADEMIC].[APP_DECISION],
        [CODE_APPDECISION].[MEDIUM_DESC] as [APP_DECISION_DESCRIPTION],
        [ACADEMIC].[APP_DECISION_DATE],
        [ORGANIZATION].[ORG_NAME_1],
        Substring(CONVERT(varchar(8), [PEOPLE].birth_date, 112),5,2) + '/' +
        Substring(CONVERT(varchar(8), [PEOPLE].birth_date, 112),7,2) + '/' +
        Substring(CONVERT(varchar(8), [PEOPLE].birth_date, 112),1,4) as [BIRTHDATE],
        SMUEmailAddress.smu_email AS [SMU_EMAIL],
        PersonalEmailAddress.personal_email AS [Personal_EMAIL],
        MailingAddress.[STATE] as [MAILING_STATE]

    FROM dbo.[ACADEMIC]

    JOIN dbo.[PEOPLE]
        ON [ACADEMIC].[PEOPLE_CODE_ID] = [PEOPLE].[PEOPLE_CODE_ID]

    JOIN dbo.[PEOPLETYPE]
        ON [PEOPLE].[PEOPLE_CODE_ID] = [PEOPLETYPE].[people_code_id]

    JOIN dbo.[ORGANIZATION]
        ON [ACADEMIC].[ORG_CODE_ID] = [ORGANIZATION].[ORG_CODE_ID]

    JOIN dbo.[CODE_APPSTATUS]
        ON [ACADEMIC].[APP_STATUS] = [CODE_APPSTATUS].[CODE_VALUE_KEY]

    JOIN dbo.[CODE_APPDECISION]
        ON [ACADEMIC].[APP_DECISION] = [CODE_APPDECISION].[CODE_VALUE_KEY]

    LEFT JOIN (
        SELECT peopleorgcodeid, email as smu_email
        FROM (
            SELECT 
                peopleorgcodeid, 
                emailaddressid, 
                email, 
                ROW_NUMBER() OVER (PARTITION BY peopleorgcodeid ORDER BY emailaddressid DESC) AS [INSTID]
            FROM powercampus2.campus6.dbo.emailaddress
            WHERE isactive = '1'
            AND email LIKE '%samuelmerritt%'
        ) sub
        WHERE [INSTID] = 1
    ) AS SMUEmailAddress
        ON [PEOPLE].[PEOPLE_CODE_ID] = SMUEmailAddress.peopleorgcodeid

    LEFT JOIN (
        SELECT peopleorgcodeid, email as personal_email
        FROM (
            SELECT 
                peopleorgcodeid, 
                emailaddressid, 
                email, 
                ROW_NUMBER() OVER (PARTITION BY peopleorgcodeid ORDER BY emailaddressid DESC) AS [INSTID]
            FROM powercampus2.campus6.dbo.emailaddress
            WHERE isactive = '1'
            AND email NOT LIKE '%samuelmerritt%'
        ) sub
        WHERE [INSTID] = 1
    ) AS PersonalEmailAddress
        ON [PEOPLE].[PEOPLE_CODE_ID] = PersonalEmailAddress.peopleorgcodeid

    OUTER APPLY (
        SELECT TOP 1 [STATE]
        FROM dbo.[ADDRESSSCHEDULE] addr
        WHERE addr.PEOPLE_ORG_CODE_ID = [PEOPLE].[PEOPLE_CODE_ID]
        AND addr.ADDRESS_TYPE = 'HOME'
        AND addr.STATUS = 'A'
        ORDER BY addr.START_DATE DESC
    ) AS MailingAddress

    WHERE 
        [ACADEMIC].[ACADEMIC_YEAR] >= N'2021'
        AND [ACADEMIC].[ACADEMIC_YEAR] <= N'2026'
        AND NOT ([ACADEMIC].[ACADEMIC_YEAR] = N'2026' AND [ACADEMIC].[ACADEMIC_TERM] = N'FALL')
        AND [ACADEMIC].[APPLICATION_FLAG] = N'Y'
        AND [ACADEMIC].[ACADEMIC_SESSION] <> N''
        AND [ACADEMIC].[CURRICULUM] <> N''
        """
        
        conn = None
        try:
            conn = pyodbc.connect(self.conn_str)
            logger.debug("SQL Server connection established successfully.")
            
            # Use the warnings context manager to suppress the SQLAlchemy warning
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=UserWarning)
                df_pc = pd.read_sql(sql_query, conn)

            logger.info(f"PowerCampus extraction complete: {len(df_pc)} rows retrieved.")
            
            return df_pc

        except pyodbc.Error as e:
            logger.error(f"Database connection or execution failed: {e}")
            raise RuntimeError(f"PowerCampus Extract Failed: {e}")
            
        finally:
            if conn:
                conn.close()
                logger.debug("SQL Server connection closed.")