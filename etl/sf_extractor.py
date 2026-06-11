"""
Salesforce Education Cloud Lead extractor.
Authenticates with simple-salesforce (Username + Password + Security Token) and
runs the SOQL query against the Lead object, returning a pandas DataFrame.

Column cleanup (mirrors the prep in input/sf_lead_extract.txt):
  - Strip the trailing "__c" from custom-field column names.
  - Strip the leading "X" that SOQL adds when a column name starts with a digit
    (e.g. "X1st_Call_Date__c" -> "1st_Call_Date").
"""
from __future__ import annotations

import pandas as pd
from loguru import logger
from simple_salesforce import Salesforce

from etl.config import SalesforceSettings


SOQL_LEADS = """
SELECT
    Id,
    LastName,
    FirstName,
    Email,
    Phone,
    LeadSource,
    Status,
    CreatedDate,
    Are_you_a_Registered_Nurse__c,
    Program_Name__c,
    UTM_Source__c,
    Utm_Campaign__c,
    Lead_Status__c,
    Calendly_Session_Scheduled_Date__c,
    Calendly_Session_Scheduled__c,
    Event_Name__c,
    Anticipated_Start_Term__c,
    Anticipated_Start_Year__c,
    Preferred_Campus__c,
    X1st_Call_Date__c,
    X2nd_Call_Date__c,
    X3rd_Call_Date__c,
    X1st_Email_Send_Date__c,
    X2nd_Email_Send_Date__c,
    X3rd_Email_Send_Date__c
FROM Lead
"""


class SalesforceExtractor:
    def __init__(self, settings: SalesforceSettings) -> None:
        self.settings = settings

    def _connect(self) -> Salesforce:
        logger.info(
            f"Connecting to Salesforce as {self.settings.username} "
            f"(domain={self.settings.domain})"
        )
        return Salesforce(
            username=self.settings.username,
            password=self.settings.password,
            security_token=self.settings.security_token,
            domain=self.settings.domain,
        )

    def extract(self) -> pd.DataFrame:
        sf = self._connect()

        logger.info("Running SOQL query against Lead object...")
        results = sf.query_all(SOQL_LEADS)

        records = results.get("records", [])
        if not records:
            logger.warning("Salesforce Lead query returned 0 records.")
            return pd.DataFrame()

        df = pd.json_normalize(records)

        # Drop Salesforce response metadata columns
        df = df.drop(
            columns=[c for c in ("attributes.type", "attributes.url") if c in df.columns]
        )

        # Strip the "__c" suffix from custom field names
        df.columns = df.columns.str.replace("__c", "", regex=False)

        # Strip the leading "X" SOQL adds to fields starting with a digit
        # (e.g. "X1st_Call_Date" -> "1st_Call_Date"). Only on date-bearing columns
        # to avoid clobbering legitimate "X..." identifiers.
        renamed = {c: c[1:] for c in df.columns if "_Date" in c and c.startswith("X")}
        if renamed:
            df = df.rename(columns=renamed)

        logger.info(f"Parsed {len(df)} Lead rows from Salesforce.")
        return df
