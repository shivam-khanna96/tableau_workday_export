"""
Microsoft Graph API email alerter.
Uses the client credentials OAuth flow (app-level auth, no user sign-in required).
Requires the Azure app registration to have the Mail.Send application permission.

Alerts are best-effort: a Graph API failure is logged but never masks the original
pipeline exit code.
"""
from __future__ import annotations

import msal
import requests
from loguru import logger

from etl.config import AlertSettings

_GRAPH_SEND_MAIL_URL = "https://graph.microsoft.com/v1.0/users/{sender}/sendMail"
_GRAPH_SCOPE = ["https://graph.microsoft.com/.default"]


class EmailAlerter:
    def __init__(self, settings: AlertSettings) -> None:
        self.settings = settings
        self._app = msal.ConfidentialClientApplication(
            client_id=settings.client_id,
            authority=f"https://login.microsoftonline.com/{settings.tenant_id}",
            client_credential=settings.client_secret,
        )

    def _acquire_token(self) -> str:
        result = self._app.acquire_token_for_client(scopes=_GRAPH_SCOPE)
        if "access_token" not in result:
            error = result.get("error_description", result.get("error", "unknown"))
            raise RuntimeError(f"Failed to acquire Graph API token: {error}")
        return result["access_token"]

    def send_failure_alert(self, error_message: str, run_id: str, log_path: str) -> None:
        """
        Send an HTML failure email to all configured recipients.
        Raises on Graph API errors so the caller can decide to swallow or re-raise.
        """
        token = self._acquire_token()

        subject = f"[ETL FAILURE] Workday pipeline failed — run_id={run_id}"
        body_html = f"""
        <html><body style="font-family: Arial, sans-serif; color: #333;">
          <h2 style="color: #c0392b;">&#9888; Workday ETL Pipeline Failure</h2>
          <table cellpadding="6" cellspacing="0" border="1"
                 style="border-collapse:collapse; width:100%; max-width:600px;">
            <tr><th style="background:#f2f2f2; text-align:left;">Field</th><th>Value</th></tr>
            <tr><td><b>Run ID</b></td><td><code>{run_id}</code></td></tr>
            <tr><td><b>Error</b></td><td style="color:#c0392b;">{error_message}</td></tr>
            <tr><td><b>Log file</b></td><td><code>{log_path}</code></td></tr>
          </table>
          <p style="margin-top:16px; color:#555;">
            The Workday data source on Tableau Cloud was <b>not updated</b>.<br>
            The dashboard is showing the previous day&apos;s data.
          </p>
        </body></html>
        """

        recipients = [
            {"emailAddress": {"address": addr.strip()}}
            for addr in self.settings.to_emails.split(",")
            if addr.strip()
        ]

        payload = {
            "message": {
                "subject": subject,
                "body": {"contentType": "HTML", "content": body_html},
                "toRecipients": recipients,
            },
            "saveToSentItems": "false",
        }

        url = _GRAPH_SEND_MAIL_URL.format(sender=self.settings.from_email)
        response = requests.post(
            url,
            json=payload,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )
        response.raise_for_status()
        logger.info(f"Failure alert sent to: {self.settings.to_emails}")
