"""
Configuration loader.
Reads .env, validates all required keys upfront, and exposes a frozen Settings dataclass.
Fails fast at startup with a clear message listing ALL missing keys.
"""
from __future__ import annotations

import os
import yaml
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class ConfigurationError(Exception):
    pass


@dataclass(frozen=True)
class WorkdaySettings:
    url: str
    username: str
    password: str
    format: str  # "json" | "csv" | "xml"


@dataclass(frozen=True)
class TableauSettings:
    server_url: str
    site_id: str
    token_name: str
    token_value: str
    project_name: str
    datasource_name: str
    unified_datasource_name: str


@dataclass(frozen=True)
class AlertSettings:
    tenant_id: str
    client_id: str
    client_secret: str
    from_email: str
    to_emails: str  # comma-separated


@dataclass(frozen=True)
class OutputSettings:
    output_dir: Path
    log_dir: Path
    log_retention_days: int


@dataclass(frozen=True)
class Settings:
    workday: WorkdaySettings
    tableau: TableauSettings
    alert: AlertSettings
    output: OutputSettings
    mappings: dict


def _require(key: str) -> str | None:
    """Return env value or None (caller collects all missing keys)."""
    return os.getenv(key)


def load_settings() -> Settings:
    missing: list[str] = []

    def get(key: str) -> str:
        val = os.getenv(key, "").strip()
        if not val:
            missing.append(key)
        return val

    workday = WorkdaySettings(
        url=get("WORKDAY_URL"),
        username=get("WORKDAY_USERNAME"),
        password=get("WORKDAY_PASSWORD"),
        format=os.getenv("WORKDAY_FORMAT", "json").strip().lower(),
    )

    tableau = TableauSettings(
        server_url=get("TABLEAU_SERVER_URL"),
        site_id=get("TABLEAU_SITE_ID"),
        token_name=get("TABLEAU_TOKEN_NAME"),
        token_value=get("TABLEAU_TOKEN_VALUE"),
        project_name=os.getenv("TABLEAU_PROJECT_NAME", "Default").strip(),
        datasource_name=os.getenv("TABLEAU_DATASOURCE_NAME", "Workday Data").strip(),
        unified_datasource_name=os.getenv("TABLEAU_UNIFIED_DATASOURCE_NAME", "Unified Admissions Data").strip()
    )

    alert = AlertSettings(
        tenant_id=get("GRAPH_TENANT_ID"),
        client_id=get("GRAPH_CLIENT_ID"),
        client_secret=get("GRAPH_CLIENT_SECRET"),
        from_email=get("ALERT_FROM_EMAIL"),
        to_emails=get("ALERT_TO_EMAILS"),
    )

    output_dir = Path(os.getenv("OUTPUT_DIR", r"D:\SMU\tableau_workday_export\output"))
    log_dir = Path(os.getenv("LOG_DIR", r"D:\SMU\tableau_workday_export\logs"))
    retention = int(os.getenv("LOG_RETENTION_DAYS", "30"))

    output = OutputSettings(
        output_dir=output_dir,
        log_dir=log_dir,
        log_retention_days=retention,
    )

    if missing:
        raise ConfigurationError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            f"Copy .env.example to .env and fill in the missing values."
        )

    # --- NEW: Load Mappings YAML ---
    mappings_path_str = os.getenv("MAPPINGS_PATH")
    if not mappings_path_str:
        missing.append("MAPPINGS_PATH")
        mappings_data = {}
    else:
        mappings_path = Path(mappings_path_str)
        if not mappings_path.exists():
            raise ConfigurationError(f"Mappings file not found at: {mappings_path}")
        
        try:
            with open(mappings_path, 'r') as f:
                mappings_data = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Failed to parse YAML file at {mappings_path}:\n{e}")

    if missing:
        raise ConfigurationError(
            f"Missing required environment variables: {', '.join(missing)}\n"
            f"Copy .env.example to .env and fill in the missing values."
        )
    # Create runtime directories
    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    return Settings(workday=workday, tableau=tableau, alert=alert, output=output, mappings=mappings_data)
