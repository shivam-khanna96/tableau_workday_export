"""
Workday & PowerCampus → Tableau Cloud ETL Pipeline
Entry point. Orchestrates extract → transform → write → publish.

Exit codes:
  0  — success
  1  — one or more flows failed (admissions/unified or attrition); other flows still ran
  3  — configuration error (missing .env variables; mid-run config errors still let other flows run)
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import date

import pandas as pd
from loguru import logger
from dotenv import load_dotenv

from etl.alerting import EmailAlerter
from etl.config import ConfigurationError, load_settings
from etl.extractor import WorkdayExtractor, PowerCampusExtractor
from etl.hyper_writer import HyperWriter
from etl.logger import init_logger
from etl.publisher import TableauCloudPublisher
from etl.transform import transform, UnifiedTransformer

# Ensure environment variables are loaded
load_dotenv()

def _try_send_alert(
    alerter: EmailAlerter,
    error_msg: str,
    run_id: str,
    log_path: str,
) -> None:
    """Send failure alert; swallow Graph API errors to preserve original exit code."""
    try:
        alerter.send_failure_alert(error_msg, run_id, log_path)
    except Exception as alert_err:
        logger.warning(f"Could not send failure alert email: {alert_err}")

def _stringify_nested(df: pd.DataFrame) -> pd.DataFrame:
    """Serialize list/dict cells (Workday multi-instance fields) to JSON text so Hyper can store them."""
    for col in df.columns:
        if df[col].map(lambda v: isinstance(v, (list, dict))).any():
            df[col] = df[col].map(
                lambda v: json.dumps(v) if isinstance(v, (list, dict)) else v
            )
    return df

def main() -> None:
    # ── 1. Load and validate configuration ─────────────────────────────────
    try:
        settings = load_settings()
    except ConfigurationError as exc:
        print(f"[CRITICAL] Configuration error:\n{exc}", file=sys.stderr)
        sys.exit(3)

    # ── 2. Initialise logger ────────────────────────────────────────────────
    run_id = init_logger(settings.output.log_dir, settings.output.log_retention_days)
    log_path = str(settings.output.log_dir / f"etl_{date.today()}.log")

    alerter = EmailAlerter(settings.alert)

    logger.info("=" * 60)
    logger.info("Admissions ETL pipeline starting")
    logger.info(f"Output dir : {settings.output.output_dir}")
    logger.info("=" * 60)

    t_start = time.perf_counter()
    writer = HyperWriter(settings.output.output_dir)
    publisher = TableauCloudPublisher(settings.tableau)
    wd_extractor = WorkdayExtractor(settings.workday)

    # Each flow runs independently; failures are collected and alerted once at the end
    failures: list[str] = []
    config_error = False
    df_wd_clean = df_unified = df_attrition = None

    try:
        # ====================================================================
        # FLOW 1: THE ORIGINAL WORKDAY PIPELINE (ADMISSIONS PIPELINE REPORT)
        # ====================================================================
        logger.info("--- Starting Workday Extraction (admissions_pipeline_report) ---")
        df_raw_wd = wd_extractor.extract(
            settings.workday.admissions_pipeline_report_url,
            "admissions_pipeline_report",
        )

        logger.info("--- Transforming Workday Data ---")
        df_wd_clean = transform(df_raw_wd)

        if df_wd_clean.empty:
            raise RuntimeError(
                "Transform produced an empty DataFrame. "
                "Check ADMISSIONS_PIPELINE_REPORT_URL and Workday credentials."
            )

        logger.info("--- Writing & Publishing workday.hyper ---")
        hyper_path_wd = writer.write(df_wd_clean, "workday.hyper")
        publisher.publish(
            hyper_path_wd, 
            target_name=settings.tableau.datasource_name
        )
        
        logger.info("Workday pipeline completed successfully.")

        # ====================================================================
        # FLOW 2: THE POWERCAMPUS UNIFICATION PIPELINE
        # ====================================================================
        logger.info("--- Starting PowerCampus Extraction ---")
        pc_server = os.environ.get("POWERCAMPUS_SERVER")
        pc_db = os.environ.get("POWERCAMPUS_DATABASE")
        
        if not pc_server or not pc_db:
            raise ConfigurationError(
                "PowerCampus database variables missing. Add POWERCAMPUS_SERVER "
                "and POWERCAMPUS_DATABASE to your .env file."
            )

        pc_extractor = PowerCampusExtractor(server=pc_server, database=pc_db)
        df_raw_pc = pc_extractor.extract()

        logger.info("--- Transforming & Unifying Data ---")
        unifier = UnifiedTransformer(mappings=settings.mappings)
        df_pc_clean = unifier.clean_powercampus_data(df_raw_pc)
        
        # Merge the datasets
        df_unified = unifier.unify_datasets(df_wd_clean, df_pc_clean)

        logger.info("--- Writing & Publishing unified_admissions.hyper ---")
        hyper_path_unified = writer.write(df_unified, "unified_admissions.hyper")
        
        publisher.publish(
            hyper_path_unified, 
            target_name=settings.tableau.unified_datasource_name
        )

    except ConfigurationError as exc:
        logger.critical(f"Configuration error mid-run: {exc}")
        failures.append(f"Admissions/Unified flow (configuration): {exc}")
        config_error = True

    except Exception as exc:
        logger.exception(f"Admissions/Unified flow failed: {exc}")
        failures.append(f"Admissions/Unified flow: {exc}")

    try:
        # ====================================================================
        # FLOW 3: ATTRITION REPORT (RAW PULL & DUMP, NO TRANSFORMS)
        # ====================================================================
        logger.info("--- Starting Workday Extraction (attrition_report) ---")
        df_attrition = wd_extractor.extract(
            settings.workday.attrition_report_url,
            "attrition_report",
        )

        if df_attrition.empty:
            raise RuntimeError(
                "Attrition report returned no rows; skipping publish to avoid "
                "overwriting existing data. Check ATTRITION_REPORT_URL."
            )

        df_attrition = _stringify_nested(df_attrition)

        logger.info("--- Writing & Publishing attrition.hyper ---")
        hyper_path_attr = writer.write(df_attrition, "attrition.hyper")
        publisher.publish(
            hyper_path_attr,
            target_name=settings.tableau.attrition_datasource_name
        )

        logger.info("Attrition pipeline completed successfully.")

    except Exception as exc:
        logger.exception(f"Attrition flow failed: {exc}")
        failures.append(f"Attrition flow: {exc}")

    # ====================================================================
    # COMPLETION
    # ====================================================================
    elapsed = time.perf_counter() - t_start

    def _rows(df) -> str:
        return str(len(df)) if df is not None else "n/a"

    logger.info("=" * 60)
    logger.info(
        f"Pipeline {'finished with failures' if failures else 'complete'}. "
        f"Workday rows={_rows(df_wd_clean)}, Unified rows={_rows(df_unified)}, "
        f"Attrition rows={_rows(df_attrition)}, duration={elapsed:.1f}s"
    )
    logger.info("=" * 60)

    if failures:
        _try_send_alert(alerter, " | ".join(failures), run_id, log_path)
        sys.exit(3 if config_error else 1)
    sys.exit(0)

if __name__ == "__main__":
    main()