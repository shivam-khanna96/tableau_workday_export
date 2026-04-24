"""
Workday & PowerCampus → Tableau Cloud ETL Pipeline
Entry point. Orchestrates extract → transform → write → publish.

Exit codes:
  0  — success
  1  — pipeline failure (Workday fetch, transform, Hyper write, or Cloud publish)
  3  — configuration error (missing .env variables)
"""
from __future__ import annotations

import os
import sys
import time
from datetime import date

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

    try:
        # ====================================================================
        # FLOW 1: THE ORIGINAL WORKDAY PIPELINE
        # ====================================================================
        logger.info("--- Starting Workday Extraction ---")
        wd_extractor = WorkdayExtractor(settings.workday)
        df_raw_wd = wd_extractor.extract()

        logger.info("--- Transforming Workday Data ---")
        df_wd_clean = transform(df_raw_wd)

        if df_wd_clean.empty:
            raise RuntimeError(
                "Transform produced an empty DataFrame. "
                "Check the Workday report URL and credentials."
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


        # ====================================================================
        # COMPLETION
        # ====================================================================
        elapsed = time.perf_counter() - t_start
        logger.info("=" * 60)
        logger.info(
            f"Pipeline complete. Workday rows={len(df_wd_clean)}, "
            f"Unified rows={len(df_unified)}, duration={elapsed:.1f}s"
        )
        logger.info("=" * 60)
        sys.exit(0)

    except ConfigurationError as exc:
        logger.critical(f"Configuration error mid-run: {exc}")
        _try_send_alert(alerter, str(exc), run_id, log_path)
        sys.exit(3)

    except Exception as exc:
        elapsed = time.perf_counter() - t_start
        logger.exception(f"Pipeline failed after {elapsed:.1f}s: {exc}")
        _try_send_alert(alerter, str(exc), run_id, log_path)
        sys.exit(1)

if __name__ == "__main__":
    main()