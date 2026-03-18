"""
Workday → Tableau Cloud ETL Pipeline
Entry point. Orchestrates extract → transform → write → publish.

Exit codes:
  0  — success
  1  — pipeline failure (Workday fetch, transform, Hyper write, or Cloud publish)
  3  — configuration error (missing .env variables)
"""
from __future__ import annotations

import sys
import time
from datetime import date

from loguru import logger

from etl.alerting import EmailAlerter
from etl.config import ConfigurationError, load_settings
from etl.extractor import WorkdayExtractor
from etl.hyper_writer import HyperWriter
from etl.logger import init_logger
from etl.publisher import TableauCloudPublisher
from etl.transform import transform


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
        # Logger not yet configured — print directly so the error is visible
        print(f"[CRITICAL] Configuration error:\n{exc}", file=sys.stderr)
        sys.exit(3)

    # ── 2. Initialise logger ────────────────────────────────────────────────
    run_id = init_logger(settings.output.log_dir, settings.output.log_retention_days)
    log_path = str(settings.output.log_dir / f"etl_{date.today()}.log")

    alerter = EmailAlerter(settings.alert)

    logger.info("=" * 60)
    logger.info("Workday ETL pipeline starting")
    logger.info(f"Output dir : {settings.output.output_dir}")
    logger.info(f"Datasource : {settings.tableau.datasource_name}")
    logger.info("=" * 60)

    t_start = time.perf_counter()

    try:
        # ── 3. Extract ──────────────────────────────────────────────────────
        extractor = WorkdayExtractor(settings.workday)
        df_raw = extractor.extract()

        # ── 4. Transform ────────────────────────────────────────────────────
        df = transform(df_raw)

        if df.empty:
            raise RuntimeError(
                "Transform produced an empty DataFrame. "
                "Check the Workday report URL and credentials."
            )

        # ── 5. Write Hyper file ─────────────────────────────────────────────
        writer = HyperWriter(settings.output.output_dir)
        hyper_path = writer.write(df, "workday.hyper")

        # ── 6. Publish to Tableau Cloud ─────────────────────────────────────
        publisher = TableauCloudPublisher(settings.tableau)
        publisher.publish(hyper_path)

        elapsed = time.perf_counter() - t_start
        logger.info("=" * 60)
        logger.info(
            f"Pipeline complete. rows={len(df)}, "
            f"duration={elapsed:.1f}s"
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
