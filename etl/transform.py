"""
Workday DataFrame transformation.
Normalises column names, coerces types, drops empty rows, and stamps
every row with an etl_load_timestamp for Tableau freshness indicators.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

import pandas as pd
from loguru import logger


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and type a raw Workday DataFrame.
    Returns a new DataFrame — the input is never mutated.
    """
    if df.empty:
        logger.warning("Transform received an empty DataFrame; returning as-is.")
        return df

    df = df.copy()

    df.columns = _normalize_columns(df.columns)
    logger.debug(f"Normalised columns: {list(df.columns)}")

    df = _coerce_types(df)
    df = _drop_all_null_rows(df)
    df["etl_load_timestamp"] = datetime.now(tz=timezone.utc).replace(tzinfo=None)

    logger.info(f"Transform complete: {len(df)} rows × {len(df.columns)} columns")
    return df


def _normalize_columns(cols: pd.Index) -> list[str]:
    """
    Convert column names to snake_case with no special characters.
    Examples:
        "Employee ID"  -> "employee_id"
        "Last Name"    -> "last_name"
        "wd:Hire_Date" -> "wd_hire_date"
    """
    normalized = []
    for col in cols:
        col = str(col).strip()
        col = re.sub(r"[^\w\s]", "_", col)   # non-word chars → underscore
        col = re.sub(r"\s+", "_", col)        # whitespace → underscore
        col = re.sub(r"_+", "_", col)         # collapse multiple underscores
        col = col.lower().strip("_")
        normalized.append(col)
    return normalized


def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Attempt smart type inference on object columns.
    Tries datetime parsing first, then numeric, leaves the rest as string.
    """
    for col in df.columns:
        if col == "etl_load_timestamp":
            continue
        if df[col].dtype != object:
            continue

        # Try datetime
        try:
            converted = pd.to_datetime(df[col], infer_datetime_format=True, errors="raise")
            df[col] = converted
            logger.debug(f"  Column '{col}' coerced to datetime.")
            continue
        except (ValueError, TypeError):
            pass

        # Try numeric
        try:
            converted = pd.to_numeric(df[col], errors="raise")
            df[col] = converted
            logger.debug(f"  Column '{col}' coerced to numeric.")
            continue
        except (ValueError, TypeError):
            pass

        # Keep as string; ensure no mixed types survive
        df[col] = df[col].astype(str).replace("nan", None)

    return df


def _drop_all_null_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows where every non-metadata column is null."""
    before = len(df)
    df = df.dropna(how="all")
    dropped = before - len(df)
    if dropped:
        logger.debug(f"Dropped {dropped} fully-null rows.")
    return df
