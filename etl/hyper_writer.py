"""
Tableau Hyper file writer.
Converts a pandas DataFrame to a .hyper extract using the Tableau Hyper API.
Uses an atomic write pattern (write to .tmp, then os.replace) so Tableau
Desktop/Cloud never reads a partially-written file.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from loguru import logger

from tableauhyperapi import (
    Connection,
    CreateMode,
    HyperProcess,
    Inserter,
    NOT_NULLABLE,
    NULLABLE,
    SqlType,
    TableDefinition,
    TableName,
    Telemetry,
)

# pandas dtype string → Tableau Hyper SQL type
_DTYPE_MAP: dict[str, SqlType] = {
    "int64": SqlType.big_int(),
    "int32": SqlType.int(),
    "float64": SqlType.double(),
    "float32": SqlType.double(),
    "bool": SqlType.bool(),
    "datetime64[ns]": SqlType.timestamp(),
    "object": SqlType.text(),
}


def _pandas_type_to_hyper(dtype: str) -> SqlType:
    """Map a pandas dtype string to the corresponding Hyper SqlType."""
    if dtype.startswith("Int"):          # nullable integer (Int64, Int32 …)
        return SqlType.big_int()
    if dtype.startswith("datetime"):     # datetime64[ns], datetime64[us] …
        return SqlType.timestamp()
    return _DTYPE_MAP.get(dtype, SqlType.text())


def _sniff_object_col_type(series: pd.Series) -> SqlType:
    """
    For object-dtype columns, inspect the first non-null value to pick the
    correct Hyper type.  Prevents Python datetime objects stored in an object
    column (e.g. etl_load_timestamp assigned via datetime.now()) from being
    mis-mapped to SqlType.text().
    """
    non_null = series.dropna()
    if non_null.empty:
        return SqlType.text()
    first = non_null.iloc[0]
    if isinstance(first, datetime):
        return SqlType.timestamp()
    if isinstance(first, bool):          # bool before int — bool is a subclass of int
        return SqlType.bool()
    if isinstance(first, int):
        return SqlType.big_int()
    if isinstance(first, float):
        return SqlType.double()
    return SqlType.text()


def _build_table_def(df: pd.DataFrame, table_name: str) -> TableDefinition:
    columns = []
    for col_name, dtype in df.dtypes.items():
        dtype_str = str(dtype)
        if dtype_str == "object":
            hyper_type = _sniff_object_col_type(df[col_name])
        else:
            hyper_type = _pandas_type_to_hyper(dtype_str)
        columns.append(
            TableDefinition.Column(col_name, hyper_type, NULLABLE)
        )
    return TableDefinition(TableName("Extract", table_name), columns)


def _sanitize_value(v: object) -> object:
    """
    Convert any NaN/NA sentinel to Python None so Hyper's text inserter
    never receives a float where it expects str | None.

    Root cause: object-dtype columns that contain a mix of strings and
    missing values store those missing values as float('nan').
    pd.notna() / .where() does NOT reliably convert them to None when the
    column dtype is object, so we handle it explicitly here.
    """
    if v is None:
        return None
    # float('nan') is the sentinel pandas uses for missing values in
    # object-dtype columns (e.g. a date column with some nulls).
    if isinstance(v, float) and pd.isna(v):
        return None
    # Catch any other pandas NA types (pd.NaT, pd.NA, np.nan …)
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        # pd.isna() raises on non-scalar containers — leave them as-is
        pass
    return v


def _df_to_rows(df: pd.DataFrame) -> list[list]:
    """
    Convert DataFrame to a list of row lists suitable for Hyper Inserter.
    - NaN / None / NaT       → Python None      (Hyper NULL)
    - datetime64[ns] columns → Python datetime  (Hyper requires native datetime)
    - object columns whose first value is a datetime → same conversion
    - object columns with float NaN mixed in → None  (see _sanitize_value)
    """
    result_df = df.copy()

    for col in result_df.columns:
        dtype_str = str(result_df[col].dtype)

        if dtype_str.startswith("datetime"):
            # numpy datetime64 column → list of Python datetime objects
            result_df[col] = result_df[col].dt.to_pydatetime()

        elif dtype_str == "object":
            # Defensive: if the column actually holds Python datetime objects
            # (e.g. assigned via datetime.now() before the pd.Timestamp fix),
            # leave them as-is — they are already native datetimes and Hyper
            # will accept them for a timestamp column.  No conversion needed.
            pass

    rows = result_df.values.tolist()

    # Final sanitization pass — converts float('nan') / NaT / pd.NA to None
    # for any column type that still has stray NA sentinels.
    return [[_sanitize_value(v) for v in row] for row in rows]


class HyperWriter:
    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir

    def write(self, df: pd.DataFrame, filename: str, table_name: str = "Extract") -> Path:
        """
        Write df to output_dir/filename using an atomic .tmp → rename pattern.
        Returns the final .hyper path.
        """
        final_path = self.output_dir / filename
        tmp_path = self.output_dir / f"{filename}.tmp"

        table_def = _build_table_def(df, table_name)
        rows = _df_to_rows(df)

        logger.info(f"Writing {len(df)} rows to {final_path} ...")

        try:
            with HyperProcess(
                telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU
            ) as hyper:
                with Connection(
                    hyper.endpoint,
                    str(tmp_path),
                    CreateMode.CREATE_AND_REPLACE,
                ) as conn:
                    conn.catalog.create_schema_if_not_exists("Extract")
                    conn.catalog.create_table(table_def)
                    with Inserter(conn, table_def) as inserter:
                        inserter.add_rows(rows)
                        inserter.execute()

            # Atomic replace — safe on Windows NTFS
            os.replace(tmp_path, final_path)
            logger.info(f"Hyper file ready: {final_path}")

        except Exception:
            # Clean up partial .tmp file so it doesn't confuse future runs
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
                logger.debug(f"Cleaned up partial tmp file: {tmp_path}")
            raise

        return final_path
