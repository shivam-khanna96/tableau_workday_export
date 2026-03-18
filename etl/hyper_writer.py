"""
Tableau Hyper file writer.
Converts a pandas DataFrame to a .hyper extract using the Tableau Hyper API.
Uses an atomic write pattern (write to .tmp, then os.replace) so Tableau
Desktop/Cloud never reads a partially-written file.
"""
from __future__ import annotations

import os
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
    # Handle nullable integer types (Int64, Int32, etc.)
    if dtype.startswith("Int"):
        return SqlType.big_int()
    return _DTYPE_MAP.get(dtype, SqlType.text())


def _build_table_def(df: pd.DataFrame, table_name: str) -> TableDefinition:
    columns = []
    for col_name, dtype in df.dtypes.items():
        hyper_type = _pandas_type_to_hyper(str(dtype))
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
    - NaN / None / NaT  → Python None  (Hyper NULL)
    - datetime64        → Python datetime (Hyper Inserter requires native datetime)
    - object columns with float NaN mixed in → None  (see _sanitize_value)
    """
    result_df = df.copy()

    for col in result_df.columns:
        if str(result_df[col].dtype).startswith("datetime"):
            result_df[col] = result_df[col].dt.to_pydatetime()

    rows = result_df.values.tolist()

    # Final sanitization pass — catches float('nan') in object/text columns
    # that slip through pandas' .where(pd.notna(...)) on mixed-type columns.
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
