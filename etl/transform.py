"""
Workday DataFrame transformation.
Normalises column names, coerces types, drops empty rows, and stamps
every row with an etl_load_timestamp for Tableau freshness indicators.
"""
from __future__ import annotations

import re
import pandas as pd
import numpy as np
from loguru import logger
import etl.config


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
    # pd.Timestamp scalar → pandas stores the column as datetime64[ns], not object.
    # Plain datetime.now() would produce object dtype, which the Hyper writer
    # would then mis-map to SqlType.text() and fail on insert.
    df["etl_load_timestamp"] = pd.Timestamp.now()

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


class UnifiedTransformer:
    def __init__(self, mappings: dict):
        self.schema_map = mappings.get('schema_mapping', {})
        self.prog_derivations = mappings.get('program_derivation_rules', [])
        prog_map_block = mappings.get('program_mapping', {})
        self.prog_dictionary = prog_map_block.get('maps', {})
        self.prog_default = prog_map_block.get('default_unmapped', 'UNKNOWN')
        self.status_rules = mappings.get('status_rules', {})

    def clean_powercampus_data(self, df_pc: pd.DataFrame) -> pd.DataFrame:
        logger.info("Starting PowerCampus data transformation...")
        
        df_clean = df_pc.copy()
        
        # 1. Calculate application_term
        term = df_clean['ACADEMIC_TERM'].fillna('').astype(str)
        year = df_clean['ACADEMIC_YEAR'].fillna('').astype(str)
        df_clean['application_term'] = (term + " " + year).str.strip()

        # 2. Program Derivation logic
        df_clean['pc_derived_program'] = pd.Series(np.nan, index=df_clean.index, dtype=object)
        for rule in self.prog_derivations:
            if rule['condition'] == "default":
                df_clean['pc_derived_program'] = df_clean['pc_derived_program'].fillna(rule['result'])
            else:
                mask = df_clean.eval(rule['condition'])
                df_clean.loc[mask, 'pc_derived_program'] = rule['result']

        df_clean['program'] = df_clean['pc_derived_program'].map(self.prog_dictionary)
        df_clean.drop(columns=['pc_derived_program'], inplace=True)
        
        # 3. Status Engine Rules
        for flag_column, rules in self.status_rules.items():
            df_clean[flag_column] = np.nan
            for rule in rules:
                if rule['condition'] == "default":
                    df_clean[flag_column] = df_clean[flag_column].fillna(rule['result'])
                else:
                    mask = df_clean.eval(rule['condition'])
                    df_clean.loc[mask, flag_column] = rule['result']

        if 'admitted_flag' in df_clean.columns:
            df_clean['admit_date'] = df_clean['APP_DECISION_DATE'].where(
                df_clean['admitted_flag'] == 1, 
                pd.NaT
            )
            
        # Assign deposit_date if deposit_flag is 1
        if 'deposit_flag' in df_clean.columns:
            df_clean['deposit_date'] = df_clean['APP_STATUS_DATE'].where(
                df_clean['deposit_flag'] == 1, 
                pd.NaT
            )

        # 4. Schema Mapping 
        df_clean = df_clean.rename(columns=self.schema_map)
        
        # 5. Normalize remaining PowerCampus columns to snake_case
        df_clean.columns = _normalize_columns(df_clean.columns)
        
        return df_clean

    def clean_workday_data(self, df_workday: pd.DataFrame) -> pd.DataFrame:
        logger.info("Standardizing Workday data for merge...")
        
        if 'application_academic_period' in df_workday.columns:
            # Split the raw string by spaces: e.g., "2026 Fall (Extended)" -> ["2026", "Fall", "(Extended)"]
            splits = df_workday['application_academic_period'].str.strip().str.split(' ')
            
            # Grab entity 1 (Season) and entity 0 (Year), flip them, and capitalize
            df_workday['application_term'] = (splits.str[1] + " " + splits.str[0]).str.upper()
        else:
            df_workday['application_term'] = np.nan
            
        return df_workday

    def unify_datasets(self, df_workday: pd.DataFrame, df_powercampus: pd.DataFrame) -> pd.DataFrame:
        logger.info("Unifying Workday and PowerCampus datasets...")
        
        df_wd_clean = self.clean_workday_data(df_workday.copy())
        
        df_wd_clean['data_source_system'] = 'Workday'
        df_powercampus['data_source_system'] = 'PowerCampus'

        df_unified = pd.concat([df_wd_clean, df_powercampus], ignore_index=True)
        
        from etl.transform import _coerce_types
        df_unified = _coerce_types(df_unified)
        
        # Add the load timestamp just like the original Workday pipeline did
        df_unified["etl_load_timestamp"] = pd.Timestamp.now()
        
        return df_unified