"""
Salesforce Lead transformation.

Steps (in order):
  1. Drop test records (FirstName/LastName/Email contains "test").
  2. Parse CreatedDate into tz-naive datetime + split date/time columns.
  3. Clean Email -> lower-cased email_key for joins/dedup.
  4. Map SF Program_Name -> Workday program name via ProgramMapping.xlsx
     sheet "SFtoWorkday"; drop leads whose program has no Workday equivalent.
  5. Drop leads where Lead_Status (Disposition Code) == "Duplicate".
  6. Program-switch: if the lead's email matches an applicant in the unified
     extract, overwrite program & application_term with the applicant's
     (latest application_date wins for tie-breaks). Mark program_switched_flag=1.
  7. Derive application_term for non-switched leads:
       Stage A — clean Anticipated_Start_Term/Year if both look valid.
       Stage B — month-window rules from mapping.yaml, keyed by program
                 (lead_program_overrides) or by (college, licensure) from
                 ProgramIntakes_FEB2026.xlsx.
  8. Drop leads where application_term still couldn't be resolved.
  9. Dedup by (email_key, program) keeping the earliest CreatedDate.
 10. Stamp data_source_system + etl_load_timestamp.
 11. snake_case columns, type-coerce, return.
"""
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger

from etl.config import SalesforceSettings
from etl.transform import _coerce_types


_VALID_TERMS = {"FALL", "SPRING", "SUMMER"}
_YEAR_RX = re.compile(r"^\d{4}$")
_TEST_RX = re.compile(r"test", re.IGNORECASE)


class SalesforceTransformer:
    def __init__(
        self,
        settings: SalesforceSettings,
        mappings: dict,
    ) -> None:
        self.settings = settings
        self.intake_rules: dict = mappings.get("lead_intake_rules", {}) or {}
        self.program_overrides: dict = mappings.get("lead_program_overrides", {}) or {}

        self.sf_to_workday: dict[str, str] = self._load_sf_to_workday(
            settings.program_mapping_path
        )
        intakes_raw = self._load_program_intakes(settings.program_intakes_path)
        # Re-key the intakes catalog by Workday program name so downstream
        # lookups (which use the mapped `program` column) succeed.
        self.program_intakes: dict[str, dict] = self._key_intakes_by_workday(
            intakes_raw, self.sf_to_workday
        )

    # ------------------------------------------------------------------
    # Reference-file loaders
    # ------------------------------------------------------------------
    @staticmethod
    def _load_sf_to_workday(path: Path) -> dict[str, str]:
        df = pd.read_excel(path, sheet_name="SFtoWorkday")
        for required in ("Program_Name", "Corrected_Program_Name"):
            if required not in df.columns:
                raise ValueError(
                    f"{path} sheet 'SFtoWorkday' missing required column '{required}'."
                )
        df = df.dropna(subset=["Program_Name", "Corrected_Program_Name"])
        df["Program_Name"] = df["Program_Name"].astype(str).str.strip()
        df["Corrected_Program_Name"] = df["Corrected_Program_Name"].astype(str).str.strip()
        df = df[df["Corrected_Program_Name"] != ""]
        mapping = dict(zip(df["Program_Name"], df["Corrected_Program_Name"]))
        logger.info(
            f"SFtoWorkday: loaded {len(mapping)} program mappings from {path.name}."
        )
        return mapping

    @staticmethod
    def _load_program_intakes(path: Path) -> dict[str, dict]:
        df = pd.read_excel(path)
        df.columns = [str(c).strip() for c in df.columns]

        def _norm(s: str) -> str:
            return re.sub(r"[\s?/_-]+", "", s.lower())

        norm_to_actual = {_norm(c): c for c in df.columns}

        def _col(*aliases: str) -> str | None:
            for a in aliases:
                hit = norm_to_actual.get(_norm(a))
                if hit:
                    return hit
            return None

        c_name = _col("Program Name", "Program")
        c_spring = _col("Spring Intake?", "Spring Intake", "Spring")
        c_summer = _col("Summer Intake?", "Summer Intake", "Summer")
        c_fall = _col("Fall Intake?", "Fall Intake", "Fall")
        c_college = _col("College")
        c_lic = _col("Pre/Post Licensure", "PrePostLicensure", "Licensure")

        if not c_name or not c_college or not c_lic:
            raise ValueError(
                f"{path} is missing required columns "
                f"(Program Name / College / Pre-Post Licensure)."
            )

        def _is_yes(v) -> bool:
            if v is None or (isinstance(v, float) and np.isnan(v)):
                return False
            return str(v).strip().lower() == "yes"

        out: dict[str, dict] = {}
        for _, row in df.iterrows():
            name = row.get(c_name)
            if pd.isna(name) or not str(name).strip():
                continue
            out[str(name).strip()] = {
                "college": str(row.get(c_college, "") or "").strip(),
                "licensure": str(row.get(c_lic, "") or "").strip(),
                "has_spring": _is_yes(row.get(c_spring)),
                "has_summer": _is_yes(row.get(c_summer)),
                "has_fall": _is_yes(row.get(c_fall)),
            }
        logger.info(f"Program intakes: loaded {len(out)} programs from {path.name}.")
        return out

    @staticmethod
    def _key_intakes_by_workday(
        intakes_raw: dict[str, dict],
        sf_to_wd: dict[str, str],
    ) -> dict[str, dict]:
        # The intakes file uses informal program names that don't always match
        # SFtoWorkday's keys exactly (e.g. "DNP Post Master's" vs "DNP - Post Master's").
        # Translate to Workday-style keys with progressively looser matching.
        def _loose(s: str) -> str:
            return re.sub(r"[\s\-_'.]+", "", str(s).lower())

        sf_to_wd_lc = {k.strip().lower(): v for k, v in sf_to_wd.items()}
        sf_to_wd_loose = {_loose(k): v for k, v in sf_to_wd.items()}

        out: dict[str, dict] = {}
        for name, info in intakes_raw.items():
            wd = (
                sf_to_wd.get(name)
                or sf_to_wd_lc.get(name.strip().lower())
                or sf_to_wd_loose.get(_loose(name))
            )
            key = wd if wd else name
            out.setdefault(key, info)
        return out

    # ------------------------------------------------------------------
    # Main transform
    # ------------------------------------------------------------------
    def transform(
        self,
        df_leads: pd.DataFrame,
        df_unified_applicants: pd.DataFrame,
    ) -> pd.DataFrame:
        logger.info(f"Salesforce Lead transform starting: {len(df_leads)} raw rows.")
        if df_leads.empty:
            logger.warning("No leads to transform; returning empty DataFrame.")
            return df_leads

        df = df_leads.copy()

        # 1. Drop test records --------------------------------------------------
        before = len(df)
        for c in ("FirstName", "LastName", "Email"):
            if c in df.columns:
                mask_test = df[c].astype(str).str.contains(_TEST_RX, na=False)
                df = df[~mask_test]
        logger.info(f"  Step 1 (test filter): dropped {before - len(df)} rows.")

        # 2. Parse CreatedDate --------------------------------------------------
        if "CreatedDate" in df.columns:
            ts = pd.to_datetime(df["CreatedDate"], errors="coerce", utc=True)
            try:
                ts = ts.dt.tz_convert(None)
            except (TypeError, AttributeError):
                pass  # already tz-naive
            df["lead_created_at"] = ts
            df["created_date"] = ts.dt.date
            df["created_time"] = ts.dt.time
            df = df.drop(columns=["CreatedDate"])
        else:
            raise ValueError("Salesforce lead extract missing 'CreatedDate'.")

        # 3. Clean email --------------------------------------------------------
        if "Email" in df.columns:
            df["email_key"] = df["Email"].astype(str).str.strip().str.lower()
            df.loc[df["email_key"].isin(["nan", "none", ""]), "email_key"] = pd.NA
        else:
            df["email_key"] = pd.NA

        # 4. Map SF program -> Workday program ---------------------------------
        if "Program_Name" not in df.columns:
            raise ValueError("Salesforce lead extract missing 'Program_Name'.")
        df["program_sf"] = df["Program_Name"].astype(str).str.strip()
        df["program"] = df["program_sf"].map(self.sf_to_workday)
        before = len(df)
        df = df[df["program"].notna() & df["program"].astype(str).str.strip().ne("")]
        logger.info(
            f"  Step 4 (program map): dropped {before - len(df)} leads with no Workday program."
        )

        # 5. Filter SF-flagged duplicates --------------------------------------
        if "Lead_Status" in df.columns:
            before = len(df)
            mask_dup = df["Lead_Status"].astype(str).str.strip().eq("Duplicate")
            df = df[~mask_dup]
            logger.info(
                f"  Step 5 (disposition=Duplicate): dropped {before - len(df)} leads."
            )

        # 6. Program-switch via applicant join ---------------------------------
        applicant_lookup = self._build_applicant_email_lookup(df_unified_applicants)
        df["program_switched_flag"] = 0
        df["application_term"] = pd.NA

        if applicant_lookup:
            in_lookup = df["email_key"].notna() & df["email_key"].isin(applicant_lookup)
            for i in df.index[in_lookup]:
                hit = applicant_lookup[df.at[i, "email_key"]]
                df.at[i, "program"] = hit["program"]
                df.at[i, "application_term"] = hit["application_term"]
                df.at[i, "program_switched_flag"] = 1
            logger.info(
                f"  Step 6 (program-switch): re-attributed {int(in_lookup.sum())} leads."
            )
        else:
            logger.info("  Step 6 (program-switch): no applicant lookup available; skipped.")

        # 7. Derive application_term for non-switched leads --------------------
        needs_intake = df["application_term"].isna()
        if needs_intake.any():
            derived = df.loc[needs_intake].apply(self._derive_intake, axis=1)
            df.loc[needs_intake, "application_term"] = derived
        n_resolved = int(df["application_term"].notna().sum())
        logger.info(
            f"  Step 7 (intake derivation): {n_resolved}/{len(df)} leads have an application_term."
        )

        # 8. Drop unresolvable leads -------------------------------------------
        before = len(df)
        df = df[
            df["application_term"].notna()
            & df["application_term"].astype(str).str.strip().ne("")
        ]
        logger.info(
            f"  Step 8 (drop unresolvable): dropped {before - len(df)} leads."
        )

        # 9. Final dedup by (email, program), earliest CreatedDate wins --------
        before = len(df)
        with_email = df[df["email_key"].notna()].copy()
        no_email = df[df["email_key"].isna()].copy()
        with_email = (
            with_email.sort_values("lead_created_at", ascending=True, na_position="last")
            .drop_duplicates(subset=["email_key", "program"], keep="first")
        )
        df = pd.concat([with_email, no_email], ignore_index=True)
        logger.info(
            f"  Step 9 (residual dedup): dropped {before - len(df)} duplicate leads."
        )

        # 10. Stamp metadata ---------------------------------------------------
        df["data_source_system"] = "Salesforce"
        df["etl_load_timestamp"] = pd.Timestamp.now()

        # 11. Normalize columns + type-coerce ----------------------------------
        df = df.drop(columns=["email_key"])
        df.columns = [self._snake(c) for c in df.columns]
        df = _coerce_types(df)

        logger.info(
            f"Salesforce Lead transform complete: {len(df)} rows × {len(df.columns)} columns."
        )
        return df

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _snake(col: str) -> str:
        s = str(col).strip()
        # Split CamelCase before lower/upper boundaries so SF column names like
        # "FirstName" / "LeadSource" land as first_name / lead_source.
        s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", s)
        s = re.sub(r"[^\w\s]", "_", s)
        s = re.sub(r"\s+", "_", s)
        s = re.sub(r"_+", "_", s).lower().strip("_")
        return s

    @staticmethod
    def _build_applicant_email_lookup(df_app: pd.DataFrame) -> dict[str, dict]:
        if df_app is None or df_app.empty:
            return {}
        if "program" not in df_app.columns or "application_term" not in df_app.columns:
            logger.warning(
                "Unified applicants missing program/application_term; "
                "program-switch skipped."
            )
            return {}

        email_cols = [c for c in df_app.columns if "email" in c.lower()]
        if not email_cols:
            logger.warning(
                "Unified applicants have no email column; program-switch skipped."
            )
            return {}

        sort_col = next(
            (c for c in ("application_date", "app_decision_date", "etl_load_timestamp")
             if c in df_app.columns),
            None,
        )

        keep = ["program", "application_term"] + ([sort_col] if sort_col else [])
        pieces: list[pd.DataFrame] = []
        for ec in email_cols:
            piece = df_app[[ec] + keep].copy()
            piece = piece.rename(columns={ec: "email_key"})
            piece = piece.dropna(subset=["email_key", "program", "application_term"])
            pieces.append(piece)
        if not pieces:
            return {}

        long = pd.concat(pieces, ignore_index=True)
        long["email_key"] = long["email_key"].astype(str).str.strip().str.lower()
        long = long[~long["email_key"].isin(["nan", "none", ""])]

        if sort_col:
            long = long.sort_values(sort_col, ascending=True, na_position="first")
        # keep="last" => most-recent application per email wins
        long = long.drop_duplicates(subset=["email_key"], keep="last")

        return long.set_index("email_key")[["program", "application_term"]].to_dict("index")

    def _derive_intake(self, row: pd.Series) -> str | None:
        # Stage A — clean Salesforce-provided term/year
        term_raw = str(row.get("Anticipated_Start_Term") or "").strip().upper()
        year_raw = str(row.get("Anticipated_Start_Year") or "").strip()
        # SF often serializes numeric years as "2026.0"
        if year_raw.endswith(".0"):
            year_raw = year_raw[:-2]
        if term_raw in _VALID_TERMS and _YEAR_RX.match(year_raw):
            year_int = int(year_raw)
            if 2020 <= year_int <= datetime.now().year + 5:
                return f"{term_raw} {year_int}"

        # Stage B — rules engine on CreatedDate
        created = row.get("lead_created_at")
        if pd.isna(created):
            return None
        program = row.get("program")
        if pd.isna(program):
            return None

        rule_key = self.program_overrides.get(program)
        if rule_key is None:
            info = self.program_intakes.get(program)
            if info is None:
                return None
            rule_key = self._default_rule_key(info)
            if rule_key is None:
                return None

        rule = self.intake_rules.get(rule_key)
        if not rule:
            return None

        month = int(created.month)
        year = int(created.year)
        for window in rule:
            if month in window.get("months", []):
                return f"{window['term']} {year + int(window['year_offset'])}"
        return None

    @staticmethod
    def _default_rule_key(info: dict) -> str | None:
        college = (info.get("college") or "").lower()
        licensure = (info.get("licensure") or "").lower()
        has_spring = bool(info.get("has_spring"))
        has_summer = bool(info.get("has_summer"))
        has_fall = bool(info.get("has_fall"))

        if "pre-licensure" in licensure:
            return "nursing_prelic"
        if "post-licensure" in licensure:
            return "nursing_postlic"

        if "health sciences" in college:
            if has_fall and not has_spring and not has_summer:
                return "health_sciences_fall_only"
            return "health_sciences_all_intakes"

        if "podiatric" in college:
            if has_fall and not has_spring and not has_summer:
                return "podiatric_dpm"
            if has_summer and not has_spring and not has_fall:
                return "podiatric_bridge"

        return None
