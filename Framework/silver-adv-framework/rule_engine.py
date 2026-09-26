# =====================================================================
# framework/rule_engine.py  (NEW — extracted from pipeline_executor)
# Ordered, data-driven transform rules applied to DataFrames.
#
# Built-in rules (configure via ctl_rule.rule_name + params_json):
#
#   audit_columns    — adds _ingest_ts, _batch_id, _run_id, _source_ref (H-05)
#   hash_key         — MD5 surrogate key from specified columns (H-02)
#   hashdiff         — MD5 change-detection hash (SCD1 gate / SCD2 trigger)
#   trim_strings     — strips leading/trailing whitespace from STRING cols
#   cast_decimal     — safe CAST to DECIMAL for validation columns
#   drop_columns     — removes specified columns before write
#   upper_case       — UPPER() on specified columns
#   coalesce_nulls   — replaces NULL with a literal default value
#   custom_sql       — arbitrary selectExpr on the DataFrame
#
# EXTENDING:
#   Add a new static method _rule_<name>(df, params) -> df
#   It will be auto-discovered by apply_rules().
# =====================================================================
# =====================================================================
# framework/rule_engine.py
# =====================================================================

import logging
import hashlib
from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException

logger = logging.getLogger(__name__)
logging.getLogger("py4j").setLevel(logging.ERROR)



class RuleEngine:
    """
    Applies ordered list of named transform rules to a DataFrame.

    Rules are defined as:
        {
            "rule": "rule_name",
            "params": { ... }
        }

    A rule implementation must exist as:
        def _rule_<rule_name>(df, params) -> DataFrame
    """

    # ----------------------------------------------------------------
    # PUBLIC
    # ----------------------------------------------------------------
    def apply_rules(self, df: DataFrame, rules: Optional[List[dict]]) -> DataFrame:
        """
        Apply ordered list of rules to DataFrame.
        """

        if not rules:
            return df

        for i, r in enumerate(rules):
            try:
                rule_name = (r.get("rule") or "").lower()   # ✅ CASE SAFE
                params = r.get("params", {}) or {}
            except AttributeError as e:
                raise ValueError(f"[RuleEngine] Invalid rule definition at position {i}: {e}") from e

            if not rule_name:
                logger.warning("[RuleEngine] Skipping empty rule at position %d: %s", i, r)
                continue

            method_name = f"_rule_{rule_name}"
            func = getattr(self, method_name, None)

            if func is None:
                raise ValueError(
                    f"[RuleEngine] Unknown rule '{rule_name}'. "
                    f"Expected method '{method_name}' not found."
                )

            logger.info(
                "[RuleEngine] Applying rule %-20s params=%s",
                rule_name,
                params
            )

            try:
                df = func(df, params)
            except AnalysisException as e:
                # Usually a bad column name / config in ctl_rule params_json.
                raise ValueError(
                    f"[RuleEngine] Rule '{rule_name}' has invalid column/config: {e}"
                ) from e
            except Exception as e:
                raise RuntimeError(
                    f"[RuleEngine] Rule '{rule_name}' failed: {e}"
                ) from e

        return df

    # ----------------------------------------------------------------
    # HELPERS
    # ----------------------------------------------------------------
    def _normalize_cols(self, df: DataFrame, cols: List[str]) -> List:
        """
        Case-insensitive column name resolution.
        """
        if not cols:
            return []

        df_cols = {c.lower(): c for c in df.columns}
        return [df_cols.get(c.lower(), c) for c in cols]

    # ----------------------------------------------------------------
    # BUILT-IN RULES
    # ----------------------------------------------------------------

    def _rule_audit_columns(self, df: DataFrame, params: dict) -> DataFrame:
        return df \
            .withColumn("_ingest_ts", F.current_timestamp()) \
            .withColumn("_batch_id", F.lit(params.get("batch_id"))) \
            .withColumn("_run_id", F.lit(params.get("run_id"))) \
            .withColumn("_source_ref", F.lit(params.get("source_ref")))

    # ----------------------------------------------------------------

    def _rule_hash_key(self, df: DataFrame, params: dict) -> DataFrame:
        # Exact legacy-compatible hasher:
        # hashlib.md5(x.encode("utf-8", errors="replace")).hexdigest()
        generate_hash_key = F.udf(
            lambda x: hashlib.md5(str(x).encode(encoding="utf-8", errors="replace")).hexdigest()
            if x is not None else None,
            StringType(),
        )

        # Optional mapping style for legacy compatibility:
        # {"mappings": {"QBE_HASH_X": "BUSINESS_KEY_COL", ...}}
        mappings = params.get("mappings", {}) or {}
        if mappings:
            for target_col, source_col in mappings.items():
                source_col = self._normalize_cols(df, [source_col])[0]
                # Match legacy behavior: remove blank/null source rows before hashing.
                df = df.filter(F.trim(F.col(source_col).cast("string")) != F.lit(""))
                df = df.na.drop(subset=[source_col])
                df = df.withColumn(target_col, generate_hash_key(F.col(source_col).cast("string")))
            return df

        cols = self._normalize_cols(df, params.get("cols", []))
        target = params.get("target")
        # Legacy configs typically did concat_sep with '_' before hashing.
        sep = params.get("separator", "_")

        if not cols or not target:
            raise ValueError("[RuleEngine.hash_key] 'cols' and 'target' are required when 'mappings' is not provided")

        concat_expr = F.concat_ws(
            sep,
            *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols],
        )

        # Match legacy behavior: drop rows where hash input is blank.
        df = df.filter(F.trim(concat_expr) != F.lit(""))

        return df.withColumn(target, generate_hash_key(concat_expr))

    # ----------------------------------------------------------------

    def _rule_hashdiff(self, df: DataFrame, params: dict) -> DataFrame:
        cols = self._normalize_cols(df, params.get("cols", []))
        target = params.get("target")
        sep = params.get("separator", "|")

        concat_expr = F.concat_ws(sep, *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols])

        return df.withColumn(target, F.md5(concat_expr))

    # ----------------------------------------------------------------

    def _rule_trim_strings(self, df: DataFrame, params: dict) -> DataFrame:
        cols = self._normalize_cols(df, params.get("cols", []))

        for c in cols:
            df = df.withColumn(c, F.trim(F.col(c)))

        return df

    # ----------------------------------------------------------------

    def _rule_upper_case(self, df: DataFrame, params: dict) -> DataFrame:
        cols = self._normalize_cols(df, params.get("cols", []))

        for c in cols:
            df = df.withColumn(c, F.upper(F.col(c)))

        return df

    # ----------------------------------------------------------------

    def _rule_coalesce_nulls(self, df: DataFrame, params: dict) -> DataFrame:
        mappings = params.get("mappings", {})

        for col_name, default_val in mappings.items():
            df = df.withColumn(
                col_name,
                F.coalesce(F.col(col_name), F.lit(default_val))
            )

        return df

    # ----------------------------------------------------------------

    def _rule_drop_columns(self, df: DataFrame, params: dict) -> DataFrame:
        cols = params.get("cols", [])
        return df.drop(*cols)

    # ----------------------------------------------------------------

    def _rule_cast_decimal(self, df: DataFrame, params: dict) -> DataFrame:
        cols = params.get("cols", [])
        precision = params.get("precision", 18)
        scale = params.get("scale", 2)

        for c in cols:
            df = df.withColumn(c, F.col(c).cast(f"decimal({precision},{scale})"))

        return df

    # ----------------------------------------------------------------

    def _rule_custom_sql(self, df: DataFrame, params: dict) -> DataFrame:
        exprs = params.get("select_exprs", [])
        if not exprs:
            raise ValueError("[RuleEngine.custom_sql] 'select_exprs' is required")
        return df.selectExpr(*exprs)

    def _rule_filter_expr(self, df: DataFrame, params: dict) -> DataFrame:
        expr = params.get("expr")
        if not expr:
            logger.warning("[RuleEngine] filter_expr has no 'expr' parameter; returning unchanged")
            return df
        return df.filter(expr)
