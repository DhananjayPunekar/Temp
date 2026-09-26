# =====================================================================
# framework/dv_rule_engine.py   (Silver -> Gold/ADV extension, v1.0)
#
# Data Vault 2.0 transform rules for the Gold / ADV layer.
#
# The Bronze->Silver RuleEngine (framework/rule_engine.py) already provides
# hash_key, hashdiff, audit_columns, trim_strings, cast_decimal, drop_columns,
# upper_case, coalesce_nulls and custom_sql. The Gold layer needs a handful of
# Data-Vault-specific rules:
#
#   record_source   - stamp/validate the QBE record-source code (REC_SRC_NM='109')
#   business_key     - build a Hub/Link business key by concatenating columns
#   dv_audit        - stamp Data Vault system columns (LD_DT, LD_END_DT,
#                     REC_SRC_NM, ERR_FLG, ERR_CD)
#   hardcode        - set one or more columns to literal values
#   filter_not_null  - drop rows where any of the given columns is NULL
#                     (e.g. LNK_PLCY_INSD_OBJ_CNTCT_PNT: skip rows with a
#                      NULL QBE_HASH_INSD_OBJ_ID)
#   filter_expr     - drop rows that fail an arbitrary SQL predicate
#                     (e.g. HUB_CVRG: QLNC_C_BO_COVERAGE_CODE IS NOT NULL)
#
# These build directly on the existing hash_key/hashdiff rules:
#   * A HUB pipeline   = business_key -> hash_key(QBE_HASH_*_ID) -> dv_audit
#   * A LINK pipeline  = hash_key over the parent business keys -> dv_audit
#                        (+ filter_not_null for the insert guards)
#   * A SAT pipeline   = hashdiff -> dv_audit  (SCD2 handled by DeltaWriter)
#
# INSTALLATION:
#   The base RuleEngine discovers rules via getattr(cls, "_rule_<name>"), and
#   PipelineExecutor.build_df calls RuleEngine.apply_rules directly. To make the
#   new rules available WITHOUT modifying the shipped engine, call:
#       from framework.dv_rule_engine import DVRuleEngine
#       DVRuleEngine.install()       # injects DV rules into RuleEngine
#   once at startup (the Gold notebook does this).
# =====================================================================
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from framework.rule_engine import RuleEngine

logger = logging.getLogger(__name__)
logging.getLogger("py4j").setLevel(logging.ERROR)



class DVRuleEngine(RuleEngine):
    """Adds Data Vault rules on top of the Bronze->Silver RuleEngine."""

    # ----------------------------------------------------------------
    # record_source
    # ----------------------------------------------------------------
    @staticmethod
    def _rule_record_source(df: DataFrame, params: dict) -> DataFrame:
        """
        Stamp the QBE record-source code and (optionally) keep only rows from
        that source.

        params:
          target  - output column name (default: REC_SRC_NM)
          value   - record source literal (default: '109' = Majesco)
          source_col - if given, FILTER df to rows where source_col == value
                       BEFORE stamping (used when the silver row carries its own
                       record-source column).
        """
        target = params.get("target", "REC_SRC_NM")
        value = str(params.get("value", "109"))
        source_col = params.get("source_col")
        if source_col and source_col in df.columns:
            df = df.filter(F.col(source_col) == F.lit(value))
        return df.withColumn(target, F.lit(value))

    # ----------------------------------------------------------------
    # business_key
    # ----------------------------------------------------------------
    @staticmethod
    def _rule_business_key(df: DataFrame, params: dict) -> DataFrame:
        """
        Build a Hub/Link business key by concatenating columns, e.g.
        PLCY_ID = Concat(MQP_DISPLAY_POLICY_NUMBER,'_',MQP_EFFECTIVE_DATE).

        params:
          cols      - ordered list of column names OR literals to concat (required)
          target    - output business-key column name (required, e.g. PLCY_ID)
          separator - join separator (default: '_')
          upper     - upper-case the result (default: False)
        Literals are expressed as quoted strings in the list, e.g.
          {"cols": ["MQP_DISPLAY_POLICY_NUMBER", "MQP_EFFECTIVE_DATE"],
           "target": "PLCY_ID", "separator": "_"}
        """
        cols = params.get("cols", [])
        target = params.get("target")
        separator = params.get("separator", "_")
        if not cols or not target:
            logger.warning("[DV.business_key] 'cols'/'target' missing - skipping.")
            return df
        parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]
        expr = F.concat_ws(separator, *parts)
        if params.get("upper"):
            expr = F.upper(expr)
        return df.withColumn(target, expr)

    # ----------------------------------------------------------------
    # dv_audit
    # ----------------------------------------------------------------
    @staticmethod
    def _rule_dv_audit(df: DataFrame, params: dict) -> DataFrame:
        """
        Stamp the standard ADV Data-Vault system columns.

        params (all optional):
          rec_src      - REC_SRC_NM literal (default: '109'); set None to skip
          ld_dt        - load-date column name (default: LD_DT)
          ld_end_dt    - end-date column name (default: LD_END_DT, set NULL)
          err_flg      - default '0'
          err_cd       - default '0'
        """
        rec_src = params.get("rec_src", "109")
        ld_dt = params.get("ld_dt", "LD_DT")
        ld_end_dt = params.get("ld_end_dt", "LD_END_DT")
        logger.info("[DV.dv_audit] Stamping LD_DT=%s LD_END_DT=%s REC_SRC_NM=%s ERR_FLG/ERR_CD='0'",
                    ld_dt, ld_end_dt, rec_src)
        df = df.withColumn(ld_dt, F.current_timestamp())
        df = df.withColumn(ld_end_dt, F.lit(None).cast("timestamp"))
        if rec_src is not None and "REC_SRC_NM" not in df.columns:
            df = df.withColumn("REC_SRC_NM", F.lit(str(rec_src)))
        if "ERR_FLG" not in df.columns:
            df = df.withColumn("ERR_FLG", F.lit(str(params.get("err_flg", "0"))))
        if "ERR_CD" not in df.columns:
            df = df.withColumn("ERR_CD", F.lit(str(params.get("err_cd", "0"))))
        return df

    # ----------------------------------------------------------------
    # hardcode
    # ----------------------------------------------------------------
    @staticmethod
    def _rule_hardcode(df: DataFrame, params: dict) -> DataFrame:
        """
        Set columns to literal values, e.g. type-code discriminators like
        CNTCT_PNT_TP_CD='Physical Address' or TRAN_TP_CD='Coverage_Level_Premium'.

        params:
          mappings - dict {column_name: literal_value}
        """
        mappings = params.get("mappings", {}) or {}
        if not isinstance(mappings, dict):
            raise ValueError("[DV.hardcode] 'mappings' must be a dict of {column: value}")
        for col_name, literal in mappings.items():
            df = df.withColumn(col_name, F.lit(literal))
        return df

    # ----------------------------------------------------------------
    # filter_not_null  (Link insert guards)
    # ----------------------------------------------------------------
    @staticmethod
    def _rule_filter_not_null(df: DataFrame, params: dict) -> DataFrame:
        """
        Drop rows where ANY of the given columns is NULL.
        e.g. LNK_PLCY_INSD_OBJ_CNTCT_PNT - do not insert if QBE_HASH_INSD_OBJ_ID
        is null.

        params:
          cols - list of column names (required)
        """
        for c in params.get("cols", []):
            if c in df.columns:
                before = df  # lazy — Spark won't count unless triggered
                df = df.filter(F.col(c).isNotNull())
                df = df.filter(F.col(c).cast('string') != '')
                logger.info("[DV.filter_not_null] Filtered rows where %s IS NULL", c)
            else:
                logger.warning("[DV.filter_not_null] Column '%s' not found in DataFrame — skipping filter", c)
        return df

    # ----------------------------------------------------------------
    # filter_expr  (Hub / generic guards)
    # ----------------------------------------------------------------
    @staticmethod
    def _rule_filter_expr(df: DataFrame, params: dict) -> DataFrame:
        """
        Keep only rows that satisfy a SQL predicate.
        e.g. HUB_CVRG requires QLNC_C_BO_COVERAGE_CODE IS NOT NULL.

        params:
          expr - SQL boolean expression string (required)
        """
        expr = params.get("expr")
        if expr:
            df = df.filter(expr)
        return df

    # ----------------------------------------------------------------
    # install - inject DV rules into the base RuleEngine
    # ----------------------------------------------------------------
    @classmethod
    def install(cls):
        """
        Inject DV rules into RuleEngine
        """
        injected = []
        skipped  = []

        for name in dir(cls):
            if name.startswith("_rule_"):
                if not hasattr(RuleEngine, name):
                    setattr(
                        RuleEngine,
                        name,
                        staticmethod(getattr(cls, name))
                    )
                    injected.append(name)
                else:
                    skipped.append(name)

        logger.info("[DVRuleEngine] Installed DV rules   : %s", injected)
        logger.info("[DVRuleEngine] Already present (skip): %s", skipped)

        return injected
