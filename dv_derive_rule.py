# =====================================================================
# framework/dv_derive_rule.py   (Silver -> Gold/ADV)
#
# Registers a single new rule, `derive`, onto the base RuleEngine using the
# SAME install pattern as DVRuleEngine. This is the bridge that lets a
# control-table column flagged col_type='derived' execute a PySpark function
# from dv_derivations.py - so NO derived/CASE logic lives in the control table.
#
# Two ways the `derive` rule is driven:
#   1) Directly from a ctl_rule row:
#        rule_name = 'derive'
#        params    = {"fn":"derive_full_name","target":"FULL_NM",
#                     "args":{"first":"MNS_FIRST_NAME", ...}}
#   2) From ctl_column_map rows flagged col_type='derived' (transform_fn +
#      transform_args). Use build_derive_rules_from_map() to turn those rows
#      into one ordered list of `derive` rule dicts to prepend to cfg['rules'].
#
# INSTALL (once at startup, in the gold notebook):

# =====================================================================
# =====================================================================
# framework/dv_derivations.py
# =====================================================================
import logging
import json
from datetime import date, datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DateType, StringType, StructType, StructField
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)
logging.getLogger("py4j").setLevel(logging.ERROR)


# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------



# Resolved once per driver process and cached — holds the gold catalog.schema
# (Settings.gold_fq) so seed files only need bare table names, no catalog/schema.
_GOLD_FQ = None
_GOLD_FQ_ATTEMPTED = False


def set_gold_fq(gold_fq: str) -> None:
    """Optional explicit override; otherwise gold_fq is self-resolved on first use."""
    global _GOLD_FQ
    _GOLD_FQ = gold_fq


def _get_dbutils(spark):
    """pyspark.dbutils.DBUtils does not exist on serverless / Spark Connect."""
    try:
        from databricks.sdk.runtime import dbutils as _dbutils
        return _dbutils
    except Exception:
        pass
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except Exception:
        pass
    import builtins
    return getattr(builtins, "dbutils", None)


def _auto_resolve_gold_fq(spark):
    """Self-resolve gold catalog.schema without any bootstrap/driver changes.

    Mirrors env_loader.Settings: catalog = TARGET_CATALOG_<ENV>, or
    TARGET_CATALOG_PREFIX_<ENV> + env (e.g. "dsi" + "_dev" = "dsi_dev");
    schema = GOLD_SCHEMA_<ENV>.
    """
    global _GOLD_FQ, _GOLD_FQ_ATTEMPTED
    if _GOLD_FQ or _GOLD_FQ_ATTEMPTED:
        return _GOLD_FQ
    _GOLD_FQ_ATTEMPTED = True
    try:
        import os
        from config.env_loader import _parse_env_file

        dbutils = _get_dbutils(spark)
        if dbutils is None:
            raise RuntimeError("dbutils unavailable in this runtime")
        envname = dbutils.secrets.get(scope="databricksKV", key="envname")
        framework_path = "/Workspace/DataBricksProcess/majesco-ic-framework/"
        env_file = os.path.join(framework_path, "env", f".env.{envname.lower()}")

        raw = _parse_env_file(env_file)
        env = envname.strip().lower()
        s = env.upper()

        prefix = raw.get(f"TARGET_CATALOG_PREFIX_{s}") or raw.get("TARGET_CATALOG_PREFIX")
        target_catalog = raw.get(f"TARGET_CATALOG_{s}") or raw.get("TARGET_CATALOG") or (f"{prefix}_{env}" if prefix else None)
        gold_schema = raw.get(f"GOLD_SCHEMA_{s}") or raw.get("GOLD_SCHEMA") or "adv_db_majescoic"

        if target_catalog:
            _GOLD_FQ = f"{target_catalog}.{gold_schema}"
            logger.info("[dv_derive_rule] Auto-resolved gold_fq=%s from %s", _GOLD_FQ, env_file)
            print(f"[dv_derive_rule] gold_fq resolved OK -> {_GOLD_FQ} (from {env_file})")
        else:
            logger.warning("[dv_derive_rule] TARGET_CATALOG_%s / TARGET_CATALOG_PREFIX_%s not found in %s", s, s, env_file)
            print(f"[dv_derive_rule] gold_fq NOT resolved -> TARGET_CATALOG_{s}/TARGET_CATALOG_PREFIX_{s} missing in {env_file}")
    except Exception as e:
        logger.warning("[dv_derive_rule] Could not auto-resolve gold_fq: %s", e)
        print(f"[dv_derive_rule] gold_fq NOT resolved -> error: {e}")
    return _GOLD_FQ


# Cache of resolved table names so SHOW TABLES runs once per candidate list,
# not once per rule invocation (each miss costs a driver-side Spark job).
_TABLE_RESOLVE_CACHE = {}


def _resolve_existing_table(spark, table_candidates, gold_fq=None):
    """Return first existing table name from candidates, else None.

    Bare table names (no '.') are auto-qualified with the framework's
    gold catalog.schema — passed explicitly via gold_fq, set via
    set_gold_fq(), or self-resolved from the env file — so seed files
    only need to supply the table name, never catalog/schema.
    """
    gold_fq = gold_fq or _auto_resolve_gold_fq(spark)

    cache_key = (tuple(table_candidates or ()), gold_fq)
    if cache_key in _TABLE_RESOLVE_CACHE:
        return _TABLE_RESOLVE_CACHE[cache_key]

    resolved = _resolve_existing_table_uncached(spark, table_candidates, gold_fq)
    _TABLE_RESOLVE_CACHE[cache_key] = resolved
    return resolved


def _resolve_existing_table_uncached(spark, table_candidates, gold_fq=None):
    for tbl in table_candidates:
        if not tbl:
            continue
        original = tbl.strip()
        tbl = original
        if gold_fq and "." not in tbl:
            tbl = f"{gold_fq}.{tbl}"
            print(f"[dv_derive_rule] Qualified bare table '{original}' -> '{tbl}'")
        try:
            if spark.catalog.tableExists(tbl):
                print(f"[dv_derive_rule] Table resolved OK -> '{tbl}' exists")
                return tbl
            print(f"[dv_derive_rule] Table NOT found -> '{tbl}' does not exist")
            continue
        except Exception:
            pass
        try:
            parts = tbl.replace("`", "").split(".")
            if len(parts) == 3:
                cat, sch, name = parts
                ns = f"`{cat}`.`{sch}`"
                q = f"SHOW TABLES IN {ns} LIKE '{name}'"
            elif len(parts) == 2:
                sch, name = parts
                ns = f"`{sch}`"
                q = f"SHOW TABLES IN {ns} LIKE '{name}'"
            else:
                q = f"SHOW TABLES LIKE '{parts[-1]}'"
            if spark.sql(q).count() > 0:
                print(f"[dv_derive_rule] Table resolved OK -> '{tbl}' exists")
                return tbl
            else:
                print(f"[dv_derive_rule] Table NOT found -> '{tbl}' does not exist")
        except Exception as e:
            print(f"[dv_derive_rule] Table lookup failed for '{tbl}': {e}")
            continue
    return None


def _get_ci(d, key):
    if d is None or key is None:
        return None
    if key in d:
        return d.get(key)
    kl = str(key).lower()
    for dk, dv in d.items():
        if str(dk).lower() == kl:
            return dv
    return None

def _col_or_null(df: DataFrame, name: str):
    """
    Safely return column if exists, else NULL string column.
    Case-insensitive lookup.
    """
    if not name:
        return F.lit(None).cast("string")

    df_cols = {c.lower(): c for c in df.columns}

    if name.lower() in df_cols:
        return F.col(df_cols[name.lower()])

    return F.lit(None).cast("string")

def derive_sql_expr(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    Evaluate a generic Spark SQL expression into target column.

    args:
      expr: SQL expression string
    """
    expr = args.get("expr")
    if not expr:
        logger.warning("[derive_sql_expr] missing expr for target %s; skipping", target)
        return df
    return df.withColumn(target, F.expr(expr))

# ---------------------------------------------------------------------
# DERIVATION FUNCTIONS
# Each function must follow:
#   fn(df, target, args) -> DataFrame
# ---------------------------------------------------------------------

def concat_name(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    Concatenate multiple name columns, skipping NULLs.
    """
    cols = args.get("cols", [])
    sep = args.get("separator", " ")

    parts = [
        F.coalesce(_col_or_null(df, c).cast("string"), F.lit(""))
        for c in cols
    ]

    expr = F.trim(F.regexp_replace(F.concat_ws(sep, *parts), r"\s+", " "))

    return df.withColumn(
        target,
        F.when(expr == "", F.lit(None)).otherwise(expr)
    )


# ---------------------------------------------------------------------

def trim_col(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    Trim a single column.
    """
    col_name = args.get("col")

    return df.withColumn(
        target,
        F.trim(_col_or_null(df, col_name).cast("string"))
    )


# ---------------------------------------------------------------------

def derive_full_name(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    Standard full name derivation logic with fallback priority.
    """

    first = _col_or_null(df, args.get("first"))
    middle = _col_or_null(df, args.get("middle"))
    surname = _col_or_null(df, args.get("surname"))
    uw = _col_or_null(df, args.get("underwriter_name"))
    contact = _col_or_null(df, args.get("contact_name"))

    person = F.trim(
        F.regexp_replace(
            F.concat_ws(
                " ",
                F.coalesce(first, F.lit("")),
                F.coalesce(middle, F.lit("")),
                F.coalesce(surname, F.lit(""))
            ),
            r"\s+",
            " "
        )
    )

    val = (
        F.when(first.isNotNull() | surname.isNotNull(), person)
         .when(uw.isNotNull(), uw)
         .when(contact.isNotNull(), contact)
         .otherwise(F.lit(None))
    )

    return df.withColumn(
        target,
        F.when(val == "", F.lit(None)).otherwise(val)
    )


# ---------------------------------------------------------------------

def derive_party_type(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    Determine party type using STTM-aligned role/column logic.

    Rules:
      - insurer, producer -> Organization
      - insured -> Organization when business_name present, else Person
      - underwriter, broker, licensed individual -> Person
      - fallback by source fields:
          business_name/company_name/producer_code -> Organization
          first/surname/underwriter/contact -> Person
    """

    role_col = args.get("role_col")
    bn = _col_or_null(df, args.get("business_name"))
    company = _col_or_null(df, args.get("company_name"))
    producer_code = _col_or_null(df, args.get("producer_code"))
    first = _col_or_null(df, args.get("first"))
    surname = _col_or_null(df, args.get("surname"))
    uw = _col_or_null(df, args.get("underwriter_name"))
    contact = _col_or_null(df, args.get("contact_name"))
    role = F.lower(F.trim(_col_or_null(df, role_col).cast("string")))

    bn_present = F.trim(F.coalesce(bn.cast("string"), F.lit(""))) != ""
    company_present = F.trim(F.coalesce(company.cast("string"), F.lit(""))) != ""
    producer_present = F.trim(F.coalesce(producer_code.cast("string"), F.lit(""))) != ""
    person_name_present = (first.isNotNull() | surname.isNotNull())

    val = (
        F.when(role == F.lit("insurer"), F.lit("Organization"))
         .when(role == F.lit("producer"), F.lit("Organization"))
         .when(role == F.lit("insured"), F.when(bn_present, F.lit("Organization")).otherwise(F.lit("Person")))
         .when(role == F.lit("underwriter"), F.lit("Person"))
         .when(role == F.lit("broker"), F.lit("Person"))
         .when(role.isin("licensed individual", "licensed_individual", "licensedindividual"), F.lit("Person"))
         .when(bn_present | company_present | producer_present, F.lit("Organization"))
         .when(person_name_present, F.lit("Person"))
         .when(uw.isNotNull(), F.lit("Person"))
         .when(contact.isNotNull(), F.lit("Person"))
         .otherwise(F.lit(None))
    )

    return df.withColumn(target, val)


# ---------------------------------------------------------------------

def derive_concat(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    Generic concatenation of columns.
    """

    cols = args.get("cols", [])
    sep = args.get("separator", "")

    # Build a case-insensitive schema map to apply special formatting by source type.
    schema_map = {f.name.lower(): f.dataType for f in df.schema.fields}

    parts = []
    for c in cols:
        base_col = _col_or_null(df, c)

        # Keep legacy key format for timestamps: yyyy-MM-ddTHH:mm:ss
        if c and isinstance(schema_map.get(c.lower()), TimestampType):
            as_text = F.date_format(base_col.cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
        else:
            as_text = base_col.cast("string")

        # concat_ws skips NULLs; convert blank tokens to NULL to avoid "_..." artifacts.
        cleaned = F.when(F.trim(as_text) == F.lit(""), F.lit(None)).otherwise(F.trim(as_text))
        parts.append(cleaned)

    expr = F.concat_ws(sep, *parts)
    
    # Normalize if requested
    if args.get("normalize", False):
        expr = F.lower(F.ltrim(expr))

    return df.withColumn(target, expr)


def derive_plcy_id(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    HUB_PLCY business key derivation.
    Wraps derive_concat with normalize always enabled.

    Args:
      cols:      source columns to concat (typically [DISPLAY_POLICY_NUMBER, EFFECTIVE_DATE])
      separator: concat separator (default "_")
    """
    # Policy ID must preserve case; avoid lowercasing timestamp separator.
    local_args = dict(args or {})
    local_args["normalize"] = False

    return derive_concat(df, target, local_args)


def derive_undrly_plcy_id_ndyc(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    HUB_UNDRLY_PLCY NDYC (Excess) business key derivation.
    

    Args:
      cols:      source columns to concat (typically [EX_POLICY_NUMB, EFFECTIVE_DATE, CARRIER, NDYC_ID])
      separator: concat separator (default "_")
    """
    local_args = dict(args or {})
    local_args["normalize"] = False    

    return derive_concat(df, target, local_args)


def derive_undrly_plcy_id_rpec(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    HUB_UNDRLY_PLCY RPEC (QuotaShare) business key derivation.
    

    Args:
      cols:      source columns to concat (typically [QUOTA_POLICY_NUMBER, EFFECTIVE_DATE, CARRIER, RPEC_ID])
      separator: concat separator (default "_")
    """
    local_args = dict(args or {})
    local_args["normalize"] = False    

    return derive_concat(df, target, local_args)


from pyspark.sql.functions import col, concat_ws, when

def derive_insd_obj_id_bo(df, target, args):
    standard_cols = args.get("standard_cols", [])
    excess_cols = args.get("excess_cols", [])
    ev_cols = args.get("ev_cols", [])
    separator = args.get("separator", "_")

    # Gate ID creation on true business data presence, not MQP-only tokens.
    # Prefer explicit args if provided; otherwise infer by column prefixes.
    ycmc_check_cols = args.get("ycmc_cols")
    if ycmc_check_cols is None:
        ycmc_check_cols = [c for c in excess_cols if isinstance(c, str) and c.upper().startswith("YCMC_")]

    uivc_check_cols = args.get("uivc_cols")
    if uivc_check_cols is None:
        uivc_check_cols = [c for c in standard_cols if isinstance(c, str) and c.upper().startswith("UIVC_")]

    # EV pipelines key their presence off MLO_ (location) columns.
    ev_check_cols = args.get("ev_cols")
    if ev_check_cols is None:
        ev_check_cols = [c for c in ev_cols if isinstance(c, str) and c.upper().startswith("MLO_")]

    def _any_non_blank(col_names):
        cond = F.lit(False)
        for c in col_names:
            present = F.trim(F.coalesce(_col_or_null(df, c).cast("string"), F.lit(""))) != F.lit("")
            cond = cond | present
        return cond

    # Date/timestamp cols must render as yyyy-MM-ddTHH:mm:ss, not the default cast-to-string date.
    schema_map = {f.name.lower(): f.dataType for f in df.schema.fields}

    def _as_text(c):
        dtype = schema_map.get(c.lower()) if c else None
        base_col = _col_or_null(df, c)
        if isinstance(dtype, (TimestampType, DateType)):
            return F.date_format(base_col.cast("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
        return base_col.cast("string")

    # Skip null/blank tokens so concatenation does not create leading/double separators.
    def _build_parts(col_names):
        return [
            F.when(
                F.trim(_as_text(c)) == F.lit(""),
                F.lit(None),
            ).otherwise(F.trim(_as_text(c)))
            for c in col_names
        ]

    standard_expr = F.concat_ws(separator, *_build_parts(standard_cols))
    excess_expr = F.concat_ws(separator, *_build_parts(excess_cols))
    ev_expr = F.concat_ws(separator, *_build_parts(ev_cols))

    # Route expression by YCMC/EV business-data presence.
    has_ycmc_data = _any_non_blank(ycmc_check_cols)
    has_uivc_data = _any_non_blank(uivc_check_cols)
    has_ev_data = _any_non_blank(ev_check_cols)

    # Priority order is configurable so callers can resolve ambiguity when a
    # row unexpectedly has both YCMC (excess) and MLO (EV) data populated.
    # Default keeps existing behavior: excess wins, then ev, then standard.
    expr_by_name = {"excess": excess_expr, "ev": ev_expr, "standard": standard_expr}
    flag_by_name = {"excess": has_ycmc_data, "ev": has_ev_data, "standard": F.lit(True)}
    priority = args.get("priority", ["excess", "ev", "standard"])

    routed = None
    for name in priority:
        if name not in expr_by_name:
            continue
        cond, val = flag_by_name[name], expr_by_name[name]
        routed = F.when(cond, val) if routed is None else routed.when(cond, val)
    routed = routed.otherwise(standard_expr)

    # Build ID only when at least one YCMC/UIVC/EV business input exists.
    result = F.when(has_ycmc_data | has_uivc_data | has_ev_data, routed).otherwise(F.lit(None))

    # Normalize if requested; restore the ISO timestamp 'T' separator that lower() lowercases.
    if args.get("normalize", False):
        result = F.regexp_replace(F.lower(F.ltrim(result)), r"(\d)t(\d)", r"$1T$2")

    return df.withColumn(target, result)

from pyspark.sql.functions import col, lit, when

def derive_insd_obj_type_bo(df, target, args):
    risk_type_col = args.get("risk_type_col", "UIVC_C_INSURABLE_RISK_TYPE")
    excess_value = args.get("excess_value", "Excess_&_Surplus_Property")
    ev_value = args.get("ev_value","Location")
    
    ycmc_present = _col_or_null(df, "YCMC_C_STATE_CODE").isNotNull()
    mlo_present = _col_or_null(df, "MLO_LOCATION_NO").isNotNull()

    return df.withColumn(
        target,
        F.when(ycmc_present, F.lit(excess_value))
         .when(~ycmc_present & mlo_present, F.lit(ev_value))
         .otherwise(_col_or_null(df, risk_type_col))
    )

def default_value(df, target, args):
    """
    Assign a constant value to a column
    """
    value = args.get("value")

    return df.withColumn(target, F.lit(value))

def derive_cntct_pnt_id(df, target, args):
    mad_cols = args.get("mad_cols", [])
    ycmc_cols = args.get("ycmc_cols", [])
    sep = args.get("separator", "_")

    mad_parts = [
        F.when(
            F.trim(_col_or_null(df, c).cast("string")) == F.lit(""),
            F.lit(None),
        ).otherwise(F.trim(_col_or_null(df, c).cast("string")))
        for c in mad_cols
    ]

    ycmc_parts = [
        F.when(
            F.trim(_col_or_null(df, c).cast("string")) == F.lit(""),
            F.lit(None),
        ).otherwise(F.trim(_col_or_null(df, c).cast("string")))
        for c in ycmc_cols
    ]

    mad_expr = F.concat_ws(sep, *mad_parts)
    ycmc_expr = F.concat_ws(sep, *ycmc_parts)

    return df.withColumn(
        target,
        F.when(
            _col_or_null(df, "YCMC_C_STATE_CODE").isNotNull(),
            ycmc_expr
        ).otherwise(mad_expr)
    )

def derive_cntct_pnt_phys(df, target, args):
    # Preferred args: mad_cols / ycmc_cols. Legacy fallback: cols.
    mad = args.get("mad_cols") or args.get("cols") or []
    ycmc = args.get("ycmc_cols") or []
    sep = args.get("separator", "_")

    def _concat_non_blank(cols):
        return F.concat_ws(
            sep,
            *[
                F.when(F.trim(_col_or_null(df, c).cast("string")) == F.lit(""), F.lit(None))
                 .otherwise(F.trim(_col_or_null(df, c).cast("string")))
                for c in cols
            ]
        )

    def _any_non_blank(cols):
        cond = F.lit(False)
        for c in cols:
            present = F.trim(F.coalesce(_col_or_null(df, c).cast("string"), F.lit(""))) != F.lit("")
            cond = cond | present
        return cond

    mad_expr = _concat_non_blank(mad)
    ycmc_expr = _concat_non_blank(ycmc)

    # Do not key off YCMC_C_STATE_CODE. Use YCMC only when YCMC columns are
    # configured and actually populated; otherwise default to MAD expression.
    if ycmc:
        has_ycmc_data = _any_non_blank(ycmc)
        result = F.when(has_ycmc_data, ycmc_expr).otherwise(mad_expr)
    else:
        result = mad_expr

    if args.get("normalize", False):
        result = F.lower(F.ltrim(result))

    return df.withColumn(target, result)



def derive_column_copy(df, target, args):
    col_name = args.get("col")
    result = _col_or_null(df, col_name)
    if args.get("normalize", False):
        result = F.lower(F.ltrim(result))
    return df.withColumn(target, result)

def derive_cntct_type(df, target, args):
    """
    Determine contact point type based on available columns
    """

    return df.withColumn(
        target,
        F.when(F.col("MCN_E_MAIL").isNotNull(), F.lit("Electronic_Address"))
         .when(F.col("MCN_PHONE_1").isNotNull(), F.lit("Phone_Number"))
         .when(F.col("MAD_LINE_1").isNotNull(), F.lit("Physical Address"))
         .otherwise(F.lit(None))
    )


def coalesce_cols(df, target, args):
    """
    Return the first non-null value across a list of columns.
    Uses _col_or_null so missing columns are treated as NULL instead of
    raising AnalysisException.
    """
    cols = args.get("cols", [])
    exprs = [_col_or_null(df, c) for c in cols]
    return df.withColumn(target, F.coalesce(*exprs))

from pyspark.sql.functions import when, expr, lit

def derive_case_value(df, target, args):
    condition = args.get("condition")
    true_value = args.get("true_value")
    false_value = args.get("false_value")

    # Detect if value is SQL expression (has function calls/operators) or literal string
    def is_sql_expr(val):
        """True if value looks like SQL expression, False if plain literal"""
        if not val or not isinstance(val, str):
            return False
        # SQL expressions have: parentheses, SQL operators, or function names
        if '(' in val or ')' in val:  # Function calls like LOWER(...)
            return True
        if any(kw in val.upper() for kw in [' IS ', ' NOT ', 'COALESCE', 'CAST']):
            return True
        return False
    
    # Use expr() for SQL expressions, lit() for literal strings
    true_expr = expr(true_value) if is_sql_expr(true_value) else lit(true_value)
    false_expr = expr(false_value) if is_sql_expr(false_value) else lit(false_value)

    return df.withColumn(
        target,
        when(expr(condition), true_expr).otherwise(false_expr)
    )

from pyspark.sql.functions import when, expr, concat_ws, col

def derive_case_concat(df, target, args):
    condition = args.get("condition")
    true_cols = args.get("true_cols", [])
    false_cols = args.get("false_cols", [])
    sep = args.get("separator", "_")

    true_expr = concat_ws(
        sep,
        *[
            F.when(F.trim(_col_or_null(df, c).cast("string")) == F.lit(""), F.lit(None))
             .otherwise(F.trim(_col_or_null(df, c).cast("string")))
            for c in true_cols
        ]
    )
    false_expr = concat_ws(
        sep,
        *[
            F.when(F.trim(_col_or_null(df, c).cast("string")) == F.lit(""), F.lit(None))
             .otherwise(F.trim(_col_or_null(df, c).cast("string")))
            for c in false_cols
        ]
    )

    return df.withColumn(
        target,
        when(expr(condition), true_expr).otherwise(false_expr)
    )


def derive_mapped_value(df, target, args):
    source_col = args.get("source_col")
    mapping = args.get("mapping", {}) or {}
    default_mode = (args.get("default_mode") or "source").lower()

    source_expr = _col_or_null(df, source_col).cast("string")
    mapped_expr = None

    for k, v in mapping.items():
        cond = F.trim(source_expr) == F.lit(str(k))
        mapped_expr = F.when(cond, F.lit(v)) if mapped_expr is None else mapped_expr.when(cond, F.lit(v))

    if mapped_expr is None:
        mapped_expr = F.lit(None)

    if default_mode == "null":
        mapped_expr = mapped_expr.otherwise(F.lit(None))
    else:
        mapped_expr = mapped_expr.otherwise(source_expr)

    return df.withColumn(target, mapped_expr)


def derive_party_id(df, target, args):
    """
    HUB_PRTY business key derivation for PRTY_ID.

    Mapping implemented from STTM logic:
      - Insured:
          if BusinessName is not null -> concat(BusinessName, '_')
          else -> concat(FirstName, '_', MiddleName, '_', Surname, '_')
      - Insurer: Company
      - Underwriter: concat(UnderwriterName, '_', UnderwriterCode)
      - Broker: Broker ContactName
      - Licensed Individual: Producer ContactName
      - Parent Agency

    Args:
      role_col, business_name, first_name, middle_name, surname,
      company_name, underwriter_name, underwriter_code,
      broker_contact_name, licensed_contact_name
    """

    role_col = args.get("role_col")
    
    business_name = _col_or_null(df, args.get("business_name")).cast("string")
    first_name = _col_or_null(df, args.get("first_name")).cast("string")
    middle_name = _col_or_null(df, args.get("middle_name")).cast("string")
    surname = _col_or_null(df, args.get("surname")).cast("string")
    company_name = _col_or_null(df, args.get("company_name")).cast("string")
    underwriter_name = _col_or_null(df, args.get("underwriter_name")).cast("string")
    underwriter_code = _col_or_null(df, args.get("underwriter_code")).cast("string")
    broker_contact_name = _col_or_null(df, args.get("broker_contact_name")).cast("string")
    licensed_contact_name = _col_or_null(df, args.get("licensed_contact_name")).cast("string")
    producer_code = _col_or_null(df, args.get("producer_code")).cast("string")
    parentagency_name = _col_or_null(df, args.get("parentagency_name")).cast("string")
    parentagency_number = _col_or_null(df, args.get("parentagency_number")).cast("string")
    

    role = F.lower(F.trim(_col_or_null(df, role_col).cast("string")))

    # Null/blank-safe tokens to avoid leading/double/trailing separators.
    first_tok = F.when(F.trim(F.coalesce(first_name, F.lit(""))) == "", F.lit(None)).otherwise(F.trim(first_name))
    middle_tok = F.when(F.trim(F.coalesce(middle_name, F.lit(""))) == "", F.lit(None)).otherwise(F.trim(middle_name))
    surname_tok = F.when(F.trim(F.coalesce(surname, F.lit(""))) == "", F.lit(None)).otherwise(F.trim(surname))
    uw_name_tok = F.when(F.trim(F.coalesce(underwriter_name, F.lit(""))) == "", F.lit(None)).otherwise(F.trim(underwriter_name))
    uw_code_tok = F.when(F.trim(F.coalesce(underwriter_code, F.lit(""))) == "", F.lit(None)).otherwise(F.trim(underwriter_code))
    producer_code_tok = F.when(F.trim(F.coalesce(producer_code, F.lit(""))) == "", F.lit(None)).otherwise(F.trim(producer_code))
    broker_contact_tok = F.when(F.trim(F.coalesce(broker_contact_name, F.lit(""))) == "", F.lit(None)).otherwise(F.trim(broker_contact_name))
    parentagency_name_tok = F.when(F.trim(F.coalesce(parentagency_name, F.lit(""))) == "", F.lit(None)).otherwise(F.trim(parentagency_name))
    parentagency_number_tok = F.when(F.trim(F.coalesce(parentagency_number, F.lit(""))) == "", F.lit(None)).otherwise(F.trim(parentagency_number))

    insured_person = F.concat_ws("_", first_tok, middle_tok, surname_tok)

    insured_value = F.when(
        F.trim(F.coalesce(insured_person, F.lit(""))) == "",
        F.lit(None)
    ).otherwise(insured_person)

    underwriter_value = F.concat_ws("_", uw_name_tok, uw_code_tok)
    # Producer should resolve from ORG path only when producer code exists.
    producer_value = F.when(
        producer_code_tok.isNotNull(),
        F.concat_ws("_", producer_code_tok, broker_contact_tok)
    ).otherwise(F.lit(None))

    # ParentAgency
    parentagency = F.concat_ws("_", parentagency_name_tok, parentagency_number_tok)
    parentagency_value = F.when(
        F.trim(F.coalesce(parentagency, F.lit(""))) == "",
        F.lit(None)
    ).otherwise(parentagency)

    # Insured: prioritize business_name (ORG insured) over person concat (PERS insured)
    insured_priority = F.when(
        F.trim(F.coalesce(business_name, F.lit(""))) != F.lit(""),
        business_name
    ).otherwise(insured_value)

    derived = (
        F.when(role == F.lit("insured"), insured_priority)
         .when(role == F.lit("insurer"), company_name)
         .when(role == F.lit("underwriter"), underwriter_value)
         .when(role == F.lit("broker"), broker_contact_name)
         .when(role == F.lit("producer"), producer_value)
         .when(role == F.lit("parentagency"), parentagency_value)
         .when(role == F.lit("underwriter"), underwriter_value)
         .when(role.isin("licensed individual", "licensed_individual", "licensedindividual"), licensed_contact_name)
         .otherwise(F.lit(None))
    )

    result = F.when(F.trim(F.coalesce(derived, F.lit(""))) == "", F.lit(None)).otherwise(derived)
    
    # Normalize if requested
    if args.get("normalize", False):
        result = F.lower(F.ltrim(result))

    return df.withColumn(target, result)




def derive_party_role_type(df, target, args=None):
    """
    Map PERS_TYPE or ORG_TYPE to standardized role codes.
    
        Safely handles cases where only one type column exists (PERS or ORG pipeline).
        Uses _col_or_null to avoid AnalysisException on missing columns.

        Optional args:
            org_type_col: ORG role column name (set only for ORG pipelines)
            pers_type_col: PERS role column name (set only for PERS pipelines)

        Behavior:
            - If args are omitted, defaults to ORG_TYPE and PERS_TYPE for backward compatibility.
            - If one arg is omitted while the other is provided, omitted side is ignored.
    
    PERS_TYPE mappings:
      insured -> Insured
      underwriter -> Underwriter
      broker -> Broker
      licensed_individual -> Licensed_Individual
    
    ORG_TYPE mappings:
      insurer -> Insurer
      insured -> Insured
      producer -> Selling_Agency
    """
    from pyspark.sql import functions as F

    args = args or {}
    has_org_arg = "org_type_col" in args
    has_pers_arg = "pers_type_col" in args

    # Backward compatible defaults only when neither arg is provided.
    if not has_org_arg and not has_pers_arg:
        org_type_col = "ORG_TYPE"
        pers_type_col = "PERS_TYPE"
    else:
        org_type_col = args.get("org_type_col")
        pers_type_col = args.get("pers_type_col")

    org_type = (
        F.lower(F.trim(_col_or_null(df, org_type_col).cast("string")))
        if org_type_col else F.lit(None).cast("string")
    )
    pers_type = (
        F.lower(F.trim(_col_or_null(df, pers_type_col).cast("string")))
        if pers_type_col else F.lit(None).cast("string")
    )

    return df.withColumn(
        target,
        F.when(org_type == F.lit("insurer"), F.lit("Insurer"))
         .when(org_type == F.lit("insured"), F.lit("Insured"))
         .when(org_type == F.lit("producer"), F.lit("Selling_Agency"))
         .when(org_type == F.lit("parentagency"), F.lit("ParentAgency"))
         .when(pers_type == F.lit("insured"), F.lit("Insured"))
         .when(pers_type == F.lit("underwriter"), F.lit("Underwriter"))
         .when(pers_type == F.lit("broker"), F.lit("Broker"))
         .when(pers_type == F.lit("licensed_individual"), F.lit("Licensed_Individual"))
         .otherwise(F.lit(None))
    )


def derive_null_if_empty(df, target, args):
    source_col = args.get("source_col", target)
    col_expr = _col_or_null(df, source_col).cast("string")
    return df.withColumn(
        target,
        F.when(F.trim(F.coalesce(col_expr, F.lit(""))) == "", F.lit(None)).otherwise(col_expr)
    )


# ---------------------------------------------------------------------

def derive_limit_type(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    Derive a limit-type label based on which source column is populated.

    Priority (first non-null wins):
      agg_col       -> 'Aggregate Limit'   (RPEC quota path)
      occ_col       -> 'Occurrence Limit'  (RPEC quota path)
      layer_col     -> 'Layer Limit'       (NDYC excess path)
      excess_col    -> 'Excess Limit'      (NDYC excess path)
      qbe_col       -> 'Qbe Limit'         (NDYC excess path)
      per_layer_col -> 'Per Layer Limit'   (NDYC excess path)

    Usage in ctl_rule params_json:
      {
        "fn": "derive_limit_type",
        "target": "LMT_TP_CD",
        "args": {
          "agg_col":       "RPEC_C_CARRIER_AGGREGATE",
          "occ_col":       "RPEC_C_CARRIER_OCCURRENCE",
          "layer_col":     "NDYC_C_LAYER_LIMIT",
          "excess_col":    "NDYC_C_LIMIT",
          "qbe_col":       "NDYC_C_QBE_LIMIT",
          "per_layer_col": "NDYC_C_PER_LAYER_LIMIT"
        }
      }
    """
    agg       = _col_or_null(df, args.get("agg_col"))
    occ       = _col_or_null(df, args.get("occ_col"))
    layer     = _col_or_null(df, args.get("layer_col"))
    excess    = _col_or_null(df, args.get("excess_col"))
    qbe       = _col_or_null(df, args.get("qbe_col"))
    per_layer = _col_or_null(df, args.get("per_layer_col"))

    val = (
        F.when(agg.isNotNull(),       F.lit("Aggregate Limit"))
         .when(occ.isNotNull(),       F.lit("Occurrence Limit"))
         .when(layer.isNotNull(),     F.lit("Layer Limit"))
         .when(excess.isNotNull(),    F.lit("Excess Limit"))
         .when(qbe.isNotNull(),       F.lit("Qbe Limit"))
         .when(per_layer.isNotNull(), F.lit("Per Layer Limit"))
         .otherwise(F.lit("Unspecified"))  # Default when no limit columns are populated
    )

    return df.withColumn(target, val)


def derive_cvrg_id_input(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    HUB_CVRG hash input derivation based on product code.
    
    Product-code-driven logic:
      - If PRODUCT_CODE = 'BO': hash input = CVRG_ID only (product-specific uniqueness)
      - Otherwise: hash input = CVRG_ID + REC_SRC_NM (source-aware uniqueness)
    
    Args:
      cvrg_id_col: column name for coverage ID (typically 'CVRG_ID')
      rec_src_nm_col: column name for record source (typically 'REC_SRC_NM')
      product_code_col: column name for product code (typically 'PRODUCT_CODE')
      separator: separator for concatenation (typically '_')
    """
    
    cvrg_id_col = args.get("cvrg_id_col", "CVRG_ID")
    rec_src_nm_col = args.get("rec_src_nm_col", "REC_SRC_NM")
    product_code_col = args.get("product_code_col", "PRODUCT_CODE")
    sep = args.get("separator", "_")
    
    cvrg_id = _col_or_null(df, cvrg_id_col).cast("string")
    rec_src_nm = _col_or_null(df, rec_src_nm_col).cast("string")
    product_code = F.upper(F.trim(_col_or_null(df, product_code_col).cast("string")))
    
    # Clean tokens to avoid leading/double/trailing separators
    cvrg_tok = F.when(F.trim(cvrg_id) == F.lit(""), F.lit(None)).otherwise(F.trim(cvrg_id))
    rec_src_tok = F.when(F.trim(rec_src_nm) == F.lit(""), F.lit(None)).otherwise(F.trim(rec_src_nm))
    
    # Product-code decision: BO excludes REC_SRC_NM, others include it
    bo_expr = F.concat_ws(sep, cvrg_tok)
    other_expr = F.concat_ws(sep, cvrg_tok, rec_src_tok)
    
    return df.withColumn(
        target,
        F.when(product_code == F.lit("BO"), bo_expr).otherwise(other_expr)
    )


def derive_cvrg_id(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    HUB_CVRG combined derivation — single rule replaces concat + normalize + hash-input steps.

    Steps performed:
      1. Concat cols into CVRG_ID using separator
      2. Normalize: lower(ltrim(CVRG_ID))
      3. Null guard: if filter_col is null/blank, CVRG_ID is set to null (drives downstream filter_not_null)
      4. Build CVRG_HASH_INPUT conditional on product code:
           BO   -> concat(CVRG_ID)
           else -> concat(CVRG_ID, REC_SRC_NM)

    Args:
      cols:             source columns to concat for the business key
      separator:        concat separator (default "_")
      filter_col:       column whose null/blank means no valid key (e.g. "C_COVERAGE_CODE")
      product_code_col: product code column for BO check (default "PRODUCT_CODE")
      rec_src_nm_col:   record source column (default "REC_SRC_NM")
      hash_input_col:   derived hash-input column name (default "CVRG_HASH_INPUT")
    """
    cols             = args.get("cols", [])
    sep              = args.get("separator", "_")
    filter_col       = args.get("filter_col")
    product_code_col = args.get("product_code_col", "PRODUCT_CODE")
    rec_src_nm_col   = args.get("rec_src_nm_col",   "REC_SRC_NM")
    hash_input_col   = args.get("hash_input_col",   "CVRG_HASH_INPUT")

    # Step 1: concat (null/blank-safe tokens)
    parts = [
        F.when(F.trim(_col_or_null(df, c).cast("string")) == F.lit(""), F.lit(None))
         .otherwise(F.trim(_col_or_null(df, c).cast("string")))
        for c in cols
    ]
    concat_expr = F.concat_ws(sep, *parts)

    # Step 2: normalize
    normalized = F.lower(F.ltrim(concat_expr))

    # Step 3: null guard — if filter_col is null/blank the whole key becomes null
    if filter_col:
        filter_val = F.trim(F.coalesce(_col_or_null(df, filter_col).cast("string"), F.lit("")))
        normalized = F.when(filter_val != F.lit(""), normalized).otherwise(F.lit(None))

    df = df.withColumn(target, normalized)

    # Step 4: conditional hash input
    product_code = F.upper(F.trim(_col_or_null(df, product_code_col).cast("string")))
    cvrg_id      = _col_or_null(df, target).cast("string")
    rec_src_nm   = _col_or_null(df, rec_src_nm_col).cast("string")

    cvrg_tok    = F.when(F.trim(F.coalesce(cvrg_id,    F.lit(""))) == F.lit(""), F.lit(None)).otherwise(F.trim(cvrg_id))
    rec_src_tok = F.when(F.trim(F.coalesce(rec_src_nm, F.lit(""))) == F.lit(""), F.lit(None)).otherwise(F.trim(rec_src_nm))

    bo_hash    = F.concat_ws(sep, cvrg_tok)
    other_hash = F.concat_ws(sep, cvrg_tok, rec_src_tok)

    df = df.withColumn(
        hash_input_col,
        F.when(product_code == F.lit("BO"), bo_hash).otherwise(other_hash)
    )

    return df

def derive_lob_id(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    HUB_LOB business key derivation.
    Wraps derive_column_copy without normalize.

    Args:
      col: source column to copy (typically MQP_C_SUB_PRODUCT_CODE)
    """
    return derive_column_copy(df, target, args)


def derive_prem_tran_id(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    Concatenate ordered hash-key columns (product-aware, fully dynamic), skipping NULLs without adding blank strings.
    
    Supports THREE modes:
    
    1. DYNAMIC MODE WITH EXTERNAL MAP (recommended - most flexible):
       Pass "product_code_col" and "product_column_map" to read product from data
       and use a map passed in args to define columns per product.
       
       Usage in seed file:
       {
         "fn":"derive_prem_tran_id",
         "target":"PREM_TRAN_ID",
         "args":{
           "product_code_col":"MQP_PRODUCT_CODE",
           "product_column_map":{
             "BO": ["QBE_HASH_PLCY_ID", "QBE_HASH_LOB_ID", "QBE_HASH_CVRG_ID", 
                    "QBE_HASH_INSD_OBJ_ID", "QBE_HASH_CNTCT_PNT_ID", 
                    "QBE_HASH_PLCY_PRTY_ROLE_ID", "REF_ID", "REC_SRC_NM"],
             "EV": ["QBE_HASH_PLCY_ID", "QBE_HASH_LOB_ID", "QBE_HASH_CVRG_ID", 
                    "ST_PRVNC_CD", "TRAN_TP_CD", "QBE_HASH_PLCY_PRTY_ROLE_ID", 
                    "REF_ID", "REC_SRC_NM"]
           },
           "separator":"_"
         }
       }
    
    2. Static product mode (product-specific seed files):
       Pass "product" and "product_column_map" for a fixed product code
    
    3. Legacy mode (backward compatible):
       Pass "cols" arg with explicit column list for all rows
    """
    # Extract arguments
    product_code_col = args.get("product_code_col")        # Dynamic: read from column
    product = args.get("product")                           # Static: fixed product
    product_column_map = args.get("product_column_map", {}) # External map for all products
    cols = args.get("cols", [])                             # Legacy: explicit columns
    sep = args.get("separator", "_")

    # =========================================================================
    # MODE 1: DYNAMIC WITH EXTERNAL MAP (Single seed file for all products)
    # =========================================================================
    if product_code_col and product_column_map:
        logger.info("[derive_prem_tran_id] DYNAMIC mode with external map using product_code_col='%s' for target %s", 
                    product_code_col, target)
        
        prod_col = F.upper(F.trim(_col_or_null(df, product_code_col).cast("string")))
        
        # Helper to build concat expression for a column set
        def _build_concat(col_list):
            if not col_list:
                return F.lit(None).cast("string")
            parts = []
            for c in col_list:
                base_col = _col_or_null(df, c)
                as_text = base_col.cast("string")
                cleaned = F.when(F.trim(as_text) == F.lit(""), F.lit(None)).otherwise(F.trim(as_text))
                parts.append(cleaned)
            return F.concat_ws(sep, *parts)
        
        # Build concat expressions for each product in the map
        product_exprs = {}
        for prod_key, col_list in product_column_map.items():
            product_exprs[prod_key] = _build_concat(col_list)
            logger.info("[derive_prem_tran_id] Registered product '%s' with %d columns", prod_key, len(col_list))
        
        # Route by product code using CASE WHEN
        concat_expr = None
        for prod_key, expr in product_exprs.items():
            if concat_expr is None:
                concat_expr = F.when(prod_col == F.lit(prod_key.upper()), expr)
            else:
                concat_expr = concat_expr.when(prod_col == F.lit(prod_key.upper()), expr)
        
        # Default fallback: use first product or NULL
        if concat_expr is not None:
            first_product_cols = list(product_column_map.values())[0]
            concat_expr = concat_expr.otherwise(_build_concat(first_product_cols))
        else:
            concat_expr = F.lit(None).cast("string")
        
        logger.info("[derive_prem_tran_id] Built dynamic routing for %d products -> %s", 
                    len(product_column_map), target)
        
        return df.withColumn(target, concat_expr)
    
    # =========================================================================
    # MODE 2: STATIC PRODUCT WITH EXTERNAL MAP
    # =========================================================================
    elif product and product_column_map:
        prod_upper = product.upper()
        if prod_upper in {k.upper(): v for k, v in product_column_map.items()}:
            # Find the matching key (case-insensitive)
            for map_key, col_list in product_column_map.items():
                if map_key.upper() == prod_upper:
                    cols = col_list
                    logger.info("[derive_prem_tran_id] STATIC mode using product='%s' from map (%d columns) for target %s", 
                                product, len(cols), target)
                    break
        else:
            logger.warning("[derive_prem_tran_id] product '%s' not found in product_column_map for target %s", product, target)
            return df
    
    # =========================================================================
    # MODE 3: LEGACY (Explicit column list)
    # =========================================================================
    elif cols:
        logger.info("[derive_prem_tran_id] LEGACY mode using explicit cols (%d columns) for target %s", len(cols), target)
    
    # =========================================================================
    # FALLBACK: No valid input
    # =========================================================================
    else:
        logger.warning("[derive_prem_tran_id] no product_code_col+product_column_map, product+product_column_map, or cols for target %s; skipping", target)
        return df

    # Build parts: convert empty strings to NULL so concat_ws skips them cleanly
    parts = []
    for c in cols:
        base_col = _col_or_null(df, c)
        as_text = base_col.cast("string")
        # Convert blank tokens to NULL to avoid extra separator artifacts
        cleaned = F.when(F.trim(as_text) == F.lit(""), F.lit(None)).otherwise(F.trim(as_text))
        parts.append(cleaned)

    # Concatenate with separator (concat_ws automatically skips NULLs)
    concat_expr = F.concat_ws(sep, *parts)

    logger.info("[derive_prem_tran_id] Concatenating %d columns with separator '%s' -> %s (skips NULLs)", len(cols), sep, target)

    return df.withColumn(target, concat_expr)


# Parsed reference payloads keyed by (table, filter_col, filter_value); avoids one
# driver-side collect() + JSON parse per rule invocation.
_LOOKUP_RECORDS_CACHE = {}


def _load_lookup_records(spark, table_name, lookup_filter_col, lookup_filter_value, payload_col):
    """Return normalized (blank -> 'NoValue') lookup records for a reference key."""
    cache_key = (table_name, lookup_filter_col, str(lookup_filter_value).lower(), payload_col)
    if cache_key in _LOOKUP_RECORDS_CACHE:
        return _LOOKUP_RECORDS_CACHE[cache_key]

    ref_rows = (
        spark.table(table_name)
        .filter(F.lower(F.col(lookup_filter_col)) == F.lit(str(lookup_filter_value).lower()))
        .select(payload_col)
        .limit(1)
        .collect()
    )

    norm_records = []
    if not ref_rows:
        logger.warning("[derive_recursive_lookup] No ref rows for key=%s in table=%s", lookup_filter_value, table_name)
    else:
        first_payload = ref_rows[0][payload_col]
        parsed = None
        if first_payload is not None:
            try:
                parsed = json.loads(first_payload) if isinstance(first_payload, str) else first_payload
            except Exception as ex:
                logger.warning("[derive_recursive_lookup] Failed to parse payload for key=%s: %s", lookup_filter_value, ex)

        if isinstance(parsed, dict):
            records = [parsed]
        elif isinstance(parsed, list):
            records = [r for r in parsed if isinstance(r, dict)]
        else:
            records = []

        for rec in records:
            n = {}
            for k, v in rec.items():
                sv = "" if v is None else str(v).strip()
                n[str(k)] = sv if sv != "" else "NoValue"
            norm_records.append(n)

    _LOOKUP_RECORDS_CACHE[cache_key] = norm_records
    logger.info(
        "[derive_recursive_lookup] Loaded %d lookup record(s) for key=%s from %s",
        len(norm_records), lookup_filter_value, table_name,
    )
    return norm_records


def derive_recursive_lookup(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    Legacy parity mode for recursive lookup WITHOUT Python UDF.

    Supports product-keyed source_to_lookup dicts (e.g. {"EV": {...}, "BO": {...}}).
    Defaults product_code_col to "PART_COL" if omitted.

    Why this implementation:
    - Avoids Python worker serialization of local module closures.
    - Prevents executor-side ModuleNotFoundError for 'framework'.
    - Keeps wildcard lookup semantics (exact value fallback to 'NoValue').
    """
    spark = df.sparkSession

    table_name = _resolve_existing_table(spark, [args.get("lookup_table")])
    if not table_name:
        logger.warning("[derive_recursive_lookup] Lookup table not found: %s", args.get("lookup_table"))
        return df.withColumn(target, F.lit(None).cast("string"))

    lookup_filter_col = args.get("lookup_filter_col", "DATA_KEY_ID")
    lookup_filter_value = args.get("lookup_filter_value")
    payload_col = args.get("payload_col", "DATA_VAL")
    return_col = args.get("return_col", "Global_Code")
    source_to_lookup = args.get("source_to_lookup", {}) or {}
    product_code_col = args.get("product_code_col", "PART_COL")
    filter_eff_exp = args.get("filter-Eff-Exp", {}) or {}

    if not lookup_filter_value:
        raise ValueError("[derive_recursive_lookup] 'lookup_filter_value' is required")
    if not source_to_lookup:
        raise ValueError("[derive_recursive_lookup] 'source_to_lookup' is required")

    # Detect if source_to_lookup is product-keyed: e.g. {"EV": {"col": "lkp"}, "BO": {...}}
    is_product_keyed = isinstance(source_to_lookup, dict) and any(
        isinstance(v, dict) for v in source_to_lookup.values()
    )

    norm_records = _load_lookup_records(
        spark, table_name, lookup_filter_col, lookup_filter_value, payload_col
    )
    if not norm_records:
        return df.withColumn(target, F.lit(None).cast("string"))

    eff_lookup_col = None
    exp_lookup_col = None
    eff_source_col = None
    exp_source_col = None
    if isinstance(filter_eff_exp, dict) and filter_eff_exp:
        lower_to_actual = {str(k).lower(): k for k in filter_eff_exp.keys()}

        eff_lookup_col = lower_to_actual.get("effective_date") or lower_to_actual.get("effective_from")
        exp_lookup_col = lower_to_actual.get("expiration_date") or lower_to_actual.get("effective_to")

        # Fallback to first two configured keys when canonical names are not present.
        if eff_lookup_col is None or exp_lookup_col is None:
            keys = list(filter_eff_exp.keys())
            if eff_lookup_col is None and len(keys) >= 1:
                eff_lookup_col = keys[0]
            if exp_lookup_col is None and len(keys) >= 2:
                exp_lookup_col = keys[1]

        eff_source_col = filter_eff_exp.get(eff_lookup_col) if eff_lookup_col is not None else None
        exp_source_col = filter_eff_exp.get(exp_lookup_col) if exp_lookup_col is not None else None

    # Normalize to an ordered list of (product_code | None, {source_col: lookup_col}).
    if is_product_keyed:
        product_maps = [
            (str(p).upper(), m)
            for p, m in source_to_lookup.items()
            if isinstance(m, dict) and m
        ]
    else:
        product_maps = [(None, source_to_lookup)] if isinstance(source_to_lookup, dict) else []

    if not product_maps:
        return df.withColumn(target, F.lit(None).cast("string"))

    df_cols = {c.lower(): c for c in df.columns}
    eff_src_col = df_cols.get(str(eff_source_col).lower()) if eff_source_col else None
    exp_src_col = df_cols.get(str(exp_source_col).lower()) if exp_source_col else None
    use_eff_exp_filter = bool(eff_lookup_col and exp_lookup_col and (eff_src_col or exp_src_col))

    max_keys = max(len(m) for _, m in product_maps)

    # One broadcastable relation covering every product. Building a single join
    # (instead of filter/union per product plus a re-join back to the source)
    # keeps the logical plan linear; the previous shape duplicated the whole
    # upstream plan per lookup rule and blew up the driver during analysis.
    lk_field_names = ["__prod"] + [f"__k{i}" for i in range(max_keys)] + ["__eff", "__exp", "__ret"]
    lk_schema = StructType([StructField(n, StringType(), True) for n in lk_field_names])

    lookup_rows = []
    for p_code, stl_map in product_maps:
        lookup_keys = list(stl_map.values())
        for rec in norm_records:
            row = [p_code if p_code is not None else "__ALL__"]
            for i in range(max_keys):
                v = _get_ci(rec, lookup_keys[i]) if i < len(lookup_keys) else None
                row.append(str(v) if v is not None else "NoValue")
            eff_v = _get_ci(rec, eff_lookup_col) if eff_lookup_col else None
            exp_v = _get_ci(rec, exp_lookup_col) if exp_lookup_col else None
            ret_v = _get_ci(rec, return_col)
            row.append(str(eff_v) if eff_v is not None else None)
            row.append(str(exp_v) if exp_v is not None else None)
            row.append(str(ret_v) if ret_v is not None else None)
            lookup_rows.append(row)

    if not lookup_rows:
        return df.withColumn(target, F.lit(None).cast("string"))

    lk_df = spark.createDataFrame(lookup_rows, schema=lk_schema)

    def _norm_src(col_name):
        actual = df_cols.get(str(col_name).lower()) if col_name else None
        if actual is None:
            return F.lit("NoValue")
        v = F.trim(F.col(actual).cast("string"))
        return F.when(F.coalesce(v, F.lit("")) == F.lit(""), F.lit("NoValue")).otherwise(v)

    prod_expr = (
        F.upper(F.trim(_col_or_null(df, product_code_col).cast("string")))
        if is_product_keyed else F.lit("__ALL__")
    )

    # Each positional key resolves to a different source column per product.
    src_key_exprs = []
    for i in range(max_keys):
        e = None
        for p_code, stl_map in product_maps:
            keys = list(stl_map.keys())
            val = _norm_src(keys[i]) if i < len(keys) else F.lit("NoValue")
            if p_code is None:
                e = val
            else:
                cond = prod_expr == F.lit(p_code)
                e = F.when(cond, val) if e is None else e.when(cond, val)
        if is_product_keyed:
            e = e.otherwise(F.lit("NoValue"))
        src_key_exprs.append(e)

    src_df = df.withColumn("__dv_row_id", F.monotonically_increasing_id()).withColumn("__dv_prod", prod_expr)
    for i, e in enumerate(src_key_exprs):
        src_df = src_df.withColumn(f"__dv_s{i}", e)

    s = src_df.alias("s")
    l = F.broadcast(lk_df).alias("l")

    join_cond = F.col("l.__prod") == F.col("s.__dv_prod")
    match_score = F.lit(0)
    for i in range(max_keys):
        exact = F.col(f"l.__k{i}") == F.col(f"s.__dv_s{i}")
        wildcard = F.col(f"l.__k{i}") == F.lit("NoValue")
        join_cond = join_cond & (exact | wildcard)
        match_score = match_score + F.when(exact, F.lit(1)).otherwise(F.lit(0))

    joined = s.join(l, join_cond, "left")

    def _safe_parse_ts(col_ref):
        return F.coalesce(
            F.expr(f"try_to_timestamp({col_ref})"),
            F.expr(f"try_to_timestamp({col_ref}, 'M/d/yyyy')"),
            F.expr(f"try_to_timestamp({col_ref}, 'MM/dd/yyyy')"),
            F.expr(f"try_to_timestamp({col_ref}, 'M/d/yy')"),
            F.expr(f"try_to_timestamp({col_ref}, 'MM/dd/yy')"),
            F.expr(f"try_to_timestamp({col_ref}, 'yyyy-MM-dd')"),
            F.expr(f"try_to_timestamp({col_ref}, 'yyyy-MM-dd HH:mm:ss')"),
            F.expr(f"try_to_timestamp({col_ref}, \"yyyy-MM-dd'T'HH:mm:ss\")"),
            F.expr(f"try_to_timestamp({col_ref}, 'M/d/yyyy H:mm:ss')"),
            F.expr(f"try_to_timestamp({col_ref}, 'MM/dd/yyyy HH:mm:ss')"),
            F.expr(f"try_cast({col_ref} as timestamp)")
        )

    if use_eff_exp_filter:
        src_eff_ts = _safe_parse_ts(f"s.`{eff_src_col}`") if eff_src_col else F.lit(None).cast("timestamp")
        src_exp_ts = _safe_parse_ts(f"s.`{exp_src_col}`") if exp_src_col else src_eff_ts
        rec_eff_ts = _safe_parse_ts("l.`__eff`")
        rec_exp_ts = _safe_parse_ts("l.`__exp`")

        date_ok = (
            src_eff_ts.isNull() |
            src_exp_ts.isNull() |
            (
                (F.coalesce(rec_eff_ts, F.to_timestamp(F.lit("0001-01-01 00:00:00"))) <= src_eff_ts)
                &
                (F.coalesce(rec_exp_ts, F.to_timestamp(F.lit("9999-12-31 23:59:59"))) >= src_exp_ts)
            )
        )
        joined = joined.withColumn("__date_ok", date_ok)
    else:
        joined = joined.withColumn("__date_ok", F.lit(True))

    ranked = (
        joined
        .withColumn("__match_score", match_score)
        .withColumn(
            "__rn",
            F.row_number().over(
                Window.partitionBy(F.col("s.__dv_row_id")).orderBy(
                    F.col("__date_ok").desc(),
                    F.col("__match_score").desc(),
                )
            ),
        )
    )

    keep_cols = [F.col(f"s.`{c}`") for c in df.columns if c != target]
    result = (
        ranked
        .filter(F.col("__rn") == 1)
        .select(
            *keep_cols,
            F.when(F.col("__date_ok"), F.col("l.__ret")).otherwise(F.lit(None)).cast("string").alias(target),
        )
    )

    logger.info(
        "[derive_recursive_lookup] key=%s table=%s target=%s mappings=%s eff_exp=%s",
        lookup_filter_value,
        table_name,
        target,
        source_to_lookup,
        {
            "effective_lookup_col": eff_lookup_col,
            "expiration_lookup_col": exp_lookup_col,
            "effective_source_col": eff_source_col,
            "expiration_source_col": exp_source_col,
            "enabled": bool(eff_lookup_col and exp_lookup_col),
        },
    )
    return result

# ---------------------------------------------------------------------
# REGISTRY
# ---------------------------------------------------------------------

DERIVATIONS = {
    "concat_name": concat_name,
    "trim_col": trim_col,
    "derive_full_name": derive_full_name,
    "derive_party_id": derive_party_id,
    "derive_party_type": derive_party_type,
    "derive_concat": derive_concat,
    "derive_plcy_id": derive_plcy_id,
    "derive_undrly_plcy_id_ndyc": derive_undrly_plcy_id_ndyc,
    "derive_undrly_plcy_id_rpec": derive_undrly_plcy_id_rpec,
    "derive_insd_obj_id_bo": derive_insd_obj_id_bo,
    "derive_insd_obj_type_bo": derive_insd_obj_type_bo,
    "default_value": default_value,
    "derive_cntct_pnt_id":derive_cntct_pnt_id,
    "derive_cntct_pnt_phys":derive_cntct_pnt_phys,
    "derive_column_copy":derive_column_copy,
    "derive_cntct_type": derive_cntct_type,
    "coalesce_cols":coalesce_cols,
    "derive_case_value":derive_case_value,
    "derive_case_concat":derive_case_concat,
    "derive_mapped_value":derive_mapped_value,
    "derive_party_role_type":derive_party_role_type,
    "derive_null_if_empty":derive_null_if_empty,
    "derive_limit_type":derive_limit_type,
    "derive_cvrg_id_input":derive_cvrg_id_input,
    "derive_cvrg_id":derive_cvrg_id,
    "derive_lob_id": derive_lob_id,
    "derive_prem_tran_id": derive_prem_tran_id,
    "derive_recursive_lookup":derive_recursive_lookup,
    "derive_sql_expr":derive_sql_expr
}


# ---------------------------------------------------------------------
# DISPATCHER
# ---------------------------------------------------------------------

def apply_derivation(df: DataFrame, fn_name: str, target: str, args: dict) -> DataFrame:
    """
    Execute derivation by name.
    Case-insensitive lookup.
    """

    fn = DERIVATIONS.get((fn_name or "").lower())

    if fn is None:
        raise ValueError(
            f"[dv_derivations] Unknown transform_fn '{fn_name}'. "
            f"Available: {sorted(DERIVATIONS)}"
        )

    logger.info(
        "[dv_derivations] fn=%s target=%s args=%s",
        fn_name, target, args
    )

    try:
        df = fn(df, target, args or {})
    except Exception as e:
        raise RuntimeError(f"[dv_derivations] Derivation '{fn_name}' failed for target '{target}': {e}") from e

    logger.info(
        "[dv_derivations] Completed fn=%s → column=%s",
        fn_name, target
    )

    return df

# ---------------------------------------------------------------------
# INSTALL HOOK (REQUIRED FOR dv_bootstrap)
# ---------------------------------------------------------------------

from framework.rule_engine import RuleEngine

def install_derive_rule():
    """
    Inject derive rule into RuleEngine
    """

    def _rule_derive(df, params):
        fn = params.get("fn")
        target = params.get("target")
        args = params.get("args", {})

        if not fn or not target:
            logger.warning("[derive] missing fn/target - skipping: %s", params)
            return df

        return apply_derivation(df, fn, target, args)

    if not hasattr(RuleEngine, "_rule_derive"):
        setattr(RuleEngine, "_rule_derive", staticmethod(_rule_derive))

    logger.info("[dv_derive_rule] Installed rule: derive")

    return ["derive"]
