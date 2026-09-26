# =====================================================================
# framework/dv_derivations.py   (Silver -> Gold/ADV, derived-column library)
#
# WHY THIS EXISTS (new requirement):
#   The team does NOT want CASE / derived SQL stored inside the control
#   tables (ctl_column_map.source_expr). Instead, a column is FLAGGED as
#   'derived' and the framework CALLS a named PySpark function here,
#   passing the source columns. The business logic lives in code (reusable,
#   testable), the control table only references it by name.
#
# Every function has the SAME signature so it can be called generically:
#       fn(df, target, args) -> DataFrame   (adds/overwrites `target`)
#   `args` is a plain dict (from ctl_column_map.transform_args JSON), so
#   NO column names are hardcoded - they come from the control row.
#
# Add a new derivation = add a function here + reference it from the
# control table. No pipeline code changes.
# =====================================================================
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)
logging.getLogger("py4j").setLevel(logging.ERROR)


def _col_or_null(df, name):
    return F.col(name) if (name and name in df.columns) else F.lit(None).cast("string")


def concat_name(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    Concatenate name parts with single spaces, skipping NULLs.
    args: {"cols": ["MNS_FIRST_NAME","MNS_MIDDLE_NAME","MNS_SURNAME"],
           "separator": " "}   (separator optional, default single space)
    """
    cols = args.get("cols", [])
    sep = args.get("separator", " ")
    parts = [F.coalesce(_col_or_null(df, c).cast("string"), F.lit("")) for c in cols]
    expr = F.trim(F.regexp_replace(F.concat_ws(sep, *parts), r"\s+", " "))
    return df.withColumn(target, F.when(expr == "", F.lit(None)).otherwise(expr))


def trim_col(df: DataFrame, target: str, args: dict) -> DataFrame:
    """args: {"col": "<source col>"}"""
    return df.withColumn(target, F.trim(_col_or_null(df, args.get("col")).cast("string")))


def derive_full_name(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    Reusable WHO full-name derivation (Insured / Underwriter / Broker-Licensed).
    args (all column names, nothing hardcoded):
      first, middle, surname, underwriter_name, contact_name
    Order of precedence mirrors the agreed mapping.
    """
    first = _col_or_null(df, args.get("first"))
    middle = _col_or_null(df, args.get("middle"))
    surname = _col_or_null(df, args.get("surname"))
    uw = _col_or_null(df, args.get("underwriter_name"))
    contact = _col_or_null(df, args.get("contact_name"))
    person = F.trim(F.regexp_replace(
        F.concat_ws(" ",
                    F.coalesce(first, F.lit("")),
                    F.coalesce(middle, F.lit("")),
                    F.coalesce(surname, F.lit(""))), r"\s+", " "))
    val = (F.when(first.isNotNull() | surname.isNotNull(), person)
            .when(uw.isNotNull(), uw)
            .when(contact.isNotNull(), contact)
            .otherwise(F.lit(None)))
    return df.withColumn(target, F.when(val == "", F.lit(None)).otherwise(val))


def derive_party_id(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    Business identifier for the party (Org name | full person name |
    underwriter name+code | contact name). Used for HUB_PRTY business key.
    args: business_name, first, middle, surname,
          underwriter_name, underwriter_code, contact_name

    Separator convention (matches reference SQL):
      Business   : CONCAT(MNS_BUSINESS_NAME, '_')
      Person     : CONCAT(first, '_', middle, '_', surname, '_')
      Underwriter: CONCAT(uw_name, '_', uw_code)
      Contact    : MCN_CONTACT_NAME  (no trailing underscore)
    """
    bn = _col_or_null(df, args.get("business_name"))
    first = _col_or_null(df, args.get("first"))
    middle = _col_or_null(df, args.get("middle"))
    surname = _col_or_null(df, args.get("surname"))
    uw = _col_or_null(df, args.get("underwriter_name"))
    uwc = _col_or_null(df, args.get("underwriter_code"))
    contact = _col_or_null(df, args.get("contact_name"))
    # Person: first_middle_surname_  (underscore-separated, trailing underscore)
    person = F.concat(
        F.coalesce(first,   F.lit("")), F.lit("_"),
        F.coalesce(middle,  F.lit("")), F.lit("_"),
        F.coalesce(surname, F.lit("")), F.lit("_"),
    )
    # Underwriter: name_code
    uw_full = F.concat(F.coalesce(uw, F.lit("")), F.lit("_"), F.coalesce(uwc, F.lit("")))
    val = (F.when(bn.isNotNull(), F.concat(bn, F.lit("_")))
            .when(first.isNotNull() | surname.isNotNull(), person)
            .when(uw.isNotNull(), uw_full)
            .when(contact.isNotNull(), contact)
            .otherwise(F.lit(None)))
    return df.withColumn(target, F.when(val == "", F.lit(None)).otherwise(val))


def derive_party_type(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    Organization vs Person. args: business_name, first, surname,
    underwriter_name, contact_name. Default 'Person'.
    """
    bn = _col_or_null(df, args.get("business_name"))
    first = _col_or_null(df, args.get("first"))
    surname = _col_or_null(df, args.get("surname"))
    uw = _col_or_null(df, args.get("underwriter_name"))
    contact = _col_or_null(df, args.get("contact_name"))
    val = (F.when(bn.isNotNull(), F.lit("Organization"))
            .when(first.isNotNull() | surname.isNotNull(), F.lit("Person"))
            .when(uw.isNotNull(), F.lit("Person"))
            .when(contact.isNotNull(), F.lit("Person"))
            .otherwise(F.lit(None)))
    return df.withColumn(target, val)


def derive_concat(df: DataFrame, target: str, args: dict) -> DataFrame:
    """
    Generic concatenation of arbitrary source columns into a single key/ref.
    Mirrors the STTM "concat(colA, colB, ...)" transformation used to build
    REF_ID / CVRG_REF_ID / business-key style columns.

    args: {"cols": ["MQP_ENTITY_REFERENCE","MCCF_ID", ...],
           "separator": ""}      separator optional, default empty string

    NULLs are coalesced to '' so the concat never returns NULL when at least
    one part is present. If every part is NULL the result is '' (callers that
    need NULL-on-all-null should follow with a filter_not_null rule).
    """
    cols = args.get("cols", [])
    sep = args.get("separator", "")
    if not cols:
        logger.warning("[derive_concat] no cols for target %s; skipping", target)
        return df
    parts = [F.coalesce(_col_or_null(df, c).cast("string"), F.lit("")) for c in cols]
    if sep:
        expr = F.concat_ws(sep, *parts)
    else:
        expr = F.concat(*parts)
    return df.withColumn(target, expr)


# Registry: control-table transform_fn name -> function
DERIVATIONS = {
    "concat_name":       concat_name,
    "trim_col":          trim_col,
    "derive_full_name":  derive_full_name,
    "derive_party_id":   derive_party_id,
    "derive_party_type": derive_party_type,
    "derive_concat":     derive_concat,
}


def apply_derivation(df: DataFrame, fn_name: str, target: str, args: dict) -> DataFrame:
    fn = DERIVATIONS.get(fn_name)
    if fn is None:
        raise ValueError(f"[dv_derivations] Unknown transform_fn '{fn_name}'. "
                         f"Available: {sorted(DERIVATIONS)}")
    logger.info("[dv_derivations] Calling fn=%-25s target=%-20s args=%s", fn_name, target, args)
    try:
        df = fn(df, target, args or {})
    except Exception as e:
        raise RuntimeError(f"[dv_derivations] Derivation '{fn_name}' failed for target '{target}': {e}") from e
    logger.info("[dv_derivations] fn=%-25s target=%-20s -> column added/updated. columns=%s",
                fn_name, target, df.columns)
    return df
