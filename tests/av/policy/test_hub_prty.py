# Databricks notebook source
# MAGIC %md
# MAGIC # Test: HUB_PRTY (AV)
# MAGIC
# MAGIC Reconciles **HUB_PRTY** (loaded by the framework) against the **expected** records produced by the
# MAGIC reference query `hub_prty.sql`.
# MAGIC
# MAGIC Only the columns in the SQL query output are validated; other target columns (`PART_COL`, `BTCH_ID`, ...) are ignored.
# MAGIC Written for **serverless** compute: no `cache` / `persist` / `unpersist`.
# MAGIC
# MAGIC **Expected query = `hub_prty.sql` with these adjustments**
# MAGIC 1. `S.DATE_DELETED IS NULL` is added. Every silver table carries `DATE_DELETED` and the framework drops
# MAGIC    soft-deleted rows before any join.
# MAGIC
# MAGIC **Note**: if other AV pipelines also load HUB_PRTY with `REC_SRC_NM = '109'`, their rows show up as
# MAGIC unexpected rows in TC02 / TC06 / TC07b / TC08b.
# MAGIC
# MAGIC **Target scope**: `REC_SRC_NM = '109'` and, when the hub has `PART_COL`, `PART_COL = 'av'`
# MAGIC (the hub merge key includes the partition columns, so each product keeps its own rows).
# MAGIC
# MAGIC **Run it after a FULL load of the pipeline**, since incremental runs only pick up new source rows.
# MAGIC
# MAGIC | ID | Test case |
# MAGIC |---|---|
# MAGIC | TC00 | Silver source carries `DATE_DELETED` (soft-delete rows excluded from expected) |
# MAGIC | TC01 | Every SQL output column exists in the target |
# MAGIC | TC02 | Record count match |
# MAGIC | TC03 | No duplicates on (`PRTY_ID`, `PRTY_TP_CD`) in the target scope |
# MAGIC | TC04 | Mandatory columns are NOT NULL in the target |
# MAGIC | TC05 | Constant values (`REC_SRC_NM`, `ERR_FLG`, `ERR_CD`) and spec-NULL columns |
# MAGIC | TC06 | Hash keys: the `QBE_HASH_PRTY_ID` values produced by the SQL equal those in the target: no more, no less (count and value). The only hash check. |
# MAGIC | TC07 | Full-row reconciliation (EXCEPT ALL, both directions) |
# MAGIC | TC08 | Key presence and column-level value match (joined on `PRTY_ID`, `PRTY_TP_CD`) |
# MAGIC | TC09 | No soft-deleted source row reaches the target |

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "dsi_dev", "Catalog")
dbutils.widgets.text("silver_schema", "silver_oct16", "Silver schema")
dbutils.widgets.text("gold_schema", "adv_db_majescoic", "Gold schema")
dbutils.widgets.text("target_table", "HUB_PRTY", "Target (hub) table")
dbutils.widgets.text("source_table", "party", "Silver source table")
dbutils.widgets.text("product_code", "AV", "Product code")
dbutils.widgets.text("sample_rows", "20", "Sample rows to show on failure")
dbutils.widgets.dropdown("fail_on_error", "Y", ["Y", "N"], "Fail notebook if any test fails")

CATALOG       = dbutils.widgets.get("catalog").strip()
SILVER        = f"{CATALOG}.{dbutils.widgets.get('silver_schema').strip()}"
GOLD          = f"{CATALOG}.{dbutils.widgets.get('gold_schema').strip()}"
TARGET_FQ     = f"{GOLD}.{dbutils.widgets.get('target_table').strip()}"
SOURCE_FQ     = f"{SILVER}.{dbutils.widgets.get('source_table').strip()}"
REC_SRC_NM    = "109"   # always 109 in this scope (no widget)
PRODUCT_CODE  = dbutils.widgets.get("product_code").strip()
SAMPLE_ROWS   = int(dbutils.widgets.get("sample_rows") or 20)
FAIL_ON_ERROR = dbutils.widgets.get("fail_on_error") == "Y"

HASH_KEY = "QBE_HASH_PRTY_ID"
ID_COL   = "PRTY_ID"

print(f"Source : {SOURCE_FQ}\nTarget : {TARGET_FQ}")
print(f"REC_SRC_NM={REC_SRC_NM}  product={PRODUCT_CODE}")

# COMMAND ----------

# MAGIC %md ## Column definitions

# COMMAND ----------

# Columns of the SQL query output, in SELECT order.
SQL_OUTPUT_COLS = ["QBE_HASH_PRTY_ID", "LD_DT", "REC_SRC_NM", "PRTY_ID", "PRTY_TP_CD", "ERR_FLG", "ERR_CD"]

# Columns the SQL sets to NULL: they must be NULL in the target too.
NULL_COLS = []

# LD_DT is CURRENT_TIMESTAMP at run time, so only NOT NULL is checked, never its value.
RUNTIME_COLS = ["LD_DT"]

# Columns validated row by row (TC07 / TC08). The hash column is checked only in TC06.
VALUE_COLS = [c for c in SQL_OUTPUT_COLS if c not in RUNTIME_COLS + [HASH_KEY]]

# Must never be NULL in the target.
MANDATORY_COLS = [c for c in SQL_OUTPUT_COLS if c not in NULL_COLS + [HASH_KEY]]

# COMMAND ----------

# MAGIC %md ## Helpers

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

RESULTS = []

# VarcharType/CharType only exist in newer PySpark versions.
_STRING_TYPES = tuple(t for t in (T.StringType, getattr(T, "VarcharType", None), getattr(T, "CharType", None)) if t)
_CHAR_TYPES   = tuple(t for t in (getattr(T, "VarcharType", None), getattr(T, "CharType", None)) if t)

def record(tc_id, name, passed, expected=None, actual=None, details=""):
    status = "PASS" if passed else "FAIL"
    RESULTS.append((tc_id, name, status, None if expected is None else str(expected),
                    None if actual is None else str(actual), str(details)[:1000]))
    print(f"[{status}] {tc_id} - {name} | expected={expected} actual={actual} {details}")

def show_sample(df, title):
    print(f"--- {title} (first {SAMPLE_ROWS}) ---")
    display(df.limit(SAMPLE_ROWS))

def col_type(df, name):
    for f in df.schema.fields:
        if f.name.upper() == name.upper():
            return f.dataType
    return None

def cast_type(dtype):
    # char/varchar cannot be used as a cast target; compare them as string.
    return T.StringType() if _CHAR_TYPES and isinstance(dtype, _CHAR_TYPES) else dtype

def norm_expr(col_name, dtype):
    """Type-aware normalisation so both sides are compared on the same textual form."""
    c = F.col(col_name)
    if isinstance(dtype, _STRING_TYPES):
        # Same clean-up the framework applies to string columns on write.
        return F.trim(F.regexp_replace(c.cast("string"), r"[\r\n\t]", ""))
    if isinstance(dtype, (T.DecimalType, T.IntegerType, T.LongType, T.ShortType,
                          T.ByteType, T.DoubleType, T.FloatType)):
        return c.cast("decimal(38,10)").cast("string")
    if isinstance(dtype, T.TimestampType) or type(dtype).__name__ == "TimestampNTZType":
        return F.date_format(c.cast("timestamp"), "yyyy-MM-dd HH:mm:ss.SSSSSS")
    if isinstance(dtype, T.DateType):
        return c.cast("date").cast("string")
    return c.cast("string")

def upper_cols(df):
    return df.toDF(*[c.upper() for c in df.columns])

# COMMAND ----------

# MAGIC %md ## Expected data (reference SQL)

# COMMAND ----------

src_df = spark.table(SOURCE_FQ)

# ---- TC00: soft-delete column --------------------------------------------------------------
# Every silver table carries DATE_DELETED; the framework drops rows where it is set before any join.
DATE_DELETED_COL = next((c for c in src_df.columns if c.upper() == "DATE_DELETED"), None)
record("TC00", "Silver source has DATE_DELETED column", DATE_DELETED_COL is not None,
       expected="DATE_DELETED", actual=DATE_DELETED_COL)
if DATE_DELETED_COL is None:
    raise AssertionError(f"{SOURCE_FQ} has no DATE_DELETED column; the expected SQL cannot mirror the framework.")

# Business key, hash and party type exactly as in hub_prty.sql.
# Note: the hash also includes the party type, so it is not MD5(PRTY_ID || '_109').
KEY_EXPR  = "LOWER(TRIM(S.BUSINESSNAME))"
HASH_EXPR = f"MD5(CONCAT(LOWER(TRIM(S.BUSINESSNAME)), '_', LOWER(TRIM(S.PARTY_TYPE)), '_{REC_SRC_NM}'))"
EXTRA_EXPRS = {
    "PRTY_TP_CD": ("CASE WHEN UPPER(TRIM(S.PARTY_TYPE)) = 'PERSON' THEN 'Person' "
                   "WHEN UPPER(TRIM(S.PARTY_TYPE)) = 'ORGANIZATION' THEN 'Organization' "
                   "ELSE S.PARTY_TYPE END"),
}

# Business key of a hub row: one business name can exist with several party types.
# Used for the TC03 duplicate check and the TC08 row matching (the hash is only used in TC06).
BUSINESS_KEY = [ID_COL, "PRTY_TP_CD"]
UNIQUE_KEYS  = [BUSINESS_KEY]


# Source rows in scope of the SQL, before the soft-delete filter.
SCOPE_WHERE = f"""S.BUSINESSNAME IS NOT NULL AND S.MQP_PRODUCT_CODE = '{PRODUCT_CODE}' AND TRIM(S.BUSINESSNAME) <> ''"""

# Expression per output column, as in the SQL.
EXPR_BY_COL = {
    HASH_KEY:     HASH_EXPR,
    "LD_DT":      "CURRENT_TIMESTAMP",
    "REC_SRC_NM": f"'{REC_SRC_NM}'",
    ID_COL:       KEY_EXPR,
    "ERR_FLG":    "0",
    "ERR_CD":     "'0'",
    **EXTRA_EXPRS,
    **{c: "NULL" for c in NULL_COLS},
}

EXPECTED_SQL = f"""
SELECT DISTINCT
       {(',' + chr(10) + '       ').join(f'{EXPR_BY_COL[c]} AS {c}' for c in SQL_OUTPUT_COLS)}
FROM {SOURCE_FQ} S
WHERE {SCOPE_WHERE}
  AND S.{DATE_DELETED_COL} IS NULL
"""

expected_raw = upper_cols(spark.sql(EXPECTED_SQL))
print(EXPECTED_SQL)

n_del = spark.sql(f"SELECT COUNT(*) AS N FROM {SOURCE_FQ} S WHERE {SCOPE_WHERE} AND S.{DATE_DELETED_COL} IS NOT NULL").first()["N"]
print(f"INFO: {n_del} source rows in scope have {DATE_DELETED_COL} set and are excluded from the expected data.")

# CONCAT returns NULL when PARTY_TYPE is NULL, so those rows get a NULL hash key in the SQL.
# The framework drops rows with a NULL primary key, so they can never be loaded.
n_null_type = expected_raw.filter(F.col(HASH_KEY).isNull()).count()
print(f"INFO: {n_null_type} expected rows have a NULL {HASH_KEY} (PARTY_TYPE is NULL); they cannot be loaded and will fail TC02 / TC06 / TC07a / TC08a.")
n_other_type = expected_raw.filter(~F.col("PRTY_TP_CD").isin("Person", "Organization")).count()
print(f"INFO: {n_other_type} expected rows have a PARTY_TYPE other than Person / Organization (passed through as-is).")

# COMMAND ----------

# MAGIC %md ## Actual data (framework output)

# COMMAND ----------

target_all = upper_cols(spark.table(TARGET_FQ))
actual_raw = target_all.filter(F.col("REC_SRC_NM") == REC_SRC_NM)
if "PART_COL" in target_all.columns:
    actual_raw = actual_raw.filter(F.lower(F.col("PART_COL")) == PRODUCT_CODE.lower())
else:
    print("WARNING: target has no PART_COL; the scope includes rows of every product.")

# COMMAND ----------

# MAGIC %md ## TC01 - SQL output columns exist in target

# COMMAND ----------

missing_cols = [c for c in SQL_OUTPUT_COLS if c not in target_all.columns]
record("TC01", "SQL output columns exist in target", not missing_cols,
       expected=len(SQL_OUTPUT_COLS), actual=len(SQL_OUTPUT_COLS) - len(missing_cols),
       details=f"missing={missing_cols}" if missing_cols else "")
if HASH_KEY in missing_cols or ID_COL in missing_cols:
    raise AssertionError(f"Target is missing {HASH_KEY} / {ID_COL}; nothing else can be validated.")

# Continue with the columns that exist, so one missing column does not block the other tests.
SQL_OUTPUT_COLS = [c for c in SQL_OUTPUT_COLS if c not in missing_cols]
VALUE_COLS      = [c for c in VALUE_COLS if c not in missing_cols]
MANDATORY_COLS  = [c for c in MANDATORY_COLS if c not in missing_cols]
NULL_COLS       = [c for c in NULL_COLS if c not in missing_cols]

# Project both sides to the SQL output columns only. Cast expected to target types (as the framework
# does on write), then normalise both sides the same way.
TARGET_TYPES = {c: col_type(target_all, c) for c in SQL_OUTPUT_COLS}

expected_typed = expected_raw.select(*[F.col(c).cast(cast_type(TARGET_TYPES[c])).alias(c) for c in SQL_OUTPUT_COLS])
actual_typed   = actual_raw.select(*SQL_OUTPUT_COLS)

expected_n = expected_typed.select(*[norm_expr(c, TARGET_TYPES[c]).alias(c) for c in SQL_OUTPUT_COLS])
actual_n   = actual_typed.select(*[norm_expr(c, TARGET_TYPES[c]).alias(c) for c in SQL_OUTPUT_COLS])

# COMMAND ----------

# MAGIC %md ## TC02 - Record count match

# COMMAND ----------

exp_cnt = expected_n.count()
act_cnt = actual_n.count()
record("TC02", "Record count match", exp_cnt == act_cnt, expected=exp_cnt, actual=act_cnt,
       details=f"diff={act_cnt - exp_cnt}")

# COMMAND ----------

# MAGIC %md ## TC03 - Duplicate check (a hub holds one row per key)

# COMMAND ----------

for key_cols in UNIQUE_KEYS:
    key_cols = [k for k in key_cols if k in SQL_OUTPUT_COLS]
    exp_dup = expected_n.groupBy(*key_cols).count().filter("count > 1")
    act_dup = actual_n.groupBy(*key_cols).count().filter("count > 1")
    n_exp_dup, n_act_dup = exp_dup.count(), act_dup.count()
    record("TC03", f"No duplicate {key_cols} in target", n_act_dup == 0, expected=0, actual=n_act_dup,
           details=f"expected data itself has {n_exp_dup} duplicate keys" if n_exp_dup else "")
    if n_act_dup:
        show_sample(act_dup.orderBy(F.desc("count")), f"Duplicate {key_cols} in target")

# COMMAND ----------

# MAGIC %md ## TC04 - Mandatory columns NOT NULL

# COMMAND ----------

null_counts_act = actual_n.select(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in SQL_OUTPUT_COLS]).first().asDict()
for c in MANDATORY_COLS:
    n = null_counts_act[c] or 0
    record("TC04", f"{c} is NOT NULL", n == 0, expected=0, actual=n)

# COMMAND ----------

# MAGIC %md ## TC05 - Constant values and spec-NULL columns

# COMMAND ----------

constant_checks = {
    "REC_SRC_NM": F.col("REC_SRC_NM") == F.lit(REC_SRC_NM),
    "ERR_FLG":    F.col("ERR_FLG").cast("decimal(38,10)") == F.lit(0),
    "ERR_CD":     F.col("ERR_CD") == F.lit("0"),
}
for c, cond in constant_checks.items():
    if c not in SQL_OUTPUT_COLS:
        continue
    bad = actual_n.filter(~F.coalesce(cond, F.lit(False)))
    n_bad = bad.count()
    record("TC05", f"{c} constant value", n_bad == 0, expected=0, actual=n_bad)
    if n_bad:
        show_sample(bad.groupBy(c).count(), f"Unexpected {c} values")

for c in NULL_COLS:
    n_not_null = actual_n.filter(F.col(c).isNotNull()).count()
    record("TC05", f"{c} is NULL (per SQL)", n_not_null == 0, expected=0, actual=n_not_null)
    if n_not_null:
        show_sample(actual_n.filter(F.col(c).isNotNull()).groupBy(c).count(), f"Non-NULL {c} values")

# COMMAND ----------

# MAGIC %md ## TC06 - Hash keys: SQL vs target (the only hash check)
# MAGIC Every `QBE_HASH_PRTY_ID` produced by the SQL must be in the target, and the target must hold no other hash:
# MAGIC no more, no less, compared as a multiset (count and value).

# COMMAND ----------

exp_h = expected_n.select(HASH_KEY)
act_h = actual_n.select(HASH_KEY)
n_exp_h, n_act_h = exp_h.count(), act_h.count()
hash_missing = exp_h.exceptAll(act_h)   # produced by the SQL, not in the target
hash_extra   = act_h.exceptAll(exp_h)   # in the target, not produced by the SQL
n_hash_missing, n_hash_extra = hash_missing.count(), hash_extra.count()
record("TC06", f"{HASH_KEY}: SQL hashes = target hashes (count and value)",
       n_exp_h == n_act_h and n_hash_missing == 0 and n_hash_extra == 0,
       expected=n_exp_h, actual=n_act_h,
       details=f"missing_in_target={n_hash_missing} extra_in_target={n_hash_extra}")
if n_hash_missing:
    show_sample(expected_n.join(hash_missing.distinct(), HASH_KEY, "inner"), f"{HASH_KEY} produced by the SQL but not in the target")
if n_hash_extra:
    show_sample(actual_n.join(hash_extra.distinct(), HASH_KEY, "inner"), f"{HASH_KEY} in the target but not produced by the SQL")

# COMMAND ----------

# MAGIC %md ## TC07 - Full-row reconciliation (EXCEPT ALL)

# COMMAND ----------

exp_v = expected_n.select(*VALUE_COLS)
act_v = actual_n.select(*VALUE_COLS)

missing_in_target = exp_v.exceptAll(act_v)   # expected rows not loaded / loaded differently
extra_in_target   = act_v.exceptAll(exp_v)   # loaded rows the spec does not produce

n_missing, n_extra = missing_in_target.count(), extra_in_target.count()
record("TC07a", "Expected rows present in target (expected EXCEPT ALL actual)", n_missing == 0, expected=0, actual=n_missing)
record("TC07b", "No unexpected rows in target (actual EXCEPT ALL expected)", n_extra == 0, expected=0, actual=n_extra)
if n_missing:
    show_sample(missing_in_target.orderBy(ID_COL), "Expected but not in target")
if n_extra:
    show_sample(extra_in_target.orderBy(ID_COL), "In target but not expected")

# COMMAND ----------

# MAGIC %md ## TC08 - Key presence and column-level value match (on `PRTY_ID`, `PRTY_TP_CD`)

# COMMAND ----------

exp_keys = expected_n.select(*BUSINESS_KEY).distinct()
act_keys = actual_n.select(*BUSINESS_KEY).distinct()

only_exp = exp_keys.join(act_keys, BUSINESS_KEY, "left_anti")
only_act = act_keys.join(exp_keys, BUSINESS_KEY, "left_anti")
n_only_exp, n_only_act = only_exp.count(), only_act.count()
record("TC08a", f"Every expected {BUSINESS_KEY} found in target", n_only_exp == 0, expected=0, actual=n_only_exp)
record("TC08b", f"Every target {BUSINESS_KEY} found in expected", n_only_act == 0, expected=0, actual=n_only_act)
if n_only_exp:
    show_sample(only_exp, "Keys only in expected")
if n_only_act:
    show_sample(only_act, "Keys only in target")

non_key_cols = [c for c in VALUE_COLS if c not in BUSINESS_KEY]
e = expected_n.select(*BUSINESS_KEY, *[F.col(c).alias(f"EXP__{c}") for c in non_key_cols])
a = actual_n.select(*BUSINESS_KEY, *[F.col(c).alias(f"ACT__{c}") for c in non_key_cols])
matched = e.join(a, BUSINESS_KEY, "inner")

mismatch_counts = matched.select(*[
    F.sum((~F.col(f"EXP__{c}").eqNullSafe(F.col(f"ACT__{c}"))).cast("int")).alias(c) for c in non_key_cols
]).first().asDict() if non_key_cols else {}

for c in non_key_cols:
    n = mismatch_counts[c] or 0
    record("TC08", f"Value match - {c}", n == 0, expected=0, actual=n)
    if n:
        show_sample(matched.filter(~F.col(f"EXP__{c}").eqNullSafe(F.col(f"ACT__{c}")))
                           .select(*BUSINESS_KEY, f"EXP__{c}", f"ACT__{c}"),
                    f"Mismatches in {c}")

# COMMAND ----------

# MAGIC %md ## TC09 - No soft-deleted source row reaches the target
# MAGIC A target key that only exists in soft-deleted source rows means the framework did not apply the `DATE_DELETED`
# MAGIC filter (hubs are insert-only, so a key loaded before its source row was soft-deleted also shows up here).

# COMMAND ----------

def _source_keys(deleted):
    cond = "IS NOT NULL" if deleted else "IS NULL"
    return upper_cols(spark.sql(
        f"SELECT DISTINCT {KEY_EXPR} AS {ID_COL} FROM {SOURCE_FQ} S WHERE {SCOPE_WHERE} AND S.{DATE_DELETED_COL} {cond}"
    )).select(norm_expr(ID_COL, T.StringType()).alias(ID_COL))

deleted_only = _source_keys(True).join(_source_keys(False), ID_COL, "left_anti")
leaked = actual_n.select(ID_COL).distinct().join(deleted_only, ID_COL, "inner")
n_leaked = leaked.count()
record("TC09", "No soft-deleted source rows in target", n_leaked == 0, expected=0, actual=n_leaked)
if n_leaked:
    show_sample(leaked, "Target keys that only exist as soft-deleted source rows")

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

summary_df = spark.createDataFrame(
    RESULTS,
    "test_id STRING, test_name STRING, status STRING, expected STRING, actual STRING, details STRING",
)
display(summary_df.orderBy(F.col("status").desc(), "test_id"))

n_fail = sum(1 for r in RESULTS if r[2] == "FAIL")
print(f"\nTOTAL={len(RESULTS)}  PASS={len(RESULTS) - n_fail}  FAIL={n_fail}")

if FAIL_ON_ERROR and n_fail:
    raise AssertionError(f"{n_fail} test(s) failed for {TARGET_FQ}. See the summary above.")
