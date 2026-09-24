# Databricks notebook source
# MAGIC %md
# MAGIC # Test: AV__GOLD__SAT_QOT_DED
# MAGIC
# MAGIC Reconciles **SAT_QOT_DED** (loaded by the framework from `seed_sat_qot_ded.sql`) against the
# MAGIC **expected** records produced by the reference query `sat_qot_ded.sql`.
# MAGIC
# MAGIC Only the columns in the SQL query output are validated; other target columns
# MAGIC (`PART_COL`, `BTCH_ID`, `BTCH_DT`, ...) are ignored.
# MAGIC
# MAGIC **Expected query = `sat_qot_ded.sql` with these adjustments**
# MAGIC 1. `QOT_ID` uses the required key format `<MQP_ENTITY_REFERENCE>_<yyyy-MM-ddTHH:mm:ss>`, built exactly as
# MAGIC    `derive_concat` (normalize = false) builds it. The plain `CAST ... AS STRING` and `LOWER` in the SQL are not used.
# MAGIC 2. The `LEFT JOIN SAT_QOT_DED ... WHERE SQD.QBE_HASH_QOT_ID IS NULL` anti-join is removed.
# MAGIC    After the load the target is populated, so the anti-join would return no rows.
# MAGIC
# MAGIC **Run it after a FULL load of the pipeline**, since incremental runs only pick up new source rows.
# MAGIC
# MAGIC | ID | Test case |
# MAGIC |---|---|
# MAGIC | TC01 | Every SQL output column exists in the target |
# MAGIC | TC02 | Record count match |
# MAGIC | TC03 | Duplicate check on the compare key |
# MAGIC | TC04 | Mandatory columns are NOT NULL in the target |
# MAGIC | TC05 | NULL-count parity per column (covers spec-NULL columns such as `DED_CD`) |
# MAGIC | TC06 | Constant values (`REC_SRC_NM`, `ERR_FLG`, `ERR_CD`) |
# MAGIC | TC07 | Every `QBE_HASH_QOT_ID` exists in `HUB_QOT` |
# MAGIC | TC08 | Hub `QOT_ID` follows `<ref>_yyyy-MM-ddTHH:mm:ss` |
# MAGIC | TC09 | Full-row reconciliation (EXCEPT ALL, both directions) |
# MAGIC | TC10 | Column-level value match (joined on compare key) |
# MAGIC | TC11 | SCD2 columns `SRC_EXPRN_DT` / `LD_END_DT` (checked per hash key, tie-safe) |

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "dsi_dev", "Catalog")
dbutils.widgets.text("silver_schema", "silver_oct16", "Silver schema")
dbutils.widgets.text("gold_schema", "adv_db_majescoic", "Gold schema")
dbutils.widgets.text("target_table", "SAT_QOT_DED", "Target table")
dbutils.widgets.text("hub_table", "HUB_QOT", "Hub table")
dbutils.widgets.text("source_table", "QOT_DED", "Silver source table")
dbutils.widgets.text("rec_src_nm", "109", "REC_SRC_NM")
dbutils.widgets.text("product_code", "AV", "Product code")
dbutils.widgets.text("compare_key", "QBE_HASH_QOT_ID,DED_TP_CD", "Compare key (comma separated)")
dbutils.widgets.dropdown("exclude_date_deleted", "N", ["Y", "N"], "Exclude DATE_DELETED rows (framework behaviour)")
dbutils.widgets.text("sample_rows", "20", "Sample rows to show on failure")
dbutils.widgets.dropdown("fail_on_error", "Y", ["Y", "N"], "Fail notebook if any test fails")

CATALOG        = dbutils.widgets.get("catalog").strip()
SILVER         = f"{CATALOG}.{dbutils.widgets.get('silver_schema').strip()}"
GOLD           = f"{CATALOG}.{dbutils.widgets.get('gold_schema').strip()}"
TARGET_FQ      = f"{GOLD}.{dbutils.widgets.get('target_table').strip()}"
HUB_FQ         = f"{GOLD}.{dbutils.widgets.get('hub_table').strip()}"
SOURCE_FQ      = f"{SILVER}.{dbutils.widgets.get('source_table').strip()}"
REC_SRC_NM     = dbutils.widgets.get("rec_src_nm").strip()
PRODUCT_CODE   = dbutils.widgets.get("product_code").strip()
COMPARE_KEY    = [c.strip().upper() for c in dbutils.widgets.get("compare_key").split(",") if c.strip()]
EXCL_DELETED   = dbutils.widgets.get("exclude_date_deleted") == "Y"
SAMPLE_ROWS    = int(dbutils.widgets.get("sample_rows") or 20)
FAIL_ON_ERROR  = dbutils.widgets.get("fail_on_error") == "Y"

print(f"Source : {SOURCE_FQ}\nHub    : {HUB_FQ}\nTarget : {TARGET_FQ}")
print(f"REC_SRC_NM={REC_SRC_NM}  product={PRODUCT_CODE}  compare_key={COMPARE_KEY}")

# COMMAND ----------

# MAGIC %md ## Column definitions

# COMMAND ----------

# Columns of the SQL query output, in SELECT order.
SQL_OUTPUT_COLS = [
    "QBE_HASH_QOT_ID", "LD_DT", "DED_TP_CD", "REF_ID", "LD_END_DT", "REC_SRC_NM",
    "DED_BSIS_CD", "DED_APLY_TO_CD", "DED_AMT", "DED_DATA_TP_CD", "DED_CD",
    "SRC_EFF_DT", "SRC_EXPRN_DT", "ERR_FLG", "ERR_CD",
]

# LD_DT is CURRENT_TIMESTAMP at run time, so only NOT NULL is checked, never its value.
RUNTIME_COLS = ["LD_DT"]

# Order-dependent SCD2 columns, checked per hash key in TC11 (ties on SRC_EFF_DT make row-level pairing non-deterministic).
SCD2_COLS = ["LD_END_DT", "SRC_EXPRN_DT"]

# Business columns validated row by row (TC09 / TC10).
VALUE_COLS = [c for c in SQL_OUTPUT_COLS if c not in RUNTIME_COLS + SCD2_COLS]

# Must never be NULL in the target.
MANDATORY_COLS = ["QBE_HASH_QOT_ID", "LD_DT", "REF_ID", "REC_SRC_NM", "SRC_EFF_DT", "ERR_FLG", "ERR_CD"]

# COMMAND ----------

# MAGIC %md ## Helpers

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

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

# QOT_ID exactly as derive_concat builds it: trimmed tokens, blank -> NULL, concat_ws('_'),
# TimestampType tokens formatted yyyy-MM-ddTHH:mm:ss, normalize=false (case preserved, 'T' stays uppercase).
def derive_concat_token(col_name):
    dtype = col_type(src_df, col_name)
    if isinstance(dtype, T.TimestampType):
        txt = (f"CONCAT(DATE_FORMAT(CAST(QD.{col_name} AS TIMESTAMP), 'yyyy-MM-dd'), 'T', "
               f"DATE_FORMAT(CAST(QD.{col_name} AS TIMESTAMP), 'HH:mm:ss'))")
    else:
        txt = f"CAST(QD.{col_name} AS STRING)"
    return f"NULLIF(TRIM({txt}), '')"

QOT_ID_EXPR = (f"CONCAT_WS('_', {derive_concat_token('MQP_ENTITY_REFERENCE')}, "
               f"{derive_concat_token('MQP_DATE_MODIFIED')})")

has_date_deleted = any(c.upper() == "DATE_DELETED" for c in src_df.columns)
date_deleted_filter = "AND QD.DATE_DELETED IS NULL" if (EXCL_DELETED and has_date_deleted) else ""

EXPECTED_SQL = f"""
WITH QOT_DED_SRC AS (
    SELECT QD.*,
           {QOT_ID_EXPR} AS QOT_ID
    FROM {SOURCE_FQ} QD
    WHERE QD.MQP_PRODUCT_CODE = '{PRODUCT_CODE}'
      AND QD.MQP_ENTITY_REFERENCE IS NOT NULL
      AND QD.MQP_DATE_MODIFIED IS NOT NULL
      {date_deleted_filter}
),
LINKED AS (
    SELECT HQ.QBE_HASH_QOT_ID,
           QD.*
    FROM QOT_DED_SRC QD
    INNER JOIN {HUB_FQ} HQ
        ON HQ.QOT_ID = QD.QOT_ID
       AND HQ.REC_SRC_NM = '{REC_SRC_NM}'
),
DEDUPED AS (
    SELECT L.*,
           MAX(MQP_DATE_MODIFIED) OVER (PARTITION BY QBE_HASH_QOT_ID) AS MAX_DATE_MODIFIED,
           ROW_NUMBER() OVER (PARTITION BY QBE_HASH_QOT_ID ORDER BY MQP_DATE_MODIFIED DESC) AS RN,
           LAG(MQP_DATE_MODIFIED) OVER (PARTITION BY QBE_HASH_QOT_ID ORDER BY MQP_DATE_MODIFIED DESC) AS PREV_MORE_RECENT_SRC_EFF_DT
    FROM LINKED L
),
FINAL AS (
    SELECT D.QBE_HASH_QOT_ID,
           CURRENT_TIMESTAMP AS LD_DT,
           D.MQP_ENTITY_REFERENCE AS REF_ID,
           CASE WHEN D.RN = 1 THEN NULL ELSE D.MAX_DATE_MODIFIED END AS LD_END_DT,
           '{REC_SRC_NM}' AS REC_SRC_NM,
           D.DED_TYPE AS DED_TP_CD,
           CAST(NULL AS STRING) AS DED_BSIS_CD,
           CAST(NULL AS STRING) AS DED_APLY_TO_CD,
           D.DED_AMT AS DED_AMT,
           CAST(NULL AS STRING) AS DED_DATA_TP_CD,
           CAST(NULL AS STRING) AS DED_CD,
           D.MQP_DATE_MODIFIED AS SRC_EFF_DT,
           CASE WHEN D.RN = 1 THEN NULL ELSE D.PREV_MORE_RECENT_SRC_EFF_DT END AS SRC_EXPRN_DT,
           0 AS ERR_FLG,
           '0' AS ERR_CD
    FROM DEDUPED D
)
SELECT {', '.join(SQL_OUTPUT_COLS)}
FROM FINAL
"""

expected_raw = upper_cols(spark.sql(EXPECTED_SQL))
print(EXPECTED_SQL)
if has_date_deleted and not EXCL_DELETED:
    n_del = src_df.filter(F.col("DATE_DELETED").isNotNull() & (F.col("MQP_PRODUCT_CODE") == PRODUCT_CODE)).count()
    print(f"NOTE: {n_del} source rows have DATE_DELETED set. The framework drops these but the SQL keeps them. "
          f"Set 'exclude_date_deleted' = Y to mirror the framework.")

# COMMAND ----------

# MAGIC %md ## Actual data (framework output)

# COMMAND ----------

target_all = upper_cols(spark.table(TARGET_FQ))
actual_raw = target_all.filter(F.col("REC_SRC_NM") == REC_SRC_NM)
if "PART_COL" in target_all.columns:
    actual_raw = actual_raw.filter(F.lower(F.col("PART_COL")) == PRODUCT_CODE.lower())

# COMMAND ----------

# MAGIC %md ## TC01 - SQL output columns exist in target

# COMMAND ----------

missing_cols = [c for c in SQL_OUTPUT_COLS if c not in target_all.columns]
record("TC01", "SQL output columns exist in target", not missing_cols,
       expected=len(SQL_OUTPUT_COLS), actual=len(SQL_OUTPUT_COLS) - len(missing_cols),
       details=f"missing={missing_cols}" if missing_cols else "")

# Continue with the columns that exist, so one missing column does not block the other tests.
SQL_OUTPUT_COLS = [c for c in SQL_OUTPUT_COLS if c not in missing_cols]
VALUE_COLS      = [c for c in VALUE_COLS if c not in missing_cols]
SCD2_COLS       = [c for c in SCD2_COLS if c not in missing_cols]
MANDATORY_COLS  = [c for c in MANDATORY_COLS if c not in missing_cols]
COMPARE_KEY     = [c for c in COMPARE_KEY if c not in missing_cols]

# Project both sides to the SQL output columns only. Cast expected to target types (as the framework
# does on write), then normalise both sides the same way.
TARGET_TYPES = {c: col_type(target_all, c) for c in SQL_OUTPUT_COLS}

def cast_type(dtype):
    # char/varchar cannot be used as a cast target; compare them as string.
    return T.StringType() if _CHAR_TYPES and isinstance(dtype, _CHAR_TYPES) else dtype

expected_typed = expected_raw.select(*[F.col(c).cast(cast_type(TARGET_TYPES[c])).alias(c) for c in SQL_OUTPUT_COLS])
actual_typed   = actual_raw.select(*SQL_OUTPUT_COLS)

expected_n = expected_typed.select(*[norm_expr(c, TARGET_TYPES[c]).alias(c) for c in SQL_OUTPUT_COLS]).cache()
actual_n   = actual_typed.select(*[norm_expr(c, TARGET_TYPES[c]).alias(c) for c in SQL_OUTPUT_COLS]).cache()

# COMMAND ----------

# MAGIC %md ## TC02 - Record count match

# COMMAND ----------

exp_cnt = expected_n.count()
act_cnt = actual_n.count()
record("TC02", "Record count match", exp_cnt == act_cnt, expected=exp_cnt, actual=act_cnt,
       details=f"diff={act_cnt - exp_cnt}")

exp_hash_cnt = expected_n.select("QBE_HASH_QOT_ID").distinct().count()
act_hash_cnt = actual_n.select("QBE_HASH_QOT_ID").distinct().count()
record("TC02b", "Distinct QBE_HASH_QOT_ID count match", exp_hash_cnt == act_hash_cnt,
       expected=exp_hash_cnt, actual=act_hash_cnt)

# COMMAND ----------

# MAGIC %md ## TC03 - Duplicate check on compare key

# COMMAND ----------

def dup_keys(df):
    return df.groupBy(*COMPARE_KEY).count().filter("count > 1")

exp_dups = dup_keys(expected_n)
act_dups = dup_keys(actual_n)
exp_dup_cnt, act_dup_cnt = exp_dups.count(), act_dups.count()

# Duplicates only fail when the target has more than the spec produces.
record("TC03", f"Duplicate keys on {COMPARE_KEY}", act_dup_cnt <= exp_dup_cnt,
       expected=exp_dup_cnt, actual=act_dup_cnt,
       details="source itself has duplicate keys; TC10 pairs them deterministically" if exp_dup_cnt else "")
if act_dup_cnt > exp_dup_cnt:
    show_sample(act_dups.orderBy(F.desc("count")), "Duplicate keys in target")

# Exact duplicate rows (all compared columns identical)
exact_dup_act = actual_n.select(*VALUE_COLS).groupBy(*VALUE_COLS).count().filter("count > 1").count()
exact_dup_exp = expected_n.select(*VALUE_COLS).groupBy(*VALUE_COLS).count().filter("count > 1").count()
record("TC03b", "Exact duplicate rows", exact_dup_act <= exact_dup_exp, expected=exact_dup_exp, actual=exact_dup_act)

# COMMAND ----------

# MAGIC %md ## TC04 - Mandatory columns NOT NULL

# COMMAND ----------

null_counts_act = actual_n.select(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in SQL_OUTPUT_COLS]).first().asDict()
for c in MANDATORY_COLS:
    n = null_counts_act[c] or 0
    record("TC04", f"{c} is NOT NULL", n == 0, expected=0, actual=n)

# COMMAND ----------

# MAGIC %md ## TC05 - NULL-count parity per column

# COMMAND ----------

null_counts_exp = expected_n.select(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in SQL_OUTPUT_COLS]).first().asDict()
for c in [c for c in SQL_OUTPUT_COLS if c not in RUNTIME_COLS]:
    e, a = null_counts_exp[c] or 0, null_counts_act[c] or 0
    record("TC05", f"NULL count parity - {c}", e == a, expected=e, actual=a)

# COMMAND ----------

# MAGIC %md ## TC06 - Constant values

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
    record("TC06", f"{c} constant value", n_bad == 0, expected=0, actual=n_bad)
    if n_bad:
        show_sample(bad.groupBy(c).count(), f"Unexpected {c} values")

# COMMAND ----------

# MAGIC %md ## TC07 - Hash key exists in HUB_QOT

# COMMAND ----------

hub_df = upper_cols(spark.table(HUB_FQ)).filter(F.col("REC_SRC_NM") == REC_SRC_NM)
hub_keys = hub_df.select(norm_expr("QBE_HASH_QOT_ID", col_type(hub_df, "QBE_HASH_QOT_ID")).alias("QBE_HASH_QOT_ID")).distinct()

orphans = actual_n.select("QBE_HASH_QOT_ID").distinct().join(hub_keys, "QBE_HASH_QOT_ID", "left_anti")
n_orphans = orphans.count()
record("TC07", "QBE_HASH_QOT_ID exists in HUB_QOT", n_orphans == 0, expected=0, actual=n_orphans)
if n_orphans:
    show_sample(orphans, "Target hash keys missing in HUB_QOT")

# COMMAND ----------

# MAGIC %md ## TC08 - QOT_ID key format `<ref>_yyyy-MM-ddTHH:mm:ss`

# COMMAND ----------

KEY_PATTERN = r"^.+_\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$"

linked_hub = (hub_df.select(norm_expr("QBE_HASH_QOT_ID", col_type(hub_df, "QBE_HASH_QOT_ID")).alias("QBE_HASH_QOT_ID"), "QOT_ID")
              .join(actual_n.select("QBE_HASH_QOT_ID").distinct(), "QBE_HASH_QOT_ID", "inner"))
bad_fmt = linked_hub.filter(~F.coalesce(F.col("QOT_ID").rlike(KEY_PATTERN), F.lit(False)))
n_bad_fmt = bad_fmt.count()
record("TC08", "Hub QOT_ID format <ref>_yyyy-MM-ddTHH:mm:ss", n_bad_fmt == 0, expected=0, actual=n_bad_fmt)
if n_bad_fmt:
    show_sample(bad_fmt, "QOT_ID values not in yyyy-MM-ddTHH:mm:ss format")

# COMMAND ----------

# MAGIC %md ## TC09 - Full-row reconciliation (EXCEPT ALL)

# COMMAND ----------

exp_v = expected_n.select(*VALUE_COLS)
act_v = actual_n.select(*VALUE_COLS)

missing_in_target = exp_v.exceptAll(act_v)   # expected rows not loaded / loaded differently
extra_in_target   = act_v.exceptAll(exp_v)   # loaded rows the spec does not produce

n_missing, n_extra = missing_in_target.count(), extra_in_target.count()
record("TC09a", "Expected rows present in target (expected EXCEPT ALL actual)", n_missing == 0, expected=0, actual=n_missing)
record("TC09b", "No unexpected rows in target (actual EXCEPT ALL expected)", n_extra == 0, expected=0, actual=n_extra)
if n_missing:
    show_sample(missing_in_target.orderBy(*COMPARE_KEY), "Expected but not in target")
if n_extra:
    show_sample(extra_in_target.orderBy(*COMPARE_KEY), "In target but not expected")

# COMMAND ----------

# MAGIC %md ## TC10 - Column-level value match
# MAGIC Rows are paired on the compare key. Duplicate keys are paired by a row number ordered on all compared columns.
# MAGIC Unpaired keys are reported separately, so each column's mismatch count only covers paired rows.

# COMMAND ----------

non_key_cols = [c for c in VALUE_COLS if c not in COMPARE_KEY]
pair_win = Window.partitionBy(*COMPARE_KEY).orderBy(*[F.col(c).asc_nulls_first() for c in non_key_cols])

exp_p = expected_n.select(*VALUE_COLS).withColumn("__RN", F.row_number().over(pair_win))
act_p = actual_n.select(*VALUE_COLS).withColumn("__RN", F.row_number().over(pair_win))
join_cols = COMPARE_KEY + ["__RN"]

e = exp_p.select(*join_cols, *[F.col(c).alias(f"EXP__{c}") for c in non_key_cols])
a = act_p.select(*join_cols, *[F.col(c).alias(f"ACT__{c}") for c in non_key_cols])

# Key presence (independent of column values)
presence = (exp_p.select(*join_cols).withColumn("__E", F.lit(1))
            .join(act_p.select(*join_cols).withColumn("__A", F.lit(1)), join_cols, "full_outer"))
n_key_only_exp = presence.filter(F.col("__A").isNull()).count()
n_key_only_act = presence.filter(F.col("__E").isNull()).count()
record("TC10a", f"Every expected key {COMPARE_KEY} found in target", n_key_only_exp == 0, expected=0, actual=n_key_only_exp)
record("TC10b", f"Every target key {COMPARE_KEY} found in expected", n_key_only_act == 0, expected=0, actual=n_key_only_act)
if n_key_only_exp:
    show_sample(presence.filter(F.col("__A").isNull()).drop("__E", "__A", "__RN"), "Keys only in expected")
if n_key_only_act:
    show_sample(presence.filter(F.col("__E").isNull()).drop("__E", "__A", "__RN"), "Keys only in target")

matched = e.join(a, join_cols, "inner").cache()
mismatch_counts = matched.select(*[
    F.sum((~F.col(f"EXP__{c}").eqNullSafe(F.col(f"ACT__{c}"))).cast("int")).alias(c) for c in non_key_cols
]).first().asDict()

for c in non_key_cols:
    n = mismatch_counts[c] or 0
    record("TC10", f"Value match - {c}", n == 0, expected=0, actual=n)
    if n:
        show_sample(
            matched.filter(~F.col(f"EXP__{c}").eqNullSafe(F.col(f"ACT__{c}")))
                   .select(*COMPARE_KEY, f"EXP__{c}", f"ACT__{c}"),
            f"Mismatches in {c}")

# COMMAND ----------

# MAGIC %md ## TC11 - SCD2 columns (`SRC_EXPRN_DT`, `LD_END_DT`)
# MAGIC Several rows can share one `QBE_HASH_QOT_ID` and `SRC_EFF_DT` (one per `DED_TP_CD`), so which row is "latest"
# MAGIC is not deterministic. These columns are compared per hash key: row count, number of open rows (NULL),
# MAGIC and the min/max of the non-NULL values.

# COMMAND ----------

def scd2_profile(df):
    aggs = [F.count(F.lit(1)).alias("ROWS")]
    for c in SCD2_COLS:
        aggs += [F.sum(F.col(c).isNull().cast("int")).alias(f"{c}__NULLS"),
                 F.min(c).alias(f"{c}__MIN"), F.max(c).alias(f"{c}__MAX")]
    return df.groupBy("QBE_HASH_QOT_ID").agg(*aggs)

if SCD2_COLS:
    exp_prof = scd2_profile(expected_n)
    act_prof = scd2_profile(actual_n)
    prof_cols = [c for c in exp_prof.columns if c != "QBE_HASH_QOT_ID"]
    prof = (exp_prof.select("QBE_HASH_QOT_ID", *[F.col(c).alias(f"EXP__{c}") for c in prof_cols])
            .join(act_prof.select("QBE_HASH_QOT_ID", *[F.col(c).alias(f"ACT__{c}") for c in prof_cols]),
                  "QBE_HASH_QOT_ID", "inner")).cache()

    for c in SCD2_COLS:
        # Open/closed pattern: same number of NULL (open) rows per hash key.
        n_open = prof.filter(~F.col(f"EXP__{c}__NULLS").eqNullSafe(F.col(f"ACT__{c}__NULLS"))).count()
        record("TC11", f"{c} open/closed row pattern per hash", n_open == 0, expected=0, actual=n_open)
        # Values: min/max of the closed rows per hash key.
        val_diff = prof.filter(~F.col(f"EXP__{c}__MIN").eqNullSafe(F.col(f"ACT__{c}__MIN")) |
                               ~F.col(f"EXP__{c}__MAX").eqNullSafe(F.col(f"ACT__{c}__MAX")))
        n_val = val_diff.count()
        record("TC11", f"{c} values per hash", n_val == 0, expected=0, actual=n_val)
        if n_open or n_val:
            show_sample(val_diff.select("QBE_HASH_QOT_ID", "EXP__ROWS", "ACT__ROWS",
                                        f"EXP__{c}__NULLS", f"ACT__{c}__NULLS",
                                        f"EXP__{c}__MIN", f"ACT__{c}__MIN",
                                        f"EXP__{c}__MAX", f"ACT__{c}__MAX"),
                        f"{c} differences per hash")

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

expected_n.unpersist(); actual_n.unpersist(); matched.unpersist()

if FAIL_ON_ERROR and n_fail:
    raise AssertionError(f"{n_fail} test(s) failed for {TARGET_FQ}. See the summary above.")
