# =====================================================================
# framework/dq_runner.py  (PRODUCTION v2.0)
#
# FIX NB-5: Eliminates the 3 × DataFrame.count() anti-pattern.
# All three DQ metrics (row count, null check, duplicate PK check)
# are now computed in a SINGLE Spark aggregation per pipeline:
#   - total rows        via F.count("*")
#   - null counts       via F.sum(F.when(col.isNull(), 1).otherwise(0))
#   - duplicate PKs     via countDistinct vs count (detected in one pass)
#
# Results are written to control_db.dq_results.
# Any CRITICAL failure raises an exception AFTER writing all results.
# =====================================================================
import logging
import uuid
from datetime import datetime
from typing import List

from pyspark.sql import functions as F

logger = logging.getLogger(__name__)
logging.getLogger("py4j").setLevel(logging.ERROR)


def run_dq(
    spark,
    settings,
    pipelines: List[dict],
    batch_id: str,
    run_id: str = None,
) -> None:
    """
    Run DQ checks for all loaded pipelines in one pass per pipeline.

    Checks:
      DQ1_ROWCOUNT     — Silver table has at least 1 row (CRITICAL)
      DQ2_NULL_<key>   — primary key columns are not NULL (CRITICAL)
      DQ3_DUPKEY       — primary key combination is unique (CRITICAL)

    Args:
        spark:     Active SparkSession.
        settings:  Settings instance (for control_fq).
        pipelines: List of pipeline config dicts.
        batch_id:  Shared batch identifier for this run.
        run_id:    Optional override; auto-generated UUID if not supplied.

    Raises:
        Exception: if any CRITICAL check fails (after writing all results).
    """
    run_id   = run_id or str(uuid.uuid4())
    dq_table = f"{settings.control_fq}.dq_results"
    all_rows = []
    failures = []

    for cfg in pipelines:
        tgt  = cfg["target_table"]
        pid  = cfg.get("pipeline_id", cfg.get("name"))
        keys = cfg.get("primary_keys") or []
        now  = datetime.now()

        try:
            df = spark.table(tgt)
        except Exception as e:
            logger.error("[DQ] Could not read table %s: %s", tgt, e)
            all_rows.append((run_id, batch_id, pid, tgt, "DQ0_TABLE_MISSING",
                             f"table {tgt} readable", "CRITICAL", 0, 1, False, now))
            failures.append(f"{tgt}.DQ0_TABLE_MISSING")
            continue

        # ----------------------------------------------------------------
        # Single aggregation pass — computes ALL metrics at once (NB-5 fix)
        # ----------------------------------------------------------------
        agg_exprs = [F.count("*").alias("__total")]

        # Null checks per key column
        for k in keys:
            agg_exprs.append(
                F.sum(F.when(F.col(k).isNull(), 1).otherwise(0)).alias(f"__null_{k}")
            )

        # Duplicate PK check: count vs count-distinct of PK tuple
        if keys:
            pk_struct = F.struct(*[F.col(k) for k in keys])
            agg_exprs.append(F.count("*").alias("__pk_count"))
            agg_exprs.append(F.countDistinct(pk_struct).alias("__pk_distinct"))

        agg_result = df.agg(*agg_exprs).collect()[0]
        total      = agg_result["__total"] or 0

        # DQ1: Row count
        passed_dq1 = total >= 1
        all_rows.append((run_id, batch_id, pid, tgt, "DQ1_ROWCOUNT",
                         "rows > 0", "CRITICAL", total, 1, passed_dq1, now))
        if not passed_dq1:
            failures.append(f"{tgt}.DQ1_ROWCOUNT")

        # DQ2: Null checks
        for k in keys:
            null_count = agg_result[f"__null_{k}"] or 0
            passed_dq2 = null_count == 0
            all_rows.append((run_id, batch_id, pid, tgt, f"DQ2_NULL_{k}",
                             f"{k} IS NOT NULL", "CRITICAL", null_count, 0, passed_dq2, now))
            if not passed_dq2:
                failures.append(f"{tgt}.DQ2_NULL_{k}")

        # DQ3: Duplicate PK
        if keys:
            pk_count    = agg_result["__pk_count"]    or 0
            pk_distinct = agg_result["__pk_distinct"] or 0
            dup_count   = pk_count - pk_distinct       # > 0 means duplicates
            passed_dq3  = dup_count == 0
            all_rows.append((run_id, batch_id, pid, tgt, "DQ3_DUPKEY",
                             "primary key unique", "CRITICAL", dup_count, 0, passed_dq3, now))
            if not passed_dq3:
                failures.append(f"{tgt}.DQ3_DUPKEY")

    # ----------------------------------------------------------------
    # Write all results in one append (minimise small-file writes)
    # ----------------------------------------------------------------
    cols = [
        "run_id", "batch_id", "pipeline_id", "target_table",
        "rule_id", "rule_desc", "severity",
        "metric_value", "threshold_value", "passed", "checked_ts",
    ]
    (
        spark.createDataFrame(all_rows, cols)
        .write
        .mode("append")
        .saveAsTable(dq_table)
    )
    logger.info("[DQ] Wrote %d result row(s) to %s", len(all_rows), dq_table)

    if failures:
        raise Exception(
            f"DQ GATE FAILED ({len(failures)} check(s)): " + "; ".join(failures)
        )

    logger.info("[DQ] All checks PASSED for batch %s", batch_id)
