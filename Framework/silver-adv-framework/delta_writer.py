# =====================================================================
# framework/delta_writer.py  (PRODUCTION v3.0 — SERVERLESS + FGAC SAFE)
#
# Behaviour is unchanged from v2.1 (SCD1 hashdiff-gated MERGE, SCD2 expire+
# insert, full/first overwrite). What changed is HOW row counts, existence
# checks and the watermark are obtained, so the code is valid on:
#   * Serverless compute  — NO df.persist()/.cache()/StorageLevel (unsupported).
#   * FGAC dedicated 15.4/16.4 — NO spark.catalog.tableExists() (L4) and NO
#                                DeltaTable.history() (L7). Only merge()/toDF()
#                                are used on DeltaTable, which FGAC allows.
#
# KEY IDEA (perf + serverless): the pipeline DataFrame is computed EXACTLY ONCE
# (by the write/merge). Row counts and the watermark are then read from Delta
# metadata (DESCRIBE HISTORY operationMetrics / a SQL MAX on the committed
# table) — never from a second df.count()/df.agg() that would recompute the
# whole join+derive DAG. This is what previously required .persist() to hide.
# =====================================================================
import logging
import random
import time
from pyspark.sql.functions import to_date
from datetime import datetime, timezone
from typing import Optional, List
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import CharType, VarcharType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

#logger = logging.getLogger(__name__)
logger = logging.getLogger(__name__)
logging.getLogger("py4j").setLevel(logging.ERROR)

_SCD2_HIGH_DATE = "9999-12-31T23:59:59.000+0000"
_DEFAULT_PHYSICAL_PARTITION_BY = ["REC_SRC_NM", "PART_COL"]

# Delta optimistic-concurrency commit conflicts — safe to retry (another writer
# committed to the same table between our read and our commit). Anything else
# (bad SQL, schema errors, permissions, etc.) is a genuine failure and is raised
# immediately, without retrying.
_MAX_CONCURRENCY_RETRIES = 3
_RETRY_BASE_DELAY_SECONDS = 120
_CONCURRENCY_CONFLICT_MARKERS = (
    # Java/Scala exception class names — embedded in str(exc) on classic
    # (non-Connect) clusters.
    "ConcurrentAppendException",
    "ConcurrentModificationException",
    "ConcurrentDeleteReadException",
    "ConcurrentDeleteDeleteException",
    "ConcurrentTransactionException",
    "MetadataChangedException",
    "ProtocolChangedException",
    "ConcurrentWriteException",
    # Delta/Databricks error-class codes — on Spark Connect / serverless, str(exc)
    # contains ONLY this code (e.g. "[DELTA_CONCURRENT_APPEND...]"), never the
    # class name above, so these markers are required to catch conflicts there.
    "DELTA_CONCURRENT_APPEND",
    "DELTA_CONCURRENT_DELETE_READ",
    "DELTA_CONCURRENT_DELETE_DELETE",
    "DELTA_METADATA_CHANGED",
    "DELTA_PROTOCOL_CHANGED",
    "DELTA_CONCURRENT_TRANSACTION",
)
# Spark Connect raises typed exceptions (e.g. delta.connect.exceptions.
# ConcurrentAppendException) whose message text has NO class-name substring at
# all; type(exc).__name__ still matches exactly, so check that too.
_CONCURRENCY_CONFLICT_TYPE_NAMES = {
    "ConcurrentAppendException",
    "ConcurrentModificationException",
    "ConcurrentDeleteReadException",
    "ConcurrentDeleteDeleteException",
    "ConcurrentTransactionException",
    "MetadataChangedException",
    "ProtocolChangedException",
    "ConcurrentWriteException",
}


class DeltaWriter:
    """Full load / SCD1 / SCD2 writer — serverless-safe and FGAC-safe."""

    # -------------------------------------------------------------------
    # Public: write dispatcher
    # -------------------------------------------------------------------
    @staticmethod
    def _resolve_col_name(columns: List[str], requested: str) -> Optional[str]:
        """Case-insensitive resolver for a column name."""
        if not requested:
            return None
        lookup = {c.lower(): c for c in columns}
        return lookup.get(requested.lower())

    @staticmethod
    def _schema_field_map(schema) -> dict:
        """Case-insensitive field lookup for a schema."""
        return {field.name.lower(): field for field in schema.fields}

    @staticmethod
    def _format_schema_fields(schema) -> str:
        """Format schema fields for logging."""
        return ", ".join(
            f"{field.name}:{field.dataType.simpleString()}"
            for field in schema.fields
        )

    @staticmethod
    def _normalize_partition_columns(columns: List[str], requested: Optional[List[str]]) -> List[str]:
        """Resolve partition columns case-insensitively and preserve order."""
        if not requested:
            return []
        lookup = {c.lower(): c for c in columns}
        resolved = []
        for col_name in requested:
            actual = lookup.get(col_name.lower())
            if actual and actual not in resolved:
                resolved.append(actual)
        return resolved

    @staticmethod
    def _merge_partition_columns(*column_groups: Optional[List[str]]) -> List[str]:
        """Merge partition column groups without duplicates, preserving order."""
        merged = []
        seen = set()
        for group in column_groups:
            for col_name in group or []:
                key = col_name.lower()
                if key in seen:
                    continue
                merged.append(col_name)
                seen.add(key)
        return merged

    @staticmethod
    def _requested_physical_partition_by(
        physical_partition_by: Optional[List[str]],
        *,
        spark=None,
        target_table: Optional[str] = None,
    ) -> List[str]:
        """Apply ctl_pipeline physical_partition_by when available; otherwise fall back to defaults unless explicitly overridden."""
        if physical_partition_by is not None:
            return list(physical_partition_by)

        ctl_physical_partition_by = DeltaWriter._load_ctl_pipeline_physical_partition_by(
            spark,
            target_table,
        )
        if ctl_physical_partition_by:
            return ctl_physical_partition_by
        return list(_DEFAULT_PHYSICAL_PARTITION_BY)

    @staticmethod
    def _ctl_pipeline_table_for_target(target_table: Optional[str]) -> str:
        """Resolve the ctl_pipeline control-table location for the target table's catalog."""
        if not target_table:
            return "process_audit_logs.ctl_pipeline"

        parts = target_table.replace("`", "").split(".")
        if len(parts) == 3:
            return f"`{parts[0]}`.`process_audit_logs`.`ctl_pipeline`"
        return "process_audit_logs.ctl_pipeline"

    @staticmethod
    def _load_ctl_pipeline_physical_partition_by(
        spark,
        target_table: Optional[str],
    ) -> List[str]:
        """Read physical_partition_by from ctl_pipeline for the current gold target table."""
        if spark is None or not target_table:
            return []

        ctl_pipeline_table = DeltaWriter._ctl_pipeline_table_for_target(target_table)
        target_table_name = target_table.replace("`", "").split(".")[-1]
        safe_target_table_name = target_table_name.replace("'", "''")
        query = f"""
            SELECT physical_partition_by
            FROM {ctl_pipeline_table}
            WHERE upper(target_table) = upper('{safe_target_table_name}')
              AND upper(layer) = 'GOLD'
              AND coalesce(is_active, true) = true
              AND physical_partition_by IS NOT NULL
              AND trim(physical_partition_by) <> ''
            ORDER BY updated_ts DESC
            LIMIT 1
        """
        try:
            rows = spark.sql(query).collect()
            if rows and rows[0]["physical_partition_by"]:
                ctl_physical_partition_by = [
                    col_name.strip()
                    for col_name in str(rows[0]["physical_partition_by"]).split(",")
                    if col_name and col_name.strip()
                ]
                if ctl_physical_partition_by:
                    logger.info(
                        "[DeltaWriter] Using ctl_pipeline physical_partition_by for %s from %s: %s",
                        target_table,
                        ctl_pipeline_table,
                        ctl_physical_partition_by,
                    )
                    return ctl_physical_partition_by
        except Exception as exc:
            logger.warning(
                "[DeltaWriter] Could not read physical_partition_by from %s for %s: %s",
                ctl_pipeline_table,
                target_table,
                str(exc).splitlines()[0],
            )
        return []

    @staticmethod
    def _resolve_physical_partition_by(
        source_columns: List[str],
        physical_partition_by: Optional[List[str]],
    ) -> List[str]:
        """Resolve requested physical partition columns present in the source DataFrame."""
        requested_physical_partition_by = DeltaWriter._requested_physical_partition_by(physical_partition_by)
        return DeltaWriter._normalize_partition_columns(source_columns, requested_physical_partition_by)

    @staticmethod
    def _validate_requested_physical_partition_columns(
        columns: List[str],
        physical_partition_by: Optional[List[str]],
        *,
        target_table: str,
        scope_name: str,
    ) -> List[str]:
        """Fail fast when required physical partition columns are missing from a DataFrame."""
        requested_physical_partition_by = DeltaWriter._requested_physical_partition_by(physical_partition_by)
        if not requested_physical_partition_by:
            return []

        resolved_physical_partition_by = DeltaWriter._normalize_partition_columns(
            columns,
            requested_physical_partition_by,
        )
        resolved_lookup = {c.lower() for c in resolved_physical_partition_by}
        missing_physical_partition_by = [
            col_name for col_name in requested_physical_partition_by if col_name.lower() not in resolved_lookup
        ]
        if missing_physical_partition_by:
            raise ValueError(
                f"[DeltaWriter] {scope_name} for {target_table} requires physical_partition_by "
                f"{requested_physical_partition_by}; missing columns={missing_physical_partition_by}"
            )
        return resolved_physical_partition_by

    @staticmethod
    def _validate_target_physical_partition_layout(
        spark,
        target_table: str,
        physical_partition_by: Optional[List[str]],
    ) -> List[str]:
        """Ensure the existing target table is physically partitioned on the requested product keys."""
        requested_physical_partition_by = DeltaWriter._requested_physical_partition_by(physical_partition_by)
        if not requested_physical_partition_by:
            return []

        target_partition_by = DeltaWriter._target_partition_columns(spark, target_table)
        target_partition_lookup = {c.lower() for c in target_partition_by}
        missing_physical_partition_by = [
            col_name for col_name in requested_physical_partition_by if col_name.lower() not in target_partition_lookup
        ]
        if missing_physical_partition_by:
            raise ValueError(
                f"[DeltaWriter] {target_table} must be physically partitioned by "
                f"{requested_physical_partition_by} to guarantee partition-local locking; "
                f"existing partition columns={target_partition_by}"
            )

        logger.info(
            "[DeltaWriter] Verified physical partition layout for %s on %s",
            target_table,
            target_partition_by,
        )
        logger.info(
            "[DeltaWriter] Validation evidence — partition-level locking is configured on physical partition columns %s for %s",
            requested_physical_partition_by,
            target_table,
        )
        return target_partition_by

    @staticmethod
    def _shared_physical_partition_columns(
        source_columns: List[str],
        target_columns: List[str],
        physical_partition_by: Optional[List[str]],
        *,
        target_table: str,
        op_name: str,
        require_all: bool = False,
    ) -> List[str]:
        """Resolve shared physical partition columns, optionally failing if any requested column is missing."""
        requested_physical_partition_by = DeltaWriter._requested_physical_partition_by(physical_partition_by)
        if not requested_physical_partition_by:
            return []

        src_physical = DeltaWriter._normalize_partition_columns(source_columns, requested_physical_partition_by)
        tgt_physical = DeltaWriter._normalize_partition_columns(target_columns, requested_physical_partition_by)
        src_lookup = {c.lower() for c in src_physical}
        tgt_lookup = {c.lower() for c in tgt_physical}
        shared_physical = [col_name for col_name in src_physical if col_name.lower() in tgt_lookup]

        if require_all and len(shared_physical) != len(requested_physical_partition_by):
            missing_in_source = [
                col_name for col_name in requested_physical_partition_by if col_name.lower() not in src_lookup
            ]
            missing_in_target = [
                col_name for col_name in requested_physical_partition_by if col_name.lower() not in tgt_lookup
            ]
            raise ValueError(
                f"[DeltaWriter] {op_name} for {target_table} requires shared physical_partition_by "
                f"{requested_physical_partition_by}; missing_in_source={missing_in_source or []}; "
                f"missing_in_target={missing_in_target or []}"
            )

        if shared_physical:
            logger.info(
                "[DeltaWriter] %s — Using physical partition scope for %s on %s",
                op_name,
                target_table,
                shared_physical,
            )
            logger.info(
                "[DeltaWriter] Validation evidence — locking/scoping is happening at partition columns %s for %s during %s",
                shared_physical,
                target_table,
                op_name,
            )
        return shared_physical

    @staticmethod
    def _resolve_write_partition_by(
        source_columns: List[str],
        partition_by: Optional[List[str]],
        physical_partition_by: Optional[List[str]],
    ) -> List[str]:
        """Combine logical and physical partition columns for Delta table layout."""
        logical_partition_by = DeltaWriter._normalize_partition_columns(source_columns, partition_by)
        physical_partition_cols = DeltaWriter._resolve_physical_partition_by(source_columns, physical_partition_by)
        return DeltaWriter._merge_partition_columns(logical_partition_by, physical_partition_cols)

    @staticmethod
    def _target_partition_columns(spark, target_table: str) -> List[str]:
        """Read existing Delta table partition columns via DESCRIBE DETAIL."""
        try:
            rows = spark.sql(f"DESCRIBE DETAIL {target_table}").select("partitionColumns").collect()
            if rows and rows[0]["partitionColumns"]:
                return list(rows[0]["partitionColumns"])
        except Exception as exc:
            logger.warning(
                "[DeltaWriter] Could not read existing partition columns for %s: %s",
                target_table,
                str(exc).splitlines()[0],
            )
        return []

    @staticmethod
    def _build_eq_null_safe_condition(
        source_columns: List[str],
        target_columns: List[str],
        join_columns: List[str],
        *,
        source_alias: str = "s",
        target_alias: str = "t",
    ):
        """Build an eqNullSafe join condition using case-insensitive column resolution."""
        condition = None
        for col_name in join_columns:
            source_col = DeltaWriter._resolve_col_name(source_columns, col_name)
            target_col = DeltaWriter._resolve_col_name(target_columns, col_name)
            if not source_col or not target_col:
                continue
            predicate = F.col(f"{target_alias}.`{target_col}`").eqNullSafe(F.col(f"{source_alias}.`{source_col}`"))
            condition = predicate if condition is None else (condition & predicate)
        return condition

    @staticmethod
    def _resolve_merge_keys(
        *,
        target_table: str,
        primary_keys: List[str],
        partition_by: Optional[List[str]],
        physical_partition_by: Optional[List[str]],
        source_columns: List[str],
        target_columns: List[str],
    ) -> List[str]:
        """Resolve merge keys, preserving current behavior and appending physical partition predicates."""
        if "LNK_" in target_table.strip().upper() and partition_by:
            base_merge_keys = DeltaWriter._normalize_partition_columns(source_columns, partition_by)
            logger.info(
                "Using partition_by columns as merge keys for link table %s: %s",
                target_table,
                base_merge_keys,
            )
        else:
            base_merge_keys = DeltaWriter._normalize_partition_columns(source_columns, primary_keys)
            logger.info(
                "Using primary_keys as merge keys for table %s: %s",
                target_table,
                base_merge_keys,
            )

        shared_physical = DeltaWriter._shared_physical_partition_columns(
            source_columns,
            target_columns,
            physical_partition_by,
            target_table=target_table,
            op_name="merge key resolution",
            require_all=True,
        )
        return DeltaWriter._merge_partition_columns(base_merge_keys, shared_physical)

    @staticmethod
    def _append_shared_physical_partition_columns(
        join_columns: List[str],
        source_columns: List[str],
        target_columns: List[str],
        physical_partition_by: Optional[List[str]],
        *,
        target_table: str,
        op_name: str,
    ) -> List[str]:
        """Append shared physical partition columns to an existing join-column list."""
        shared_physical = DeltaWriter._shared_physical_partition_columns(
            source_columns,
            target_columns,
            physical_partition_by,
            target_table=target_table,
            op_name=op_name,
            require_all=True,
        )
        return DeltaWriter._merge_partition_columns(join_columns, shared_physical)

    @staticmethod
    def _build_partition_filter_predicate(
        source_df: DataFrame,
        partition_by: List[str],
    ) -> Optional[str]:
        """
        Build a WHERE clause predicate to filter target reads to only the partition
        values present in the source batch. This enables partition pruning at read time.
        Returns None if no partitions are specified or source is empty.
        """
        if not partition_by or not source_df.count():
            return None

        try:
            # Get distinct partition value combinations from source
            partition_values = source_df.select(*partition_by).distinct().collect()
            if not partition_values:
                return None

            # Build OR'd equality conditions for each partition combination
            predicates = []
            for row in partition_values:
                conditions = []
                for col_name in partition_by:
                    val = row[col_name]
                    if val is None:
                        conditions.append(f"`{col_name}` IS NULL")
                    else:
                        # Escape single quotes in string values
                        safe_val = str(val).replace("'", "''")
                        conditions.append(f"`{col_name}` = '{safe_val}'")
                if conditions:
                    predicates.append(f"({' AND '.join(conditions)})")

            if predicates:
                where_clause = " OR ".join(predicates)
                logger.info(
                    "[DeltaWriter] Built partition filter predicate for %d partition combinations: %s",
                    len(partition_values),
                    where_clause[:200] + ("..." if len(where_clause) > 200 else ""),
                )
                return where_clause
        except Exception as exc:
            logger.warning(
                "[DeltaWriter] Could not build partition filter predicate: %s",
                str(exc).splitlines()[0],
            )
        return None

    @staticmethod
    def _scope_target_df_to_source_partitions(
        source_df: DataFrame,
        target_df: DataFrame,
        physical_partition_by: Optional[List[str]],
        *,
        target_table: str,
        op_name: str,
    ) -> DataFrame:
        """Restrict target reads to the physical partitions present in the source batch."""
        requested_physical_partition_by = DeltaWriter._requested_physical_partition_by(physical_partition_by)
        if not requested_physical_partition_by:
            logger.info(
                "[DeltaWriter] %s — Physical partition scoping is disabled for %s",
                op_name,
                target_table,
            )
            return target_df

        shared_physical = DeltaWriter._shared_physical_partition_columns(
            source_df.columns,
            target_df.columns,
            requested_physical_partition_by,
            target_table=target_table,
            op_name=op_name,
            require_all=True,
        )

        source_partition_scope = source_df.select(
            *[F.col(f"`{col_name}`").alias(col_name) for col_name in shared_physical]
        ).distinct()
        scope_condition = DeltaWriter._build_eq_null_safe_condition(
            source_partition_scope.columns,
            target_df.columns,
            shared_physical,
            source_alias="p",
            target_alias="t",
        )
        if scope_condition is None:
            raise ValueError(
                f"[DeltaWriter] {op_name} for {target_table} could not build a scoped target read predicate "
                f"for physical partitions {shared_physical}"
            )

        logger.info(
            "[DeltaWriter] %s — Scoping target reads for %s to physical partitions on %s",
            op_name,
            target_table,
            shared_physical,
        )
        logger.info(
            "[DeltaWriter] Validation evidence — target read/lock scope is restricted to partition columns %s for %s",
            shared_physical,
            target_table,
        )
        return target_df.alias("t").join(source_partition_scope.alias("p"), scope_condition, "left_semi")

    @staticmethod
    def _is_concurrency_conflict(exc: Exception) -> bool:
        """True only for Delta OCC commit conflicts; genuine failures return False."""
        if type(exc).__name__ in _CONCURRENCY_CONFLICT_TYPE_NAMES:
            return True
        msg = str(exc)
        return any(marker in msg for marker in _CONCURRENCY_CONFLICT_MARKERS)

    @staticmethod
    def _run_with_concurrency_retry(action, *, target_table: str, op_name: str):
        """
        Execute a Delta commit (`action`), retrying ONLY on optimistic-concurrency
        conflicts raised when another writer commits to the same table between our
        read and our commit. Genuine failures propagate immediately, unretried.
        """
        retry_count = 0
        while True:
            try:
                return action()
            except Exception as exc:
                if not DeltaWriter._is_concurrency_conflict(exc):
                    raise
                if retry_count >= _MAX_CONCURRENCY_RETRIES:
                    logger.error(
                        "[DeltaWriter] %s on %s failed after %d concurrency retries: %s",
                        op_name, target_table, retry_count, str(exc).splitlines()[0],
                    )
                    raise
                retry_count += 1
                delay = _RETRY_BASE_DELAY_SECONDS * (2 ** (retry_count - 1)) + random.uniform(0, 1)
                logger.info(
                    "[DeltaWriter] %s on %s hit a concurrent write conflict (retry %d/%d); retrying in %.1fs: %s",
                    op_name, target_table, retry_count, _MAX_CONCURRENCY_RETRIES, delay, str(exc).splitlines()[0],
                )
                time.sleep(delay)

    @staticmethod
    def _align_df_to_target_schema(
        spark,
        df: DataFrame,
        target_table: str,
        table_exists: bool,
    ):
        """
        Align matching columns to the existing target schema and proactively truncate
        CHAR/VARCHAR values before saveAsTable() so Delta length checks do not fail.
        """
        if not table_exists:
            return df

        try:
            logger.info("[DeltaWriter] Validating source schema against target schema for %s", target_table)
            source_schema = df.schema
            target_schema = spark.table(target_table).schema

            logger.info("[DeltaWriter] Source schema for %s -> %s", target_table, DeltaWriter._format_schema_fields(source_schema))
            logger.info("[DeltaWriter] Target schema for %s -> %s", target_table, DeltaWriter._format_schema_fields(target_schema))

            source_fields = DeltaWriter._schema_field_map(source_schema)
            target_fields = DeltaWriter._schema_field_map(target_schema)

            transform_map = {}
            cast_messages = []
            truncated_columns = []

            for source_key, source_field in source_fields.items():
                target_field = target_fields.get(source_key)
                if not target_field:
                    logger.debug(
                        "[DeltaWriter] Source column %s(%s) not present in target schema for %s",
                        source_field.name,
                        source_field.dataType.simpleString(),
                        target_table,
                    )
                    continue

                source_col = source_field.name
                source_type = source_field.dataType
                target_type = target_field.dataType
                logger.debug(
                    "[DeltaWriter] Evaluating source column %s(%s) against target column %s(%s)",
                    source_col,
                    source_type.simpleString(),
                    target_field.name,
                    target_type.simpleString(),
                )
                source_col_expr = F.col(f"`{source_col}`")
                source_string_expr = source_col_expr.cast("string")
                column_expr = source_col_expr
                needs_transform = False

                max_len = (
                    int(target_type.length)
                    if isinstance(target_type, (CharType, VarcharType))
                    and getattr(target_type, "length", None) is not None
                    else None
                )
                if max_len is None:
                    char_varchar_type = target_field.metadata.get("__CHAR_VARCHAR_TYPE_STRING")
                    if char_varchar_type:
                        _, separator, length_text = str(char_varchar_type).partition("(")
                        if separator and length_text.endswith(")"):
                            try:
                                max_len = int(length_text[:-1])
                                logger.debug(
                                    "[DeltaWriter] Resolved length constraint for column %s from metadata %s -> %s",
                                    source_col,
                                    char_varchar_type,
                                    max_len,
                                )
                            except ValueError:
                                logger.warning(
                                    "[DeltaWriter] Could not parse char/varchar metadata for %s on %s: %s",
                                    source_col,
                                    target_table,
                                    char_varchar_type,
                                )

                if max_len is not None:
                    logger.debug(
                        "[DeltaWriter] Applying length validation for column %s with target type %s and max length %s",
                        source_col,
                        target_type.simpleString(),
                        max_len,
                    )
                    truncated_columns.append(f"{source_col}({max_len})")
                    column_expr = F.when(
                        source_col_expr.isNull(),
                        F.lit(None),
                    ).otherwise(F.substring(source_string_expr, 1, max_len)).cast(target_type)
                    needs_transform = True
                elif source_type != target_type:
                    logger.debug(
                        "[DeltaWriter] Casting source column %s from %s to %s",
                        source_col,
                        source_type.simpleString(),
                        target_type.simpleString(),
                    )
                    column_expr = source_col_expr.cast(target_type)
                    needs_transform = True

                if source_type != target_type:
                    cast_messages.append(
                        f"{source_col}:{source_type.simpleString()}->{target_type.simpleString()}"
                    )

                if needs_transform:
                    transform_map[source_col] = column_expr

            if cast_messages:
                logger.info(
                    "[DeltaWriter] Casted source columns to target schema for %s: %s",
                    target_table,
                    ", ".join(cast_messages),
                )
            else:
                logger.debug("[DeltaWriter] No datatype casts required for %s", target_table)

            if truncated_columns:
                logger.info(
                    "[DeltaWriter] Applied string length alignment for %s on columns: %s",
                    target_table,
                    ", ".join(truncated_columns),
                )

            aligned_df = df.withColumns(transform_map) if transform_map else df
            logger.debug(
                "[DeltaWriter] Schema alignment complete for %s. transformed_columns=%s",
                target_table,
                ", ".join(transform_map.keys()) if transform_map else "none",
            )
            return aligned_df
        except Exception as exc:
            logger.warning(
                "[DeltaWriter] Could not align source schema to %s: %s",
                target_table,
                str(exc).splitlines()[0],
            )
            return df

    @staticmethod
    def _write_data(
        spark,
        df: DataFrame,
        target_table: str,
        partition_by: Optional[List[str]] = None,
        physical_partition_by: Optional[List[str]] = None,
        clear_existing: bool = False,
        table_exists: bool = False,
    ) -> None:
        """Write data once, optionally clearing existing rows with the existing MERGE pattern."""
        partition_by = partition_by or []
        requested_physical_partition_by = DeltaWriter._requested_physical_partition_by(
            physical_partition_by,
            spark=spark,
            target_table=target_table,
        )
        effective_partition_by = DeltaWriter._resolve_write_partition_by(
            df.columns,
            partition_by,
            requested_physical_partition_by,
        )

        logger.info(
            "************* Write Data logical_partition_by=%s physical_partition_by=%s clear_existing=%s effective_partition_by=%s",
            partition_by,
            requested_physical_partition_by,
            clear_existing,
            effective_partition_by,
        )
        if clear_existing:
            logger.info("*************Running MERGE INTO STATEMENT.....")
            DeltaWriter._run_with_concurrency_retry(
                lambda: spark.sql(
                    f"MERGE INTO {target_table} AS t USING (SELECT 1) AS s ON TRUE WHEN MATCHED THEN DELETE"
                ),
                target_table=target_table,
                op_name="clear_existing MERGE DELETE",
            )
            logger.info("*************MERGE INTO STATEMENT Ran Successfully!")

        df = DeltaWriter._align_df_to_target_schema(
            spark=spark,
            df=df,
            target_table=target_table,
            table_exists=table_exists,
        )

        writer = (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .option("delta.autoOptimize.optimizeWrite", "true")
            .option("delta.autoOptimize.autoCompact", "true")
        )

        try: 
            # Delta requires partitioning to match the existing table exactly on append;
            # apply the combined logical/physical partitioning for new tables, and only
            # reuse it on existing tables when the current layout already matches.
            if effective_partition_by:
                if not table_exists:
                    writer = writer.partitionBy(*effective_partition_by)
                else:
                    existing_partition_by = DeltaWriter._normalize_partition_columns(
                        df.columns,
                        DeltaWriter._target_partition_columns(spark, target_table),
                    )
                    if [c.lower() for c in existing_partition_by] == [c.lower() for c in effective_partition_by]:
                        writer = writer.partitionBy(*existing_partition_by)
                    else:
                        logger.info(
                            "[DeltaWriter] Skipping partitionBy on existing table %s. existing=%s requested=%s",
                            target_table,
                            existing_partition_by,
                            effective_partition_by,
                        )

            logger.info("**************saveAsTable in progress....")
            DeltaWriter._run_with_concurrency_retry(
                lambda: writer.saveAsTable(target_table),
                target_table=target_table,
                op_name="saveAsTable append",
            )

        except Exception as e:
            logger.error("ERROR::%s", e)
            raise RuntimeError("Unable to write the data -> %s", e)
    

    @staticmethod
    def write(
        df: DataFrame,
        target_table: str,
        load_type: str = "incremental",
        scd_type: str = "scd1",
        primary_keys: Optional[List[str]] = None,
        partition_by: Optional[List[str]] = None,
        physical_partition_by: Optional[List[str]] = None,
        hashdiff_col: str = "hashdiff",
        vacuum_retention_hours: int = 168,
        watermark_col: Optional[str] = None,
        rank_order_cols: Optional[str] = None,
        layer: str = "GOLD",
    ) -> dict:
        primary_keys = primary_keys or []
        partition_by = partition_by or []
        spark        = df.sparkSession
        layer_upper  = (layer or "GOLD").upper()
        
        # GOLD layer: use physical partitioning; SILVER layer: skip physical partition logic
        logger.info("[DeltaWriter] write() called with layer=%s for %s", layer_upper, target_table)
        
        # Physical partition logic only applies to GOLD layer
        if layer_upper == "GOLD":
            requested_physical_partition_by = DeltaWriter._requested_physical_partition_by(
                physical_partition_by,
                spark=spark,
                target_table=target_table,
            )
            effective_physical_partition_by = DeltaWriter._validate_requested_physical_partition_columns(
                df.columns,
                requested_physical_partition_by,
                target_table=target_table,
                scope_name="source DataFrame",
            )
        else:
            logger.info("[DeltaWriter] SILVER layer: skipping physical partition validation for %s", target_table)
            effective_physical_partition_by = []
        
        exists       = DeltaWriter._table_exists(spark, target_table)   # FGAC-safe (SHOW TABLES)
        if exists and layer_upper == "GOLD":
            DeltaWriter._validate_target_physical_partition_layout(
                spark,
                target_table,
                effective_physical_partition_by,
            )

        try:
            # First load / full overwrite ------------------------------------
            logger.info("*****************First load / full overwrite %s, %s",exists, load_type)

            if not exists or load_type.lower() == "full":
                logger.info("[DeltaWriter] %s -> %s",
                            "FIRST LOAD" if not exists else "FULL OVERWRITE", target_table)

                # SCD1 Hub/Link tables: stamp LD_DT the same way SCD2 does, so the
                # column is populated on first load instead of relying on ctl_column_map.
                if scd_type.lower() == "scd1":
                    _ld_dt_col = DeltaWriter._resolve_col_name(df.columns, "LD_DT")
                    if _ld_dt_col:
                        df = df.withColumn(_ld_dt_col, F.current_timestamp())

                DeltaWriter._write_data(
                    spark=spark,
                    df=df,
                    target_table=target_table,
                    partition_by=partition_by,
                    physical_partition_by=effective_physical_partition_by,
                    clear_existing=exists and load_type.lower() == "full",
                    table_exists=exists,
                )

                # Row count from the write's OWN metrics — no second compute.
                row_count = DeltaWriter._written_rows(spark, target_table)
                logger.info("**********Row_count-> %s", row_count)
                return {
                    "rows_inserted": row_count,
                    "rows_updated": 0,
                    "rows_affected": row_count,
                    "new_watermark_ts": DeltaWriter._max_watermark(spark, target_table, watermark_col),
                }

        except Exception as exc:
            # Concurrent-create race on first load -> fall back to MERGE.
            if not exists and DeltaWriter._is_table_already_exists_error(exc):
                logger.warning("[DeltaWriter] FIRST LOAD race for %s; merging instead.", target_table)
                if not primary_keys:
                    raise ValueError(
                        f"[DeltaWriter] concurrent create for {target_table} but no primary_keys"
                    ) from exc
                if scd_type.lower() == "scd2":
                    return DeltaWriter._write_scd2(df, target_table, primary_keys, hashdiff_col, partition_by, watermark_col, rank_order_cols, effective_physical_partition_by, layer=layer_upper)
                return DeltaWriter._write_scd1(df, target_table, primary_keys, hashdiff_col, watermark_col, False, effective_physical_partition_by, layer=layer_upper)
            raise

        # Incremental MERGE ----------------------------------------------
        if not primary_keys:
            raise ValueError(f"[DeltaWriter] incremental load requires primary_keys for {target_table}")

        if scd_type.lower() == "scd2":
            return DeltaWriter._write_scd2(df, target_table, primary_keys, hashdiff_col, partition_by, watermark_col, rank_order_cols, effective_physical_partition_by, layer=layer_upper)
        return DeltaWriter._write_scd1(df, target_table, primary_keys, hashdiff_col, watermark_col, False, effective_physical_partition_by, layer=layer_upper)

    # -------------------------------------------------------------------
    # SCD1 incremental MERGE (hashdiff-gated upsert)
    # -------------------------------------------------------------------
    @staticmethod
    def _write_scd1(df, target_table, primary_keys, hashdiff_col="hashdiff", watermark_col=None, scd1_insert_only=False, physical_partition_by=None, layer="GOLD") -> dict:
        spark = df.sparkSession
        layer_upper = (layer or "GOLD").upper()
        
        # Physical partition logic only for GOLD layer
        if layer_upper == "GOLD":
            effective_physical_partition_by = DeltaWriter._requested_physical_partition_by(
                physical_partition_by,
                spark=spark,
                target_table=target_table,
            )
            DeltaWriter._validate_requested_physical_partition_columns(
                df.columns,
                effective_physical_partition_by,
                target_table=target_table,
                scope_name="SCD1 source DataFrame",
            )
            DeltaWriter._validate_target_physical_partition_layout(
                spark,
                target_table,
                effective_physical_partition_by,
            )
        else:
            logger.info("[DeltaWriter] SCD1 SILVER layer: skipping physical partition validation for %s", target_table)
            effective_physical_partition_by = []
        
        tgt   = DeltaTable.forName(spark, target_table)   # FGAC allows forName + merge()/toDF()

        tgt_df  = tgt.toDF()

        logger.info("[DeltaWriter] SCD1 source schema for %s -> %s", target_table, DeltaWriter._format_schema_fields(df.schema))
        logger.info("[DeltaWriter] SCD1 target schema for %s -> %s", target_table, DeltaWriter._format_schema_fields(tgt_df.schema))
        # Stamp LD_DT the same way SCD2 does: current_timestamp() for every row in the
        # incoming batch. On insert this becomes the row's load date; on update it is
        # excluded from the matched-update set below so the original load date is preserved.
        ld_dt_col = DeltaWriter._resolve_col_name(tgt_df.columns, "LD_DT")
        if ld_dt_col:
            df = df.withColumn(ld_dt_col, F.current_timestamp())

        df = DeltaWriter._align_df_to_target_schema(
            spark=spark,
            df=df,
            target_table=target_table,
            table_exists=True,
        )

        merge_keys = DeltaWriter._resolve_merge_keys(
            target_table=target_table,
            primary_keys=primary_keys,
            partition_by=None,
            physical_partition_by=effective_physical_partition_by,
            source_columns=df.columns,
            target_columns=tgt_df.columns,
        )
        pk_cond = DeltaWriter._build_eq_null_safe_condition(
            df.columns,
            tgt_df.columns,
            merge_keys,
            source_alias="s",
            target_alias="t",
        )
        if pk_cond is None:
            raise ValueError(
                f"[DeltaWriter] unable to resolve SCD1 merge keys for {target_table}: "
                f"primary_keys={primary_keys}, physical_partition_by={effective_physical_partition_by}"
            )

        logger.info("[DeltaWriter] SCD1 MERGE -> %s", target_table)

        has_hashdiff = hashdiff_col in df.columns
        merger = tgt.alias("t").merge(df.alias("s"), pk_cond)

        # Matched-update column set: all source columns except LD_DT (keeps the
        # original load date untouched on subsequent updates to the same key).
        update_set = {c: F.col(f"s.`{c}`") for c in df.columns if not ld_dt_col or c != ld_dt_col}

        if scd1_insert_only:
            logger.info("[DeltaWriter] SCD1 insert-only mode for %s", target_table)
        elif has_hashdiff:
            merger = merger.whenMatchedUpdate(
                condition=(F.col(f"t.{hashdiff_col}") != F.col(f"s.{hashdiff_col}")),
                set=update_set,
            )
        else:
            technical_cols = {"LD_DT", "LD_END_DT", "BTCH_ID", "BTCH_DT", "SRC_EFF_DT", "SRC_EXPRN_DT", "REF_ID", "ERR_CD", "ERR_FLG", "REC_SRC_NM", "PART_COL"}
            compare_cols = [
                c for c in sorted(set(df.columns).intersection(set(tgt_df.columns)))
                if c not in primary_keys and c.upper() not in technical_cols
            ]
            if compare_cols:
                change_cond = None
                for c in compare_cols:
                    col_changed = ~F.col(f"t.`{c}`").eqNullSafe(F.col(f"s.`{c}`"))
                    change_cond = col_changed if change_cond is None else (change_cond | col_changed)
                merger = merger.whenMatchedUpdateAll(condition=change_cond)
                merger = merger.whenMatchedUpdate(condition=change_cond, set=update_set)
                logger.info("[DeltaWriter] SCD1 fallback compare on %d column(s) for %s", len(compare_cols), target_table)
            else:
                logger.warning("[DeltaWriter] No comparable columns for %s; unconditional matched update", target_table)
                merger = merger.whenMatchedUpdate(set=update_set)
                
        try:
            merger = merger.whenNotMatchedInsertAll()
            DeltaWriter._run_with_concurrency_retry(
                merger.execute,
                target_table=target_table,
                op_name="SCD1 MERGE",
            )
        except Exception as e:
            raise RuntimeError(f"[DeltaWriter] SCD1 merge failed for {target_table}: {e}") from e
        metrics = DeltaWriter._read_merge_metrics(spark, target_table)
        metrics_json = {
            "rows_inserted":    metrics.get("numTargetRowsInserted", 0),
            "rows_updated":     metrics.get("numTargetRowsUpdated",  0),
            "rows_affected":    metrics.get("numTargetRowsInserted", 0) + metrics.get("numTargetRowsUpdated", 0),
            "new_watermark_ts": DeltaWriter._max_watermark(spark, target_table, watermark_col),
        }
        return metrics_json

    # -------------------------------------------------------------------
    # SCD2 MERGE (LD_END_DT based)
    # -------------------------------------------------------------------
    @staticmethod
    def _write_scd2(df, target_table, primary_keys, hashdiff_col="hashdiff", partition_by=None, watermark_col=None, rank_order_cols=None, physical_partition_by=None, layer="GOLD") -> dict:
        spark        = df.sparkSession
        partition_by = partition_by or []
        layer_upper  = (layer or "GOLD").upper()

        for c in ["REC_SRC_NM", "ERR_CD"]:
            if c in df.columns:
                df = df.withColumn(c, F.col(c).cast("string"))
        if "ERR_FLG" in df.columns:
            df = df.withColumn("ERR_FLG", F.col("ERR_FLG").cast("int"))

        # Physical partition logic only for GOLD layer
        if layer_upper == "GOLD":
            effective_physical_partition_by = DeltaWriter._requested_physical_partition_by(
                physical_partition_by,
                spark=spark,
                target_table=target_table,
            )
            exists = DeltaWriter._table_exists(spark, target_table)
            DeltaWriter._validate_requested_physical_partition_columns(
                df.columns,
                effective_physical_partition_by,
                target_table=target_table,
                scope_name="SCD2 source DataFrame",
            )
            if exists:
                DeltaWriter._validate_target_physical_partition_layout(
                    spark,
                    target_table,
                    effective_physical_partition_by,
                )
        else:
            logger.info("[DeltaWriter] SCD2 SILVER layer: skipping physical partition validation for %s", target_table)
            effective_physical_partition_by = []
            exists = DeltaWriter._table_exists(spark, target_table)
        
        df = DeltaWriter._align_df_to_target_schema(
            spark=spark,
            df=df,
            target_table=target_table,
            table_exists=exists,
        )

        # FIRST LOAD -----------------------------------------------------
        if not exists:
            logger.info("[DeltaWriter] SCD2 FIRST LOAD -> %s", target_table)
            first_df = (
                df.withColumn("LD_DT", F.current_timestamp())
                  .withColumn("LD_END_DT", F.lit(None).cast("timestamp"))
            )

            # writer = first_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            # if partition_by:
            #     writer = writer.partitionBy(*partition_by)
            try:
                DeltaWriter._write_data(
                    spark=spark,
                    df=first_df,
                    target_table=target_table,
                    partition_by=partition_by,
                    physical_partition_by=effective_physical_partition_by,
                    clear_existing=exists,
                )
            except Exception as e:
                raise RuntimeError(f"[DeltaWriter] SCD2 first load failed for {target_table}: {e}") from e

            row_count = DeltaWriter._written_rows(spark, target_table)
            return {
                "rows_inserted": row_count, "rows_updated": 0, "rows_affected": row_count,
                "new_watermark_ts": DeltaWriter._max_watermark(spark, target_table, watermark_col),
            }

        # INCREMENTAL SCD2 -----------------------------------------------
        tgt = DeltaTable.forName(spark, target_table)
        tgt_df = tgt.toDF()
        # apply src_exprn_dt only when both target schema and source df carry the required columns
        has_src_exprn_dt = (
            DeltaWriter._resolve_col_name(tgt_df.columns, "src_exprn_dt") is not None
            and DeltaWriter._resolve_col_name(df.columns, "src_eff_dt") is not None
        )

        # When rank_order_cols is set, dedup source to rank=1 per PK for MERGE to avoid multi-row
        # match errors; keep full df for INSERT so all unique revisions (e.g. different REF_IDs) are written.
        _df_for_merge = df
        _row_key = list(primary_keys)
        _merge_key = DeltaWriter._resolve_merge_keys(
            target_table=target_table,
            primary_keys=primary_keys,
            partition_by=partition_by,
            physical_partition_by=effective_physical_partition_by,
            source_columns=df.columns,
            target_columns=tgt_df.columns,
        )
        logger.info("[DeltaWriter] SCD2 MERGE (LD_END_DT) -> %s on %s", target_table, _merge_key)
        _rank_extra = []  # rank cols not in PK — used for within-batch SCD2 insert key
        _rank_exprs = []  # Define early for STEP 2 access when ranking inserted records
        if rank_order_cols:
            _cols_ci = {c.lower(): c for c in df.columns}
            _rank_exprs, _rank_extra = [], []
            for _token in rank_order_cols.split(","):
                _parts = _token.strip().split()
                _actual = _cols_ci.get(_parts[0].strip().lower())
                _dir = _parts[1].strip().upper() if len(_parts) > 1 else "DESC"
                if _actual:
                    _rank_exprs.append(F.col(_actual).desc() if _dir == "DESC" else F.col(_actual).asc())
                    if _actual.lower() not in {k.lower() for k in primary_keys}:
                        _rank_extra.append(_actual)
            if _rank_exprs:
                _merge_win = Window.partitionBy(*_merge_key).orderBy(*_rank_exprs)
                _df_for_merge = (
                    df.withColumn("__rn__", F.row_number().over(_merge_win))
                      .filter(F.col("__rn__") == 1)
                      .drop("__rn__")
                )

        logger.info("*******Using merge keys to Inactivate exisitng Active records*******")
        logger.info(f"merge keys are: {_merge_key}")
        pk_cond = DeltaWriter._build_eq_null_safe_condition(
            _df_for_merge.columns,
            tgt_df.columns,
            _merge_key,
            source_alias="s",
            target_alias="t",
        )
        if pk_cond is None:
            raise ValueError(
                f"[DeltaWriter] unable to resolve SCD2 merge keys for {target_table}: "
                f"primary_keys={primary_keys}, partition_by={partition_by}, physical_partition_by={effective_physical_partition_by}"
            )
        active_cond = F.col("t.`LD_END_DT`").isNull()

        # Resolve hashdiff columns case-insensitively (common: HASHDIFF in control/schema).
        src_hashdiff_col = DeltaWriter._resolve_col_name(_df_for_merge.columns, hashdiff_col)
        tgt_hashdiff_col = DeltaWriter._resolve_col_name(tgt_df.columns, hashdiff_col)

        if src_hashdiff_col and tgt_hashdiff_col:
            # Null-safe inequality: expire current row only when values truly differ.
            change_cond = ~F.col(f"t.`{tgt_hashdiff_col}`").eqNullSafe(F.col(f"s.`{src_hashdiff_col}`"))
        else:
            # Never default to unconditional TRUE; that causes full re-insert every run.
            # Fallback to comparing non-technical columns when hashdiff is unavailable.
            # SRC_EFF_DT / SRC_EXPRN_DT are excluded per keys-to-exclude-update-check convention.
            technical_cols = {"LD_DT", "LD_END_DT", "BTCH_ID", "BTCH_DT", "SRC_EFF_DT", "SRC_EXPRN_DT", "REF_ID", "ERR_CD", "ERR_FLG", "REC_SRC_NM", "PART_COL"}
            src_pk_map = {k.lower(): k for k in primary_keys}
            common_cols = []
            tgt_cols_map = {c.lower(): c for c in tgt_df.columns}
            for s_col in _df_for_merge.columns:
                s_lower = s_col.lower()
                if s_lower in src_pk_map:
                    continue
                if s_col.upper() in technical_cols:
                    continue
                t_col = tgt_cols_map.get(s_lower)
                if not t_col or t_col.upper() in technical_cols:
                    continue
                common_cols.append((s_col, t_col))

            if common_cols:
                logger.warning(
                    "[DeltaWriter] SCD2 hashdiff column not found (requested=%s, src=%s, tgt=%s). "
                    "Falling back to column-by-column change detection on %d column(s).",
                    hashdiff_col,
                    src_hashdiff_col,
                    tgt_hashdiff_col,
                    len(common_cols),
                )
                change_cond = None
                for s_col, t_col in common_cols:
                    col_changed = ~F.col(f"t.`{t_col}`").eqNullSafe(F.col(f"s.`{s_col}`"))
                    change_cond = col_changed if change_cond is None else (change_cond | col_changed)
            else:
                logger.warning(
                    "[DeltaWriter] SCD2 hashdiff column not found and no comparable columns found "
                    "(requested=%s, src=%s, tgt=%s). Treating rows as unchanged.",
                    hashdiff_col,
                    src_hashdiff_col,
                    tgt_hashdiff_col,
                )
                change_cond = F.lit(False)

        # STEP 1a: expire LD_END_DT — active row identified by LD_END_DT IS NULL
        try:
            logger.info("[DeltaWriter] SCD2 STEP 1a — Starting expire merge for %s on %d primary key(s)", target_table, len(primary_keys))
            logger.debug(f"pk_cond: {pk_cond}")
            logger.debug(f"active_cond: {active_cond}")
            logger.debug(f"change_cond: {change_cond}")
            DeltaWriter._run_with_concurrency_retry(
                lambda: (tgt.alias("t")
                    .merge(_df_for_merge.alias("s"), pk_cond & active_cond)
                    .whenMatchedUpdate(condition=change_cond, set={"LD_END_DT": F.current_timestamp() - F.expr("INTERVAL 1 SECOND")})
                    .execute()),
                target_table=target_table,
                op_name="SCD2 STEP 1a expire merge",
            )
            logger.info("[DeltaWriter] SCD2 STEP 1a — Expire merge completed, old records marked with LD_END_DT")
        except Exception as e:
            logger.error("[DeltaWriter] SCD2 STEP 1a — Expire merge failed: %s", str(e))
            raise RuntimeError(f"[DeltaWriter] SCD2 expire merge failed for {target_table}: {e}") from e
        # STEP 1b: expire src_exprn_dt independently — active row identified by SRC_EXPRN_DT IS NULL,
        # so this is decoupled from LD_END_DT and safe when products leave src_eff_dt null.
        if has_src_exprn_dt:
            src_exprn_active_cond = F.col("t.`SRC_EXPRN_DT`").isNull()
            try:
                logger.info("[DeltaWriter] SCD2 STEP 1b — Starting SRC_EXPRN_DT merge for %s", target_table)
                DeltaWriter._run_with_concurrency_retry(
                    lambda: (tgt.alias("t")
                        .merge(_df_for_merge.alias("s"), pk_cond & src_exprn_active_cond)
                        .whenMatchedUpdate(
                            condition=change_cond,
                            set={
                                "SRC_EXPRN_DT": F.when(
                                    F.col("s.`SRC_EFF_DT`").isNotNull(), F.col("s.`SRC_EFF_DT`")
                                ).otherwise(F.col("t.`SRC_EXPRN_DT`"))
                            },
                        )
                        .execute()),
                    target_table=target_table,
                    op_name="SCD2 STEP 1b src_exprn_dt merge",
                )
                logger.info("[DeltaWriter] SCD2 STEP 1b — SRC_EXPRN_DT merge completed")
            except Exception as e:
                logger.error("[DeltaWriter] SCD2 STEP 1b — SRC_EXPRN_DT merge failed: %s", str(e))
                raise RuntimeError(f"[DeltaWriter] SCD2 src_exprn_dt merge failed for {target_table}: {e}") from e
        else:
            logger.info("[DeltaWriter] SCD2 STEP 1b — Skipped (SRC_EXPRN_DT not in target schema)")
        # STEP 2: insert new + changed versions
        # Within-batch SCD2 (rank_extra present): anti-join ALL rows on extended key to avoid re-inserting
        # expired revisions already in target. LD_END_DT preserved from df (set by executor).
        # BTCH_DT excluded from insert key — same logical record can appear in later silver batches
        # with a different BTCH_DT, which must NOT be treated as a new row.
        if _rank_extra:
            _insert_extra = [c for c in _rank_extra if c.upper() not in {"BTCH_DT", "BTCH_ID"}]
            _insert_key = list(primary_keys) + _insert_extra
            # Compare against ACTIVE records only to avoid matching expired revisions from earlier batches.
            # Ranking filter (lines ~627-640) ensures only ONE version per PK is inserted.
            _tgt_for_insert = spark.table(target_table)
        else:
            _insert_key = _row_key
            # Compare against ACTIVE target records only (LD_END_DT IS NULL) to insert new versions
            # when source data changes. Expired records must not block new inserts.
            # (Within-batch: _rank_extra path handles historical REF_ID revisions via ranking)
            _tgt_for_insert = spark.table(target_table)

        # Apply partition pruning: read only the source batch's partition values from target
        if effective_physical_partition_by:
            partition_filter = DeltaWriter._build_partition_filter_predicate(
                df,
                effective_physical_partition_by,
            )
            if partition_filter:
                logger.info(
                    "[DeltaWriter] SCD2 STEP 2 — Reading target table %s with partition pruning on %s",
                    target_table,
                    effective_physical_partition_by,
                )
                _tgt_for_insert = spark.sql(f"SELECT * FROM {target_table} WHERE {partition_filter}")
            else:
                logger.info(
                    "[DeltaWriter] SCD2 STEP 2 — Partition pruning filter could not be built; reading full table for %s",
                    target_table,
                )
        else:
            logger.info(
                "[DeltaWriter] SCD2 STEP 2 — Physical partition pruning disabled; reading full table for %s",
                target_table,
            )

        _tgt_for_insert = DeltaWriter._scope_target_df_to_source_partitions(
            source_df=df,
            target_df=_tgt_for_insert,
            physical_partition_by=effective_physical_partition_by,
            target_table=target_table,
            op_name="SCD2 STEP 2 insert scope",
        )
        
        logger.info("[DeltaWriter] SCD2 STEP 2 — Comparing source records against ACTIVE target records only (LD_END_DT IS NULL)")
        
        # Alias-qualified join keys: FGAC-protected targets are re-analysed by name, where
        # DataFrame-bound columns become unresolvable/ambiguous (both sides share names).
        _src_for_insert = df.alias("n")
        _tgt_for_insert = _tgt_for_insert.alias("x")
        
        # Build join on ALL business columns (excluding technical_cols and rank_extra)
        # to detect ANY change in non-technical data, enabling SCD2 new versions
        # This comparison is done against ALL target records to prevent re-inserting
        # historical revisions (e.g., REF_ID revisions) that already exist
        technical_cols = {"LD_DT", "LD_END_DT", "BTCH_ID", "BTCH_DT", "SRC_EXPRN_DT", "REF_ID", "ERR_CD", "ERR_FLG", "REC_SRC_NM", "PART_COL"}
        rank_extra_upper = {col.upper() for col in _rank_extra}
        logger.debug(f"***************rank_extra_upper***************: {rank_extra_upper}")
        join_cols = [
            c for c in df.columns 
            if c.upper() not in technical_cols and c.upper()
        ]
        join_cols = DeltaWriter._append_shared_physical_partition_columns(
            join_cols,
            df.columns,
            _tgt_for_insert.columns,
            effective_physical_partition_by,
            target_table=target_table,
            op_name="SCD2 STEP 2 anti-join",
        )
        logger.info("[DeltaWriter] SCD2 STEP 2 — Anti-join on %d business/physical partition columns vs scoped target records: %s", len(join_cols), ", ".join(join_cols))
        
        join_exprs = [F.col(f"n.`{k}`").eqNullSafe(F.col(f"x.`{k}`")) for k in join_cols]
        # print("--------_src_for_insert--------")
        # _src_for_insert.show(truncate = False)

        # print("--------_tgt_for_insert--------")
        # _tgt_for_insert.show(truncate = False)
        to_insert   = _src_for_insert.join(_tgt_for_insert, join_exprs, "left_anti")
        
        # Apply ranking to keep only ONE active version per primary key when multiple revisions exist
        # (e.g., multiple REF_IDs in same batch). Keep only rank=1 for INSERT to ensure SCD2 semantics.
        # if rank_order_cols:
        #     logger.info("[DeltaWriter] SCD2 STEP 2 — Applying rank_order_cols to keep ONE active version per primary key")
        #     _rank_win = Window.partitionBy(*primary_keys).orderBy(*_rank_exprs)
        #     # to_insert = (
        #     #     to_insert.withColumn("__rank__", F.row_number().over(_rank_win))
        #     #               .filter(F.col("__rank__") == 1)
        #     #               .drop("__rank__")
        #     # )
        #     logger.info("[DeltaWriter] SCD2 STEP 2 — Rank filtering applied, keeping highest-ranked revision per PK")
                
        insert_count_before_transform = to_insert.count()
        logger.info("[DeltaWriter] SCD2 STEP 2 — Records to insert after anti-join (new/changed): %d", insert_count_before_transform)
        
        to_insert = to_insert.withColumn("LD_DT", F.current_timestamp())
        logger.info("[DeltaWriter] SCD2 STEP 2 — Set LD_DT=current_timestamp and LD_END_DT=NULL for %d records", insert_count_before_transform)
        
        to_insert = DeltaWriter._align_df_to_target_schema(
            spark=spark,
            df=to_insert,
            target_table=target_table,
            table_exists=True,
        )
        logger.info("[DeltaWriter] SCD2 STEP 2 — Schema alignment complete, ready to insert")
        effective_insert_partition_by = DeltaWriter._resolve_write_partition_by(
            to_insert.columns,
            partition_by,
            effective_physical_partition_by,
        )
        insert_writer = to_insert.write.format("delta").mode("append")
        if effective_insert_partition_by:
            existing_insert_partition_by = DeltaWriter._normalize_partition_columns(
                to_insert.columns,
                DeltaWriter._target_partition_columns(spark, target_table),
            )
            if [c.lower() for c in existing_insert_partition_by] == [c.lower() for c in effective_insert_partition_by]:
                logger.info(
                    "[DeltaWriter] SCD2 STEP 2 — Applying partitionBy for %s: %s",
                    target_table,
                    existing_insert_partition_by,
                )
                insert_writer = insert_writer.partitionBy(*existing_insert_partition_by)
            else:
                logger.info(
                    "[DeltaWriter] SCD2 STEP 2 — Skipping partitionBy on existing table %s. existing=%s requested=%s",
                    target_table,
                    existing_insert_partition_by,
                    effective_insert_partition_by,
                )
        # SERVERLESS-SAFE: single write; inserted count comes from the write's
        # metrics (no persist, no pre-count that would recompute the join).
        try:
            logger.info("[DeltaWriter] SCD2 STEP 2 — Writing %d new/changed records to %s", insert_count_before_transform, target_table)
            DeltaWriter._run_with_concurrency_retry(
                lambda: insert_writer.saveAsTable(target_table),
                target_table=target_table,
                op_name="SCD2 STEP 2 insert",
            )
            logger.info("[DeltaWriter] SCD2 STEP 2 — Write completed successfully")
        except Exception as e:
            logger.error("[DeltaWriter] SCD2 STEP 2 — Write failed: %s", str(e))
            raise RuntimeError(f"[DeltaWriter] SCD2 insert failed for {target_table}: {e}") from e
        ins_count = DeltaWriter._written_rows(spark, target_table)
        logger.info("[DeltaWriter] SCD2 STEP 2 — Total records in target table after insert: %d", ins_count)

        return {
            "rows_inserted": insert_count_before_transform, "rows_updated": 0, "rows_affected": insert_count_before_transform,
            "new_watermark_ts": DeltaWriter._max_watermark(spark, target_table, watermark_col),
        }

    # -------------------------------------------------------------------
    # OPTIMIZE + ZORDER / VACUUM  (SQL — run from a maintenance job)
    # -------------------------------------------------------------------
    @staticmethod
    def optimize_zorder(spark, target_table: str, zorder_cols: List[str]):
        cols = ", ".join(zorder_cols)
        logger.info("[DeltaWriter] OPTIMIZE %s ZORDER BY (%s)", target_table, cols)
        spark.sql(f"OPTIMIZE {target_table} ZORDER BY ({cols})")

    @staticmethod
    def vacuum(spark, target_table: str, retention_hours: int = 168):
        logger.info("[DeltaWriter] VACUUM %s RETAIN %d HOURS", target_table, retention_hours)
        spark.sql(f"VACUUM {target_table} RETAIN {retention_hours} HOURS")

    # -------------------------------------------------------------------
    # Helpers — all SQL / metadata based (serverless + FGAC safe)
    # -------------------------------------------------------------------
    @staticmethod
    def _table_exists(spark, fqn: str) -> bool:
        """FGAC-safe existence check via SHOW TABLES (not spark.catalog.tableExists)."""
        try:
            parts = fqn.replace("`", "").split(".")
            if len(parts) == 3:
                cat, sch, tbl = parts
                ns = f"`{cat}`.`{sch}`"
            elif len(parts) == 2:
                sch, tbl = parts
                ns = f"`{sch}`"
            else:
                tbl = parts[-1]; ns = None
            safe = tbl.replace("'", "''")
            q = f"SHOW TABLES IN {ns} LIKE '{safe}'" if ns else f"SHOW TABLES LIKE '{safe}'"
            return spark.sql(q).count() > 0
        except Exception as exc:
            logger.warning("[DeltaWriter] _table_exists check failed for %s: %s", fqn, exc)
            return False

    @staticmethod
    def _op_metrics(spark, fqn: str) -> dict:
        """Most-recent operationMetrics via DESCRIBE HISTORY (SQL — FGAC L7 safe)."""
        try:
            rows = spark.sql(f"DESCRIBE HISTORY {fqn} LIMIT 1").select("operationMetrics").collect()
            if rows and rows[0][0]:
                return dict(rows[0][0])
        except Exception as exc:
            logger.warning("[DeltaWriter] Could not read operationMetrics for %s: %s", fqn, exc)
        return {}

    @staticmethod
    def _written_rows(spark, fqn: str) -> int:
        m = DeltaWriter._op_metrics(spark, fqn)
        for k in ("numOutputRows", "numTargetRowsInserted"):
            if k in m:
                try:
                    return int(m[k])
                except Exception:
                    pass
        return 0

    @staticmethod
    def _read_merge_metrics(spark, target_table: str) -> dict:
        m = DeltaWriter._op_metrics(spark, target_table)
        return {
            "numTargetRowsInserted": int(m.get("numTargetRowsInserted", 0) or 0),
            "numTargetRowsUpdated":  int(m.get("numTargetRowsUpdated",  0) or 0),
        }

    @staticmethod
    def _max_watermark(spark, target_table: str, watermark_col: Optional[str]):
        """
        Max watermark read from the COMMITTED target table via SQL — NOT from the
        source df (which would trigger a second full recompute of the pipeline).
        Serverless-safe and computes the pipeline only once.
        """
        if not watermark_col:
            return None
        try:
            cols = {c.lower() for c in spark.table(target_table).columns}
            if watermark_col.lower() not in cols:
                return None
            row = spark.sql(f"SELECT MAX(`{watermark_col}`) AS mx FROM {target_table}").collect()
            if row and row[0]["mx"] is not None:
                val = row[0]["mx"]  
                logger.info(f"*******Original Watermark: {val}*********")
                val = val if isinstance(val, datetime) else datetime.fromisoformat(str(val))
                if val is not None:
                    val = val.date()
                    if val > datetime.now(ZoneInfo("America/Chicago")).date():
                        logger.info(f"*******IF Watermark: {val}*********")
                        val = datetime.now(ZoneInfo("America/Chicago")).date()
                logger.info(f"*******Latest Watermark: {val}*********")
                return val
        except Exception as exc:
            logger.warning("[DeltaWriter] Could not compute max watermark '%s' on %s: %s",
                           watermark_col, target_table, exc)
        return None

    @staticmethod
    def _is_table_already_exists_error(exc: Exception) -> bool:
        msg = str(exc)
        return ("TABLE_OR_VIEW_ALREADY_EXISTS" in msg
                or "TableAlreadyExistsException" in msg
                or "already exists" in msg.lower())
