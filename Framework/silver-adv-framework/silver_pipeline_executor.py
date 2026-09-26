# =====================================================================
# framework/pipeline_executor.py   (MERGED Silver + Gold executor, v3.0)
#
# ONE executor now serves BOTH layers. The only thing that ever differed
# between the Silver and Gold executors was the WRITE TARGET SCHEMA:
#       Silver:  settings.silver_fq.<target_table>
#       Gold:    settings.gold_fq.<target_table>
#
# That choice is now a single `layer` argument on execute() (default 'silver').
# Everything else — source loading, join, rule engine, ctl_table_schema
# column projection, primary-key de-duplication, and the SCD1/SCD2 MERGE in
# DeltaWriter — is byte-for-byte the SAME logic as before. No behaviour change.
#
# Backwards compatibility: `GoldPipelineExecutor` is kept as a thin subclass
# that calls execute(..., layer='gold'), so existing Gold notebook imports
# (from framework.gold_pipeline_executor import GoldPipelineExecutor) and the
# merged import (from framework.pipeline_executor import GoldPipelineExecutor)
# both work unchanged.
# =====================================================================
# =====================================================================
# framework/pipeline_executor.py
# =====================================================================

import ast
import contextlib
import hashlib
import io
import json
import logging
import os
import re
import threading
import time
from pprint import pformat

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F

from framework.rule_engine import RuleEngine
from framework.delta_writer import DeltaWriter
from framework.source_connector import ConnectorRegistry
try:
    from framework.run_log import RunLogger
except ImportError:
    RunLogger = None

logger = logging.getLogger(__name__)
logging.getLogger("py4j").setLevel(logging.ERROR)


def _bool_env(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, str(default))).strip().lower()
    return raw in {"1", "true", "t", "yes", "y"}


def _debug_enabled() -> bool:
    return logger.isEnabledFor(logging.DEBUG)


def _metrics_enabled() -> bool:
    return _debug_enabled() and _bool_env("LOG_DATAFRAME_METRICS", False)


def _query_plan_enabled() -> bool:
    return _debug_enabled() and _bool_env("LOG_QUERY_PLAN", False)


def _schema_enabled() -> bool:
    return _debug_enabled() and _bool_env("LOG_SCHEMA", True)


def _expr_enabled() -> bool:
    return _debug_enabled() and _bool_env("LOG_EXPRESSIONS", True)


def _window_enabled() -> bool:
    return _debug_enabled() and _bool_env("LOG_WINDOW_FUNCTIONS", True)


def _primary_keys_enabled() -> bool:
    return _debug_enabled() and _bool_env("LOG_PRIMARY_KEYS", True)


def _dataframe_sample_enabled() -> bool:
    return _debug_enabled() and _bool_env("LOG_DATAFRAME_SAMPLE", False)


def _dataframe_sample_rows() -> int:
    raw_value = str(os.getenv("LOG_DATAFRAME_SAMPLE_ROWS", "10")).strip()
    try:
        parsed_value = int(raw_value)
    except ValueError:
        return 10
    return max(1, parsed_value)


def _fmt_vars(**kwargs) -> str:
    return pformat({k: kwargs[k] for k in sorted(kwargs)}, compact=True)


def _log_dataframe_sample(stage: str, df: DataFrame, pid: str = "") -> None:
    if not _dataframe_sample_enabled():
        return
    sample_rows = _dataframe_sample_rows()
    logger.debug(
        "[PipelineExecutor] DATAFRAME_SAMPLE_START %s",
        _fmt_vars(stage=stage, pipeline_id=pid, rows=sample_rows),
    )
    try:
        df.show(sample_rows, truncate=False)
    except Exception as exc:
        logger.warning(
            "[PipelineExecutor] DATAFRAME_SAMPLE_FAILED %s",
            _fmt_vars(stage=stage, pipeline_id=pid, rows=sample_rows, error=str(exc)[:300]),
        )
        return
    logger.debug(
        "[PipelineExecutor] DATAFRAME_SAMPLE_END %s",
        _fmt_vars(stage=stage, pipeline_id=pid, rows=sample_rows),
    )


def _log_schema(stage: str, df: DataFrame, pid: str = "") -> None:
    if not _schema_enabled():
        return
    schema_lines = [f"{field.name}: {field.dataType} nullable={field.nullable}" for field in df.schema.fields]
    logger.debug("[PipelineExecutor] SCHEMA %s", _fmt_vars(stage=stage, pipeline_id=pid, schema=schema_lines))


def _log_query_plan(stage: str, df: DataFrame, pid: str = "") -> None:
    if not _query_plan_enabled():
        return
    try:
        plan_buffer = io.StringIO()
        with contextlib.redirect_stdout(plan_buffer):
            df.explain(mode="extended")
        plan_text = plan_buffer.getvalue().strip()
        if not plan_text:
            logger.warning(
                "[PipelineExecutor] QUERY_PLAN_SKIPPED %s",
                _fmt_vars(stage=stage, pipeline_id=pid, error="explain() returned no output"),
            )
            return
        logger.debug(
            "[PipelineExecutor] QUERY_PLAN %s",
            _fmt_vars(stage=stage, pipeline_id=pid, plan=plan_text),
        )
    except Exception as exc:
        logger.warning("[PipelineExecutor] QUERY_PLAN_SKIPPED %s", _fmt_vars(stage=stage, pipeline_id=pid, error=str(exc)[:300]))


def _log_expression(stage: str, pid: str, output_column: str, expression: str, source_columns=None, alias: str = None, cast_datatype: str = None) -> None:
    if not _expr_enabled():
        return
    logger.debug(
        "[PipelineExecutor] EXPRESSION %s",
        _fmt_vars(
            stage=stage,
            pipeline_id=pid,
            output_column=output_column,
            source_columns=list(source_columns or []),
            expression=expression,
            alias=alias,
            cast_datatype=cast_datatype,
        ),
    )


def _log_window(stage: str, pid: str, output_column: str, function_name: str, partition_by, order_by, frame: str = None, conditions: str = None) -> None:
    if not _window_enabled():
        return
    logger.debug(
        "[PipelineExecutor] WINDOW %s",
        _fmt_vars(
            stage=stage,
            pipeline_id=pid,
            output_column=output_column,
            function=function_name,
            partition_by=list(partition_by or []),
            order_by=list(order_by or []),
            frame=frame,
            conditions=conditions,
        ),
    )

# -----------------------------------------------------------------------
# Thread-safe source cache
# -----------------------------------------------------------------------
_SOURCE_CACHE: dict = {}
_CACHE_LOCK = threading.Lock()

# Track tables that already consumed FULL per run key (run_id/batch_id).
# This is module-scoped so behavior remains stable even if caller recreates
# executor instances or passes a fresh context dict per pipeline.
_FULL_LOAD_TRACKER: dict = {}
_FULL_LOAD_LOCK = threading.Lock()


def _get_cached(key):
    with _CACHE_LOCK:
        return _SOURCE_CACHE.get(key)


def _put_cached(key, df):
    with _CACHE_LOCK:
        _SOURCE_CACHE[key] = df


def clear_source_cache():
    with _CACHE_LOCK:
        for df in _SOURCE_CACHE.values():
            try:
                df.unpersist()
            except Exception:
                pass
        _SOURCE_CACHE.clear()
    logger.debug("[PipelineExecutor] Source cache cleared.")


class PipelineExecutor:
    def __init__(self, spark: SparkSession, settings):
        self.spark = spark
        self.settings = settings

    def _log_count(self, stage: str, df: DataFrame, pid: str = "") -> None:
        """Diagnostic row count after a transformation stage — triggers a Spark job only when explicitly enabled."""
        if _metrics_enabled():
            try:
                logger.debug("[PipelineExecutor] DATAFRAME_METRICS %s", _fmt_vars(stage=stage, pipeline_id=pid, record_count=df.count()))
            except Exception as exc:
                logger.warning("[PipelineExecutor] DATAFRAME_METRICS_FAILED %s", _fmt_vars(stage=stage, pipeline_id=pid, error=str(exc)[:300]))
        _log_dataframe_sample(stage, df, pid)

    def _align_to_existing_table_schema(self, df: DataFrame, target_table: str, keep_extra_cols=None) -> DataFrame:
        """
        If target table already exists, cast/select columns to that physical schema.
        This avoids Delta merge failures when ctl_table_schema drifts from the actual
        Delta table schema (common during iterative model updates).
        
        Also sanitizes string/varchar columns by removing CR/LF/TAB and trimming
        leading/trailing spaces, preventing Delta constraint violations.

        keep_extra_cols: optional columns (e.g. scd2_rank_order_cols not present in the
        physical table, like MQP_DATE_MODIFIED) to carry through unaligned so later
        in-batch ranking/dedup logic can still use them. Caller must drop them before write.
        """
        logger.info("[PipelineExecutor] Checking existing table schema for %s", target_table)
        if not DeltaWriter._table_exists(self.spark, target_table):
            logger.info("[PipelineExecutor] Target table %s does not exist yet; skipping alignment", target_table)
            return df

        target_schema = self.spark.table(target_table).schema
        df_cols_lower = {c.lower(): c for c in df.columns}  # Case-insensitive mapping
        aligned_cols = []
        target_names_lower = {field.name.lower() for field in target_schema.fields}

        for field in target_schema.fields:
            col_expr = None
            # Case-insensitive column resolution
            actual_col = df_cols_lower.get(field.name.lower())
            if actual_col:
                col_expr = F.col(actual_col).cast(field.dataType)
            else:
                col_expr = F.lit(None).cast(field.dataType)
            
            # Remove CR/LF/TAB then trim to clean hidden whitespace/newlines.
            dtype_str = str(field.dataType).upper()
            if any(t in dtype_str for t in ["VARCHAR", "CHAR", "STRING"]):
                col_expr = F.trim(F.regexp_replace(col_expr, r"[\r\n\t]", ""))
            
            aligned_cols.append(col_expr.alias(field.name))

        for _extra in (keep_extra_cols or []):
            _actual = df_cols_lower.get(_extra.lower())
            if _actual and _extra.lower() not in target_names_lower:
                aligned_cols.append(F.col(_actual).alias(_actual))

        logger.info("[PipelineExecutor] Aligning DataFrame schema to existing table: %s", target_table)
        return df.select(*aligned_cols)

    def _resolve_effective_load_type(self, cfg, context, requested_load_type):
        """
        Determine load mode for this pipeline execution.

                FULL is always handled at TABLE level (never at pipeline level):
                - First pipeline for a table in a run uses FULL
                - Remaining pipelines for the same table automatically use INCREMENTAL

                Optional scoping:
                    - full_load_tables = "T1,T2" or ["T1", "T2"]
                        If provided, FULL is restricted to those tables only.
        """
        context = context or {}
        effective = (requested_load_type or cfg.get("load_type_default") or "incremental").strip()

        if effective.lower() != "full":
            return effective

        target_table = (cfg.get("target_table") or "").strip()
        if not target_table:
            return effective

        raw_tables = context.get("full_load_tables")
        normalized_tables = None
        if raw_tables:
            if isinstance(raw_tables, str):
                normalized_tables = {x.strip().upper() for x in raw_tables.split(",") if x.strip()}
            elif isinstance(raw_tables, (list, tuple, set)):
                normalized_tables = {str(x).strip().upper() for x in raw_tables if str(x).strip()}

        if normalized_tables and target_table.upper() not in normalized_tables:
            return "incremental"

        # Use a stable batch-scoped key first. In this project, run_id is often
        # unique per pipeline execution, which would incorrectly re-enable FULL
        # for each pipeline targeting the same table.
        run_key_source = (
            (context or {}).get("full_load_run_key")
            or (context or {}).get("batch_id")
            or (context or {}).get("main_batch_id")
            or (context or {}).get("job_id")
            or "__default__"
        )
        run_key = str(run_key_source)

        if run_key == "__default__":
            logger.warning(
                "[PipelineExecutor] No batch/job key found in context. Using shared FULL tracker key '__default__'."
            )

        with _FULL_LOAD_LOCK:
            done_tables = _FULL_LOAD_TRACKER.setdefault(run_key, set())
            already_done = target_table.upper() in done_tables
            if not already_done:
                done_tables.add(target_table.upper())

            # Safety cap to avoid unbounded growth in long-lived drivers.
            if len(_FULL_LOAD_TRACKER) > 1000:
                _FULL_LOAD_TRACKER.clear()

        if already_done:
            logger.info(
                "[PipelineExecutor] FULL already applied for table %s in run %s; switching pipeline %s to INCREMENTAL",
                target_table,
                run_key,
                cfg.get("pipeline_id"),
            )
            return "incremental"

        logger.info(
            "[PipelineExecutor] Applying TABLE-LEVEL FULL load for table %s in run %s via pipeline %s",
            target_table,
            run_key,
            cfg.get("pipeline_id"),
        )
        return "full"

    # ================================================================
    # Schema
    # ================================================================
    def _get_target_schema(self, cfg):
        table = cfg["target_table"]

        try:
            rows = self.spark.sql(f"""
            SELECT columns_json
            FROM {self.settings.control_fq}.ctl_table_schema
            WHERE table_name = '{table}'
        """).collect()
        except Exception as e:
            raise RuntimeError(f"[PipelineExecutor] Failed to load ctl_table_schema for {table}: {e}") from e

        if not rows:
            return None

        raw_json = rows[0]["columns_json"]

        try:
            parsed = json.loads(raw_json)
        except Exception:
            parsed = ast.literal_eval(raw_json)

        # ✅ normalize top-level keys
        parsed = {k.lower(): v for k, v in parsed.items()}

        if "columns" not in parsed:
            raise ValueError(f"Invalid columns_json for {table}")

        normalized_cols = []

        for c in parsed["columns"]:
            # ✅ normalize each column dict
            c_norm = {k.lower(): v for k, v in c.items()}

            name = c_norm.get("name")
            dtype = c_norm.get("type")

            if not name or not dtype:
                raise ValueError(f"Invalid column entry in {table}: {c}")

            # ✅ enforce consistent casing (important!)
            normalized_cols.append((name.upper(), dtype.upper()))

        # ✅ return dict
        return dict(normalized_cols)

    # ================================================================
    # Column Mapping
    # ================================================================
    def _apply_column_mapping(self, df, cfg, use_ctl_schema: bool = True, keep_extra_cols=None):
        mapped = {c["target_column"]: c["source_expr"] for c in cfg["column_map"]}
        if _expr_enabled():
            logger.debug("[PipelineExecutor] COLUMN_MAPPING %s", _fmt_vars(pipeline_id=cfg.get("pipeline_id"), mapped_columns=mapped))
        # Helper: sanitize string expressions by removing CR/LF/TAB then trimming.
        # This prevents char/varchar length violations from hidden newline chars.
        def _maybe_trim_expr(expr_obj, inferred_is_string: bool = True) -> str:
            """Wrap expr with REGEXP_REPLACE + TRIM if it is treated as string."""
            if inferred_is_string:
                return f"TRIM(REGEXP_REPLACE({str(expr_obj)}, '[\\r\\n\\t]', ''))"
            return str(expr_obj)
        logger.debug("[PipelineExecutor] COLUMN_MAPPING_MODE %s", _fmt_vars(pipeline_id=cfg.get("pipeline_id"), use_ctl_schema=use_ctl_schema))
        if not use_ctl_schema:
            # Apply only explicit column_map expressions; rely on existing target
            # table schema alignment at write time for final projection/types.
            for target_col, source_expr in mapped.items():
                expr = source_expr

                expr_str = str(expr)
                alias_col_match = re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.([A-Za-z_][A-Za-z0-9_]*)", expr_str)
                if alias_col_match:
                    bare_col = alias_col_match.group(1)
                    if bare_col in df.columns:
                        expr = bare_col

                if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(expr)) and str(expr) not in df.columns:
                    df = df.withColumn(target_col, F.lit(None))
                else:
                    # Apply TRIM to all string expressions as a safe default (most source data is strings)
                    trimmed_expr = _maybe_trim_expr(expr, inferred_is_string=True)
                    df = df.withColumn(target_col, F.expr(trimmed_expr))

            return df

        target_schema = self._get_target_schema(cfg)
        if target_schema is None:
            return df

        select_exprs = []

        # Helper: sanitize string expressions by removing CR/LF/TAB then trimming.
        def _maybe_trim(expr_str: str, dtype: str) -> str:
            dtype_upper = dtype.upper()
            is_string_type = any(t in dtype_upper for t in ["VARCHAR", "CHAR", "STRING"])
            if is_string_type:
                return f"TRIM(REGEXP_REPLACE({expr_str}, '[\\r\\n\\t]', ''))"
            return expr_str

        for col_name, dtype in target_schema.items():
            if col_name in mapped:
                expr = mapped[col_name]

                # Support alias-qualified mapping expressions coming from ctl_column_map
                # (e.g. HUB.QBE_HASH_PRTY_ID). After joins we project plain column names,
                # so rewrite to bare column if available.
                expr_str = str(expr)
                alias_col_match = re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.([A-Za-z_][A-Za-z0-9_]*)", expr_str)
                if alias_col_match:
                    bare_col = alias_col_match.group(1)
                    if bare_col in df.columns:
                        expr = bare_col

                if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(expr)) and expr not in df.columns:
                    select_exprs.append(f"CAST(NULL AS {dtype}) AS {col_name}")
                else:
                    expr_str_trimmed = _maybe_trim(str(expr), dtype)
                    select_exprs.append(f"CAST({expr_str_trimmed} AS {dtype}) AS {col_name}")

            elif col_name in df.columns:
                col_str_trimmed = _maybe_trim(col_name, dtype)
                select_exprs.append(f"CAST({col_str_trimmed} AS {dtype}) AS {col_name}")
            else:
                select_exprs.append(f"CAST(NULL AS {dtype}) AS {col_name}")

        # Carry through columns not in the physical schema (e.g. MQP_DATE_MODIFIED)
        # so later in-batch SCD2 ranking can use them; caller drops them before write.
        target_names_lower = {c.lower() for c in target_schema}
        df_cols_ci = {c.lower(): c for c in df.columns}
        for extra in (keep_extra_cols or []):
            if extra.lower() in target_names_lower:
                continue
            actual = df_cols_ci.get(extra.lower())
            if actual:
                select_exprs.append(f"`{actual}` AS `{actual}`")

        return df.selectExpr(*select_exprs)

    # ================================================================
    # BUILD DF
    # ================================================================
    def build_df(self, cfg, context=None, use_ctl_schema: bool = True):
        context = context or {}
        dfs = {}

        # ============================================================
        # LOAD SOURCES
        # ============================================================
        for src in cfg["sources"]:
            alias = src["alias"]

            source_type = (src.get("source_type") or "delta").lower()
            connector = ConnectorRegistry.get(source_type)

            source_ref = src["table"]

            if "." not in source_ref:
                if source_type == "silver":
                    source_ref = f"{self.settings.silver_fq}.{source_ref}"
                elif source_type == "gold":
                    source_ref = f"{self.settings.gold_fq}.{source_ref}"
                else:  # bronze, delta, majesco, or any unrecognised type
                    source_ref = f"{self.settings.bronze_fq}.{source_ref}"

            # OPTIMIZATION: cache by resolved table ref + pre-rule signature (NOT by
            # pipeline_id). The 6 LNK sub-pipelines share the same hub/link lookups
            # (hub_plcy, hub_prty, hub_cntct_pnt, lnk_plcy_prty_role); this reads &
            # persists each shared table ONCE instead of once per sub-pipeline.
            _pre_sig = src.get("pre_rules_json") or src.get("rules") or "[]"
            key = f"{source_ref}::{hashlib.md5(str(_pre_sig).encode()).hexdigest()}"

            cached = _get_cached(key)
            if cached is not None:
                logger.info(
                    "[PipelineExecutor] SOURCE_CACHE_HIT %s",
                    _fmt_vars(
                        pipeline_id=cfg.get("pipeline_id"),
                        alias=alias,
                        source=source_ref,
                        source_type=source_type,
                        load_type=context.get("load_type"),
                    ),
                )
                dfs[alias] = cached.alias(alias)
                continue

            logger.debug(
                "[PipelineExecutor] SOURCE_CACHE_MISS %s",
                _fmt_vars(
                    pipeline_id=cfg.get("pipeline_id"),
                    alias=alias,
                    source=source_ref,
                    source_type=source_type,
                    load_type=context.get("load_type"),
                ),
            )
            logger.info("[PipelineExecutor] STAGE_START %s", _fmt_vars(stage="load_source", pipeline_id=cfg.get("pipeline_id"), alias=alias, source=source_ref, load_type=context.get("load_type")))
            source_stage_start = time.time()

            try:
                df = connector.read(
                    self.spark,
                    source_ref,
                    product=context.get("products"),
                    product_filter_col=cfg.get("product_filter_col"),
                    is_common=cfg.get("is_common", False),
                    watermark_col=cfg.get("watermark_col"),
                    load_type=context.get("load_type"),
                    start_date=context.get("start_date"),
                    end_date=context.get("end_date"),
                    watermark_ts=context.get("watermark_ts"),
                    # PQB filter: only applied to sources that carry MQP_PQB_FLAG (i.e. MIS_QUOTE_POLICIES).
                    # pqb_flag_col comes from ctl_pipeline; pqb_flags comes from the widget via context.
                    pqb_flag_col=cfg.get("pqb_flag_col"),
                    pqb_flags=context.get("pqb_flags"),
                    debug_filter_column=context.get("debug_filter_column"),
                    debug_filter_value=context.get("debug_filter_value"),
                    num_of_days_for_cmn_tables=context.get("num_of_days_for_cmn_tables"),
                )
            except Exception as e:
                raise RuntimeError(f"[PipelineExecutor] Failed to read source '{alias}' ({source_ref}): {e}") from e
            logger.info("[PipelineExecutor] STAGE_END %s", _fmt_vars(stage="load_source", pipeline_id=cfg.get("pipeline_id"), alias=alias, source=source_ref, stage_duration_seconds=round(time.time() - source_stage_start, 2)))
            _log_schema(f"source_load:{alias}", df, cfg.get("pipeline_id"))
            _log_query_plan(f"source_load:{alias}", df, cfg.get("pipeline_id"))

            # Soft-delete filter: only applies when the source physically carries DATE_DELETED.
            _date_deleted_col = next((c for c in df.columns if c.lower() == "date_deleted"), None)
            if _date_deleted_col:
                df = df.filter(F.col(_date_deleted_col).isNull())
                logger.info("[PipelineExecutor] DATE_DELETED filter applied for source '%s' (%s)", alias, source_ref)
                self._log_count(f"After DATE_DELETED filter ({alias})", df, cfg.get("pipeline_id"))
                
            # Apply pre_rules_json — runs per-source BEFORE the join.
            # Used to derive join keys (e.g. PRTY_ID) or filter a gold lookup
            # (e.g. HUB source filtered to PRTY_TP_CD = 'Person') so that
            # the join step can reference the derived column by name.
            # Registry normalizes ctl_source.pre_rules_json to src['rules'].
            # Keep backward compatibility with src['pre_rules_json'] if present.
            self._log_count(f"Source raw load ({alias})", df, cfg.get("pipeline_id"))
            pre_rules_raw = src.get("pre_rules_json")
            if pre_rules_raw is None:
                pre_rules_raw = src.get("rules")
            if not pre_rules_raw:
                pre_rules_raw = "[]"
            if isinstance(pre_rules_raw, str):
                try:
                    pre_rules = json.loads(pre_rules_raw)
                except (TypeError, ValueError) as e:
                    raise ValueError(f"[PipelineExecutor] Invalid pre_rules_json for source '{alias}': {e}") from e
            else:
                pre_rules = list(pre_rules_raw) if pre_rules_raw else []
            if pre_rules:
                normalized_pre_rules = []
                for rule_def in pre_rules:
                    if "rule" in rule_def:
                        normalized_pre_rules.append({
                            "rule": rule_def.get("rule"),
                            "params": rule_def.get("params", {}) or {},
                        })
                        continue

                    params = rule_def.get("params_json", {}) or {}
                    if isinstance(params, str):
                        try:
                            params = json.loads(params)
                        except (TypeError, ValueError) as e:
                            raise ValueError(f"[PipelineExecutor] Invalid params_json for source '{alias}': {e}") from e

                    normalized_pre_rules.append({
                        "rule": rule_def.get("rule_name"),
                        "params": params,
                    })

                # Filter out rules with None/empty rule name
                normalized_pre_rules = [r for r in normalized_pre_rules if r.get("rule")]

                if normalized_pre_rules:
                    logger.info("[PipelineExecutor] STAGE_START %s", _fmt_vars(stage="pre_rules", pipeline_id=cfg.get("pipeline_id"), alias=alias, rule_count=len(normalized_pre_rules)))
                    pre_rule_stage_start = time.time()
                    engine = RuleEngine()
                    try:
                        df = engine.apply_rules(df, normalized_pre_rules)
                    except Exception as e:
                        raise RuntimeError(f"[PipelineExecutor] Pre-rule failed on source '{alias}': {e}") from e
                    logger.info(
                        "[PipelineExecutor] STAGE_END %s",
                        _fmt_vars(
                            stage="pre_rules",
                            pipeline_id=cfg.get("pipeline_id"),
                            alias=alias,
                            rule_count=len(normalized_pre_rules),
                            stage_duration_seconds=round(time.time() - pre_rule_stage_start, 2),
                        ),
                    )
                else:
                    logger.debug(
                        "[PipelineExecutor] PRE_RULES_SKIPPED %s",
                        _fmt_vars(pipeline_id=cfg.get("pipeline_id"), alias=alias, reason="no_pre_rules"),
                    )

            df = df.alias(alias)
            self._log_count(f"Source after pre-rules ({alias})", df, cfg.get("pipeline_id"))
            # SERVERLESS-SAFE: no .persist()/.cache() (unsupported on serverless).
            # Keep a lazy plan reference so shared lookups are declared once; Delta
            # scan caching + broadcast joins handle reuse. For very heavy reuse,
            # materialise the lookup to a Delta scratch table instead of caching.
            _put_cached(key, df)
            dfs[alias] = df

        # ============================================================
        # ✅ NEW: MULTI-SOURCE HANDLING (JOIN / SINGLE / UNION)
        # ============================================================
        sources = cfg.get("sources", [])
        joins = cfg.get("joins")

        # ✅ CASE 1: JOIN PIPELINE
        if joins:
            logger.info(f"[PipelineExecutor] JOIN pipeline: {cfg['pipeline_id']}")
            logger.debug(
                "[PipelineExecutor] JOIN_BASE %s",
                _fmt_vars(
                    pipeline_id=cfg.get("pipeline_id"),
                    base_source_alias=cfg.get("base_source_alias"),
                    join_count=len(joins_sorted) if 'joins_sorted' in locals() else len(joins),
                ),
            )
            df = dfs[cfg["base_source_alias"]]

            # DRIVER-OOM FIX: do NOT force-broadcast lookups. Forcing broadcast on a
            # lookup that is not actually tiny (e.g. lnk_plcy_prty_role) makes the
            # DRIVER collect and hold that whole table -> driver OOM, especially with
            # several sub-pipelines running at once. Let AQE + autoBroadcastJoinThreshold
            # (set conservatively in the notebook) pick broadcast only for genuinely
            # small tables.
            joins_sorted = sorted(joins, key=lambda x: x["join_seq"])
            logger.info(
                "[PipelineExecutor] JOIN_SEQUENCE %s",
                _fmt_vars(
                    pipeline_id=cfg.get("pipeline_id"),
                    base_source_alias=cfg.get("base_source_alias"),
                    join_sequence=[
                        {
                            "join_seq": j.get("join_seq"),
                            "right_alias": j.get("right_alias"),
                            "join_type": j.get("join_type"),
                        }
                        for j in joins_sorted
                    ],
                ),
            )

            for j in joins_sorted:
                right_alias = j["right_alias"]
                right_df = dfs[right_alias]

                on_pairs = j.get("on_pairs") or [{
                    "left": j["left_col"],
                    "right": j["right_col"],
                }]

                # ✅ case-insensitive column resolution
                left_cols = {c.lower(): c for c in df.columns}
                right_cols = {c.lower(): c for c in right_df.columns}

                # Join with aliases, then project: keep all left columns and only
                # non-overlapping right columns. This guarantees unique column names
                # for downstream derive rules.
                lhs_alias = "__lhs"
                rhs_alias = f"__{right_alias}"

                left_df = df.alias(lhs_alias)
                # No explicit broadcast — AQE decides safely (see driver-OOM fix note above).
                right_df_aliased = right_df.alias(rhs_alias)

                logger.info(
                    "[PipelineExecutor] STAGE_START %s",
                    _fmt_vars(
                        stage="join",
                        pipeline_id=cfg.get("pipeline_id"),
                        right_alias=right_alias,
                        join_type=j.get("join_type"),
                        on_pairs=on_pairs,
                    ),
                )
                join_stage_start = time.time()
                join_condition = None
                for pair in on_pairs:
                    left_col = pair["left"]
                    right_col = pair["right"]

                    if left_col.lower() not in left_cols:
                        raise ValueError(f"Column {left_col} not found in left DF")

                    if right_col.lower() not in right_cols:
                        raise ValueError(f"Column {right_col} not found in right DF")

                    left_join_col = left_cols[left_col.lower()]
                    right_join_col = right_cols[right_col.lower()]

                    pair_condition = (
                        F.col(f"{lhs_alias}.{left_join_col}") ==
                        F.col(f"{rhs_alias}.{right_join_col}")
                    )
                    join_condition = pair_condition if join_condition is None else (join_condition & pair_condition)

                left_col_names = list(df.columns)
                left_col_names_lower = {c.lower() for c in left_col_names}
                right_keep_cols = [c for c in right_df.columns if c.lower() not in left_col_names_lower]

                select_exprs = [F.col(f"{lhs_alias}.{c}").alias(c) for c in left_col_names]
                select_exprs.extend(F.col(f"{rhs_alias}.{c}").alias(c) for c in right_keep_cols)

                if right_keep_cols:
                    logger.info(
                        "[PipelineExecutor] Keeping non-overlapping columns from %s: %s",
                        right_alias,
                        right_keep_cols,
                    )

                logger.debug(
                    "[PipelineExecutor] JOIN_RESOLVED %s",
                    _fmt_vars(
                        pipeline_id=cfg.get("pipeline_id"),
                        right_alias=right_alias,
                        join_type=j.get("join_type"),
                        on_pairs=[
                            {
                                "left": left_cols[pair["left"].lower()],
                                "right": right_cols[pair["right"].lower()],
                            }
                            for pair in on_pairs
                        ],
                        right_keep_cols=right_keep_cols,
                    ),
                )
                df = left_df.join(right_df_aliased, join_condition, j["join_type"].lower()).select(*select_exprs)
                logger.info(
                    "[PipelineExecutor] STAGE_END %s",
                    _fmt_vars(
                        stage="join",
                        pipeline_id=cfg.get("pipeline_id"),
                        right_alias=right_alias,
                        join_type=j.get("join_type"),
                        stage_duration_seconds=round(time.time() - join_stage_start, 2),
                    ),
                )

                self._log_count(f"After join ({right_alias})", df, cfg.get("pipeline_id"))


        # ✅ CASE 2: SINGLE SOURCE
        elif len(sources) == 1:
            logger.info(f"[PipelineExecutor] SINGLE source pipeline: {cfg['pipeline_id']}")
            df = list(dfs.values())[0]
            self._log_count("Single source", df, cfg.get("pipeline_id"))

        # ✅ CASE 3: UNION PIPELINE (NO JOINS)
        else:
            from functools import reduce
            
            if not dfs:
                raise ValueError(
                    f"[PipelineExecutor] Pipeline {cfg['pipeline_id']} has no ctl_source rows "
                    f"configured; cannot build a DataFrame. Check dsi_dev.process_audit_logs.ctl_source."
                )


            logger.info(f"[PipelineExecutor] UNION pipeline: {cfg['pipeline_id']}")

            df = reduce(
                lambda a, b: a.unionByName(b, allowMissingColumns=True),
                dfs.values()
            )
            self._log_count("After union", df, cfg.get("pipeline_id"))

        # LD_DT is a framework-stamped column, never a business value from a joined/
        # unioned source (e.g. HUB.LD_DT) — drop any inherited copy so column mapping
        # and the rule engine can't see or propagate a parent table's LD_DT.
        _ld_dt_leaked = [c for c in df.columns if c.lower() == "ld_dt"]
        if _ld_dt_leaked:
            df = df.drop(*_ld_dt_leaked)
            logger.info("[PipelineExecutor] Dropped inherited LD_DT column(s) %s for %s", _ld_dt_leaked, cfg.get("pipeline_id"))

        # ============================================================
        # RULE ENGINE
        # ============================================================
        if cfg.get("rules"):

            enhanced_rules = []
            for r in cfg["rules"]:
                r_copy = dict(r)

                params = r_copy.get("params", {}) or {}

                params.update({
                    "batch_id": context.get("batch_id"),
                    "run_id": context.get("run_id"),
                })

                r_copy["params"] = params
                enhanced_rules.append(r_copy)

            logger.info("[PipelineExecutor] STAGE_START %s", _fmt_vars(stage="rule_engine", pipeline_id=cfg.get("pipeline_id"), rule_count=len(enhanced_rules)))
            rule_stage_start = time.time()
            engine = RuleEngine()
            try:
                df = engine.apply_rules(df, enhanced_rules)
            except Exception as e:
                raise RuntimeError(f"[PipelineExecutor] Rule engine failed for pipeline {cfg.get('pipeline_id')}: {e}") from e
            logger.info(
                "[PipelineExecutor] STAGE_END %s",
                _fmt_vars(
                    stage="rule_engine",
                    pipeline_id=cfg.get("pipeline_id"),
                    rule_count=len(enhanced_rules),
                    stage_duration_seconds=round(time.time() - rule_stage_start, 2),
                ),
            )
            self._log_count("After rule engine", df, cfg.get("pipeline_id"))

        # ============================================================
        # COLUMN MAP
        # ============================================================
        # Keep SCD2 lag-order columns (e.g. MQP_DATE_MODIFIED) alive through the
        # projection even when they aren't part of the physical/target schema.
        _keep_extra = [c.strip() for c in (cfg.get("scd2_lag_order_cols") or "").split(",") if c.strip()]
        logger.info(
            "[PipelineExecutor] STAGE_START %s",
            _fmt_vars(
                stage="column_mapping",
                pipeline_id=cfg.get("pipeline_id"),
                use_ctl_schema=use_ctl_schema,
                keep_extra_cols=_keep_extra,
            ),
        )
        column_mapping_stage_start = time.time()
        df = self._apply_column_mapping(df, cfg, use_ctl_schema=use_ctl_schema, keep_extra_cols=_keep_extra)
        logger.info(
            "[PipelineExecutor] STAGE_END %s",
            _fmt_vars(
                stage="column_mapping",
                pipeline_id=cfg.get("pipeline_id"),
                use_ctl_schema=use_ctl_schema,
                keep_extra_cols=_keep_extra,
                stage_duration_seconds=round(time.time() - column_mapping_stage_start, 2),
            ),
        )
        self._log_count("After column mapping", df, cfg.get("pipeline_id"))
        df = df.withColumn("LD_DT", F.current_timestamp()) ## must to present
        self._log_count("build_df output (LD_DT stamped)", df, cfg.get("pipeline_id"))
        return df

    # ================================================================
    # EXECUTE
    # ================================================================
    def execute(self, cfg, settings, context, load_type, layer="silver"):

        pid = cfg["pipeline_id"]
        start_ts = __import__("datetime").datetime.utcnow()

        # ✅ target selection
        if layer.lower() == "gold":
            target = f"{self.settings.gold_fq}.{cfg['target_table']}"
        else:
            target = f"{self.settings.silver_fq}.{cfg['target_table']}"

        pipeline_stage_start = time.time()
        logger.info("[PipelineExecutor] PIPELINE_START %s", _fmt_vars(pipeline_id=pid, pipeline_name=cfg.get("pipeline_name"), run_id=(context or {}).get("run_id"), batch_id=(context or {}).get("batch_id"), environment=getattr(self.settings, "environment", None), source_tables=[src.get("table") for src in cfg.get("sources", [])], target=target, ingestion_mode=load_type))

        # Resolve effective load mode before source reads so connector-side
        # incremental date/watermark filtering uses the same mode as writer.
        effective_load_type = self._resolve_effective_load_type(cfg, context, load_type)
        runtime_context = dict(context or {})
        runtime_context["load_type"] = effective_load_type
        logger.info(
            "[PipelineExecutor] LOAD_MODE_RESOLVED %s",
            _fmt_vars(
                pipeline_id=pid,
                requested_load_type=load_type,
                effective_load_type=effective_load_type,
                watermark_ts=(context or {}).get("watermark_ts"),
                start_date=(context or {}).get("start_date"),
                end_date=(context or {}).get("end_date"),
            ),
        )

        # Hybrid mode:
        # - Existing GOLD tables: use physical table schema (skip ctl_table_schema projection)
        # - Others (including developing/new tables): keep ctl_table_schema behavior
        use_ctl_schema = not (layer.lower() == "gold" and DeltaWriter._table_exists(self.spark, target))
        if not use_ctl_schema:
            logger.info("[PipelineExecutor] Existing Gold table detected; using physical table schema for %s", target)
        else:
            logger.info(
                "[PipelineExecutor] Target schema mode %s",
                _fmt_vars(pipeline_id=pid, target=target, use_ctl_schema=use_ctl_schema, layer=layer),
            )

        df = self.build_df(cfg, runtime_context, use_ctl_schema=use_ctl_schema)
        _log_schema("build_df_returned", df, pid)
        _log_query_plan("build_df_returned", df, pid)
        self._log_count("build_df returned to execute()", df, pid)

        logger.info("[PipelineExecutor] Target: %s", target)

        # ================= DEDUP + PRIMARY KEYS =================
        pk = cfg["primary_keys"]

        if isinstance(pk, str):
            pk = [c.strip() for c in pk.split(",") if c.strip()]

        if not pk:
            raise ValueError(f"No primary keys for pipeline {pid}")
        if _primary_keys_enabled():
            logger.debug("[PipelineExecutor] PRIMARY_KEYS %s", _fmt_vars(pipeline_id=pid, primary_keys=pk))

        # ================= INTRA-BATCH SRC_EXPRN_DT (before alignment — computes columns) =================
        # scd2_lag_order_cols drives LEAD window ASC (→ SRC_EXPRN_DT); first col is the LEAD value target.
        # Only apply LEAD logic if SRC_EXPRN_DT already exists; do NOT create it if missing.
        _df_cols_lower = {c.lower(): c for c in df.columns}

        _lag_cols = cfg.get("scd2_lag_order_cols")
        logger.info("[PipelineExecutor] Config check: scd2_lag_order_cols=%s", _lag_cols)
        if _lag_cols:
            _tgt_col = "SRC_EXPRN_DT"
            _lag_parts = [c.strip() for c in _lag_cols.split(",") if c.strip()]
            _lag_primary = _lag_parts[0]
            _lag_primary_actual = _df_cols_lower.get(_lag_primary.lower())
            _tgt_col_actual = _df_cols_lower.get(_tgt_col.lower())
            logger.info(
                "[PipelineExecutor] LEAD check: scd2_lag_order_cols=%s primary=%s found=%s target=%s found=%s",
                _lag_cols, _lag_primary, _lag_primary_actual, _tgt_col, _tgt_col_actual,
            )
            if _lag_primary_actual and _tgt_col_actual:
                # Partition defaults to first PK (entity hash key)
                _part_keys_actual = [_df_cols_lower.get(pk[0].lower(), pk[0])]
                # Build multi-column ORDER BY ASC for the LEAD window
                _lead_order_exprs = []
                for _oc in _lag_parts:
                    _oc_actual = _df_cols_lower.get(_oc.lower())
                    if _oc_actual:
                        _lead_order_exprs.append(F.col(_oc_actual).asc())
                    else:
                        logger.warning("[PipelineExecutor] scd2_lag_order_cols part '%s' not in DataFrame; skipping for pipeline %s", _oc, pid)
                _w = Window.partitionBy(*_part_keys_actual).orderBy(*_lead_order_exprs)
                df = df.withColumn(_tgt_col_actual, F.lead(_lag_primary_actual).over(_w))
                _log_window(
                    stage="intra_batch_lead",
                    pid=pid,
                    output_column=_tgt_col_actual,
                    function_name="lead",
                    partition_by=_part_keys_actual,
                    order_by=_lag_parts,
                    frame="UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING",
                    conditions=f"scd2_lag_order_cols={_lag_cols}",
                )
                logger.info(
                    "[PipelineExecutor] Intra-batch %s = LEAD(%s) OVER (%s ORDER BY %s) for %s",
                    _tgt_col_actual, _lag_primary_actual, _part_keys_actual, _lag_parts, pid,
                )
                self._log_count("After intra-batch SRC_EXPRN_DT LEAD", df, pid)
            else:
                logger.warning(
                    "[PipelineExecutor] LEAD skipped: scd2_lag_order_cols=%s primary=%s found=%s, target=%s found=%s",
                    _lag_cols, _lag_primary, _lag_primary_actual, _tgt_col, _tgt_col_actual,
                )

        # Align to physical Delta table schema when table already exists.
        # This prevents write-time merge errors from type drift.
        _ld_end_dt_handled = False  # set True below if the LNK block closes LD_END_DT itself
        _is_lnk_scd2 = "LNK_" in (cfg.get("target_table") or "").strip().upper() and cfg.get("scd_type", "").lower() == "scd2"
        if _is_lnk_scd2:
            _lag_cols_cfg = cfg.get("scd2_lag_order_cols") or ""
            _lag_cols_ci = {c.lower(): c for c in df.columns}
            # Only order by lag columns that actually exist in the final df — avoids
            # hard-failing when a config column (e.g. MQP_DATE_MODIFIED) isn't mapped.
            _lag_cols_actual = [
                _lag_cols_ci[c.strip().lower()]
                for c in _lag_cols_cfg.split(",")
                if c.strip() and c.strip().lower() in _lag_cols_ci
            ]

            if not _lag_cols_actual:
                logger.warning(
                    "[PipelineExecutor] LNK SCD2 rank skipped for %s: none of scd2_lag_order_cols=%s found in df",
                    pid, _lag_cols_cfg,
                )
            else:
                # Entity grain for intrabatch versioning comes from ctl_pipeline.partition_by
                # (NOT the physical Delta partition_by used elsewhere) — falls back to the
                # full primary key when partition_by isn't usable against this DataFrame.
                _part_by_cfg = cfg.get("partition_by") or ""
                if isinstance(_part_by_cfg, str):
                    _part_by_cfg = [c.strip() for c in _part_by_cfg.split(",") if c.strip()]
                _entity_cols = [
                    _lag_cols_ci[c.strip().lower()]
                    for c in _part_by_cfg
                    if c.strip() and c.strip().lower() in _lag_cols_ci
                ] or pk
                
                _lag_desc_exprs_lnk = []
                for _lc in [c.strip() for c in _lag_cols_actual if c.strip()]:
                    _lag_desc_exprs_lnk.append(F.col(_lc).desc())

                _lag_order_exprs = [F.col(c) for c in _lag_cols_actual]

                # Drop exact within-batch duplicates on the full primary key.
                _dedup_win = Window.partitionBy(*pk).orderBy(*_lag_order_exprs)
                df = (df.withColumn("__rownumber__", F.row_number().over(_dedup_win))
                        .filter(F.col("__rownumber__") == 1)
                        .drop("__rownumber__"))

                # Rank within the entity grain (partition_by) to close every row except the latest.
                _rank_win = Window.partitionBy(*_entity_cols).orderBy(*_lag_desc_exprs_lnk)
                df = df.withColumn("__rank__", F.row_number().over(_rank_win))
                _log_window(
                    stage="lnk_scd2_rank",
                    pid=pid,
                    output_column="__rank__",
                    function_name="row_number",
                    partition_by=_entity_cols,
                    order_by=_lag_cols_actual,
                    frame="UNBOUNDED PRECEDING TO CURRENT ROW",
                    conditions="LNK SCD2 entity ranking",
                )
                df = df.withColumn(
                    "LD_END_DT",
                    F.when(F.col("__rank__") != 1, F.col("LD_DT")).otherwise(F.lit(None).cast("timestamp"))
                ).drop("__rank__")
                _log_expression(
                    stage="lnk_scd2_rank",
                    pid=pid,
                    output_column="LD_END_DT",
                    expression="when(__rank__ != 1, LD_DT).otherwise(cast(null as timestamp))",
                    source_columns=["__rank__", "LD_DT"],
                    alias="LD_END_DT",
                    cast_datatype="timestamp",
                )
                _ld_end_dt_handled = True
                logger.info(
                    "[PipelineExecutor] LNK SCD2 within-batch: dedup on pk=%s, rank on entity=%s, order=%s for %s",
                    pk, _entity_cols, _lag_cols_actual, pid,
                )

                # Strict schema alignment: drop lag columns (e.g. MQP_DATE_MODIFIED) that were
                # carried through only for ranking and are not part of the final target schema.
                _target_schema_cols = {c.lower() for c in (self._get_target_schema(cfg) or {})}
                _rank_only_cols = [c for c in _lag_cols_actual if c.lower() not in _target_schema_cols]
                if _rank_only_cols:
                    if "BTCH_DT" in _rank_only_cols:
                        logger.debug("[PipelineExecutor] Removing BTCH_DT from rank-only columns for %s", pid)
                        _rank_only_cols.remove("BTCH_DT")
                    df = df.drop(*_rank_only_cols)
                    logger.info("[PipelineExecutor] Dropped rank-only column(s) %s for %s", _rank_only_cols, pid)
                self._log_count("After LNK SCD2 within-batch dedup/rank", df, pid)

        # MQP_DATE_MODIFIED is used only for LNK ordering above and isn't part of
        # target_schema — keep it alive through alignment (still needed by the
        # scd2_rank_order_cols dedup below), it is dropped only in the LNK block.
        _mqp_dt_mod = "MQP_DATE_MODIFIED"
        _keep_mqp_dt_mod = [_mqp_dt_mod] if _is_lnk_scd2 else []
        df = self._align_to_existing_table_schema(df, target, keep_extra_cols=_keep_mqp_dt_mod)
        self._log_count("After align to existing table schema", df, pid)

        # Drop rows where ANY PK column is NULL.
        # NULL PKs cannot be correctly matched in a Delta MERGE (NULL == NULL → NULL,
        # not TRUE), so they would be re-inserted as new rows on every incremental run.
        # These rows typically come from unmatched LEFT joins — they should not be loaded.
        pk_not_null = F.col(pk[0]).isNotNull()
        for _pk_col in pk[1:]:
            pk_not_null = pk_not_null & F.col(_pk_col).isNotNull()
        df = df.filter(pk_not_null)
        logger.info("[PipelineExecutor] NULL-PK filter applied for pipeline %s (PKs: %s)", pid, pk)
        if _debug_enabled() and _bool_env("LOG_FILTER_RULES", True):
            logger.debug("[PipelineExecutor] FILTER %s", _fmt_vars(stage="null_pk_filter", pipeline_id=pid, condition=" AND ".join(f"{col_name} IS NOT NULL" for col_name in pk)))
        self._log_count("After NULL-PK filter", df, pid)

        # Deduplicate on PK — use watermark_col DESC so the latest row wins when
        # multiple source updates share the same key within one batch.
        # NOTE: watermark_col may not exist in the final DataFrame if it wasn't
        # included in ctl_column_map (e.g., it's a tracking column, not a data column).
        # Only use it for ordering if it's actually in the DataFrame.
        _wm_col = cfg.get("watermark_col")
        _rank_raw = cfg.get("scd2_rank_order_cols") if cfg.get("scd_type", "").lower() == "scd2" else None

        if _rank_raw:
            # Within-batch SCD2: drop true duplicates (same biz data), keep all biz-changing rows;
            # set LD_END_DT = SRC_EXPRN_DT for non-latest rows (within-batch expired).
            _df_cols_ci = {c.lower(): c for c in df.columns}
            _dedup_exprs = []
            for _token in _rank_raw.split(","):
                _parts = _token.strip().split()
                _col_name = _parts[0].strip()
                _direction = _parts[1].strip().upper() if len(_parts) > 1 else "ASC"
                _actual = _df_cols_ci.get(_col_name.lower())
                if _actual:
                    _dedup_exprs.append(F.col(_actual).desc() if _direction == "DESC" else F.col(_actual).asc())
                else:
                    logger.warning("[PipelineExecutor] scd2_rank_order_cols column %s not in DataFrame; skipping for pipeline %s", _col_name, pid)

            _lag_cols_raw = cfg.get("scd2_lag_order_cols")
            _technical_set = {"REF_ID", "SRC_EFF_DT", "SRC_EXPRN_DT", "LD_DT", "LD_END_DT",
                               "BTCH_ID", "BTCH_DT", "ERR_CD", "ERR_FLG", "REC_SRC_NM", "PART_COL"}
            _pk_set = {k.upper() for k in pk}
            _biz_cols = [c for c in df.columns if c.upper() not in _technical_set and c.upper() not in _pk_set]
            logger.info("[PipelineExecutor] SCD2 biz comparison columns for %s: %s", pid, _biz_cols)

            if _biz_cols and _lag_cols_raw and _dedup_exprs:
                _lag_asc_exprs = []
                for _lc in [c.strip() for c in _lag_cols_raw.split(",") if c.strip()]:
                    _lc_actual = _df_cols_ci.get(_lc.lower())
                    if _lc_actual:
                        _lag_asc_exprs.append(F.col(_lc_actual).asc())
                if _lag_asc_exprs:
                    _biz_concat = F.lower(F.concat_ws("~", *[F.coalesce(F.trim(F.col(c).cast("string")), F.lit("")) for c in _biz_cols]))
                    df = df.withColumn("__biz__", _biz_concat)
                    _lag_win = Window.partitionBy(*pk).orderBy(*_lag_asc_exprs)
                    df = df.withColumn("__biz_prev__", F.lag("__biz__").over(_lag_win))
                    df = df.filter(F.col("__biz_prev__").isNull() | (F.col("__biz__") != F.coalesce(F.col("__biz_prev__"), F.lit(""))))
                    df = df.drop("__biz__", "__biz_prev__")
                    logger.info("[PipelineExecutor] Within-batch SCD2: dropped true-duplicate rows for pipeline %s", pid)
                    # Re-compute SRC_EXPRN_DT on surviving rows only — dropped duplicates must not contribute.
                    _src_exprn_col = _df_cols_ci.get("src_exprn_dt")
                    _src_eff_col = _df_cols_ci.get("src_eff_dt")
                    if _src_exprn_col and _src_eff_col:
                        df = df.withColumn(_src_exprn_col, F.lead(_src_eff_col).over(Window.partitionBy(*pk).orderBy(*_lag_asc_exprs)))
                    self._log_count("After within-batch true-duplicate drop", df, pid)
            logger.info("[PipelineExecutor] SCD2 dedup_exprs=%s pk=%s for %s", _dedup_exprs, pk, pid)
            if _dedup_exprs and not _ld_end_dt_handled:
                _rank_win = Window.partitionBy(*pk).orderBy(*_dedup_exprs)
                df = df.withColumn("__rank__", F.row_number().over(_rank_win))
                _ld_end_actual = _df_cols_ci.get("ld_end_dt")
                if _ld_end_actual:
                    df = df.withColumn(_ld_end_actual,
                        F.when(F.col("__rank__") != 1, F.current_timestamp() - F.expr("INTERVAL 1 SECOND"))
                         .otherwise(F.lit(None).cast("timestamp"))
                    )
                df = df.drop("__rank__")
            elif _dedup_exprs and _ld_end_dt_handled:
                logger.info("[PipelineExecutor] Skipping generic LD_END_DT rank step for %s; already closed by LNK block", pid)
            logger.info("[PipelineExecutor] SCD2 within-batch complete: order=%s for pipeline %s", _rank_raw, pid)
            self._log_count("After SCD2 within-batch dedup", df, pid)

        elif _wm_col and _wm_col in df.columns:
            logger.info("[PipelineExecutor] Dedup ordering by watermark_col %s DESC for pipeline %s", _wm_col, pid)
            df = (df.withColumn("rn", F.row_number().over(Window.partitionBy(*pk).orderBy(F.col(_wm_col).desc())))
                   .filter(F.col("rn") == 1).drop("rn"))
            self._log_count("After watermark-ordered PK dedup", df, pid)
        else:
            if _wm_col:
                logger.warning("[PipelineExecutor] Watermark column %s not in DataFrame; using non-deterministic ordering for dedup", _wm_col)
            df = (df.withColumn("rn", F.row_number().over(Window.partitionBy(*pk).orderBy(F.lit(1))))
                   .filter(F.col("rn") == 1).drop("rn"))
            self._log_count("After non-deterministic PK dedup", df, pid)

        # Ordering is done — drop MQP_DATE_MODIFIED only for LNK pipelines; it was
        # kept alive above solely for that ordering and isn't part of target_schema.
        if _is_lnk_scd2 and _mqp_dt_mod in df.columns:
            _target_schema_cols_final = {c.lower() for c in (self._get_target_schema(cfg) or {})}
            if _mqp_dt_mod.lower() not in _target_schema_cols_final:
                df = df.drop(_mqp_dt_mod)
                logger.info("[PipelineExecutor] Dropped %s for LNK pipeline %s", _mqp_dt_mod, pid)
        self._log_count("Final df before write", df, pid)
        _log_schema("final_df_before_write", df, pid)
        _log_query_plan("final_df_before_write", df, pid)

        # ================= WRITE =================
        try:
            writer = DeltaWriter()
            _partition_by_cfg = cfg.get("partition_by")
            if isinstance(_partition_by_cfg, str):
                _partition_by_cfg = [c.strip() for c in _partition_by_cfg.split(",") if c.strip()]
            _partition_by_actual = [c for c in (_partition_by_cfg or []) if c in df.columns]
            logger.info(
                "[PipelineExecutor] STAGE_START %s",
                _fmt_vars(
                    stage="write",
                    pipeline_id=pid,
                    target=target,
                    load_type=effective_load_type,
                    scd_type=cfg.get("scd_type"),
                    primary_keys=pk,
                    partition_by=_partition_by_actual,
                    watermark_col=cfg.get("watermark_col"),
                    rank_order_cols=cfg.get("scd2_rank_order_cols"),
                    layer = layer
                ),
            )
            write_stage_start = time.time()
            result = writer.write(
                df=df,
                target_table=target,
                primary_keys=pk,
                load_type=effective_load_type,
                scd_type=cfg.get("scd_type"),
                partition_by=_partition_by_actual,
                watermark_col=cfg.get("watermark_col"),
                rank_order_cols=cfg.get("scd2_rank_order_cols"),
                layer = layer
            )
            logger.info("[PipelineExecutor] STAGE_END %s", _fmt_vars(stage="write", pipeline_id=pid, target=target, stage_duration_seconds=round(time.time() - write_stage_start, 2), rows_inserted=result.get("rows_inserted"), rows_updated=result.get("rows_updated"), new_watermark_ts=result.get("new_watermark_ts")))
            logger.info("[PipelineExecutor] DONE %s result=%s", pid, result)
            logger.info("[PipelineExecutor] PIPELINE_COMPLETE %s", _fmt_vars(pipeline_id=pid, pipeline_name=cfg.get("pipeline_name"), run_id=(context or {}).get("run_id"), batch_id=(context or {}).get("batch_id"), target=target, status="SUCCESS", total_duration_seconds=round(time.time() - pipeline_stage_start, 2), rows_inserted=result.get("rows_inserted"), rows_updated=result.get("rows_updated"), new_watermark_ts=result.get("new_watermark_ts")))
            if RunLogger:
                RunLogger(self.spark, settings, context.get("run_id", "")).log(
                    batch_id=context.get("batch_id"),
                    pipeline_id=pid,
                    product_code=cfg.get("product_code"),
                    target_table=cfg.get("target_table"),
                    load_type=effective_load_type,
                    rows_inserted=result.get("rows_inserted"),
                    rows_updated=result.get("rows_updated"),
                    rows_affected=result.get("rows_affected"),
                    watermark_ts=context.get("watermark_ts"),
                    new_watermark_ts=result.get("new_watermark_ts"),
                    status="SUCCESS",
                    error_message=None,
                    start_ts=start_ts,
                    end_ts=__import__("datetime").datetime.utcnow(),
                )
            return result
        except Exception as _exc:
            logger.info("[PipelineExecutor] STAGE_END %s", _fmt_vars(stage="write", pipeline_id=pid, target=target, status="FAILED", stage_duration_seconds=round(time.time() - write_stage_start, 2) if 'write_stage_start' in locals() else None, error=str(_exc)[:500]))
            logger.error("[PipelineExecutor] FAILED %s: %s", pid, _exc, exc_info=True)
            logger.info("[PipelineExecutor] PIPELINE_COMPLETE %s", _fmt_vars(pipeline_id=pid, pipeline_name=cfg.get("pipeline_name"), run_id=(context or {}).get("run_id"), batch_id=(context or {}).get("batch_id"), target=target, status="FAILED", total_duration_seconds=round(time.time() - pipeline_stage_start, 2), error=str(_exc)[:500]))
            if RunLogger:
                RunLogger(self.spark, settings, context.get("run_id", "")).log(
                    batch_id=context.get("batch_id"),
                    pipeline_id=pid,
                    product_code=cfg.get("product_code"),
                    target_table=cfg.get("target_table"),
                    load_type=effective_load_type,
                    rows_inserted=0,
                    rows_updated=0,
                    rows_affected=0,
                    watermark_ts=context.get("watermark_ts"),
                    new_watermark_ts=None,
                    status="FAILED",
                    error_message=str(_exc)[:2000],
                    start_ts=start_ts,
                    end_ts=__import__("datetime").datetime.utcnow(),
                )
            raise


# ================================================================
# BACKWARD COMPATIBILITY
# ================================================================
class GoldPipelineExecutor(PipelineExecutor):
    def execute(self, cfg, settings, context, load_type, layer="gold") -> dict:
        return super().execute(cfg, settings, context, load_type, layer="gold")
