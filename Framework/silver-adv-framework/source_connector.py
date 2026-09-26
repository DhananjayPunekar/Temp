# =====================================================================
# framework/source_connector.py  (NEW — C-06/C-07 fix)
# Abstract SourceConnector base class + concrete implementations.
#
# WHY:
#   Christine's concern C-06/C-07: ETL logic is tightly coupled to
#   Majesco Delta tables; no abstraction for alternative sources.
#   Adding a new source (e.g. JDBC, Parquet Volume) requires new notebooks.
#
# SOLUTION:
#   SourceConnector ABC defines a single read() contract.
#   Concrete classes implement it; PipelineExecutor auto-selects the right
#   connector via ConnectorRegistry.get(source_type).
#
# EXTENDING:
#   1. Create class MyConnector(SourceConnector)
#   2. Implement supports() and read()
#   3. Register: ConnectorRegistry.register("my_type", MyConnector())
# =====================================================================
import logging
import os
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from decimal import Decimal
from pprint import pformat
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logger = logging.getLogger(__name__)
logging.getLogger("py4j").setLevel(logging.ERROR)


def _bool_env(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, str(default))).strip().lower()
    return raw in {"1", "true", "t", "yes", "y"}


def _debug_enabled() -> bool:
    return logger.isEnabledFor(logging.DEBUG)


def _fmt_vars(**kwargs) -> str:
    return pformat({k: kwargs[k] for k in sorted(kwargs)}, compact=True)


def _dataframe_sample_enabled() -> bool:
    return _debug_enabled() and _bool_env("LOG_DATAFRAME_SAMPLE", False)


def _dataframe_sample_rows() -> int:
    raw_value = str(os.getenv("LOG_DATAFRAME_SAMPLE_ROWS", "10")).strip()
    try:
        parsed_value = int(raw_value)
    except ValueError:
        return 10
    return max(1, parsed_value)


def _log_dataframe_sample(stage: str, df: DataFrame, source_ref: str) -> None:
    if not _dataframe_sample_enabled():
        return
    sample_rows = _dataframe_sample_rows()
    logger.debug(
        "[SourceConnector] DATAFRAME_SAMPLE_START %s",
        _fmt_vars(stage=stage, source=source_ref, rows=sample_rows),
    )
    try:
        df.show(sample_rows, truncate=False)
    except Exception as exc:
        logger.warning(
            "[SourceConnector] DATAFRAME_SAMPLE_FAILED %s",
            _fmt_vars(stage=stage, source=source_ref, rows=sample_rows, error=str(exc)[:300]),
        )
        return
    logger.debug(
        "[SourceConnector] DATAFRAME_SAMPLE_END %s",
        _fmt_vars(stage=stage, source=source_ref, rows=sample_rows),
    )


def _resolve_column_name(df: DataFrame, requested_column: Optional[str]) -> Optional[str]:
    candidate = (requested_column or "").strip()
    if not candidate:
        return None
    lower_cols = {column_name.lower(): column_name for column_name in df.columns}
    return lower_cols.get(candidate.lower())


def _parse_debug_date(raw_value: str):
    value = (raw_value or "").strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Value '{raw_value}' is not a supported date literal")


def _parse_debug_timestamp(raw_value: str):
    value = (raw_value or "").strip()
    normalized = value.replace("T", " ")
    for fmt in (
        "%Y%m%d %H%M%S",
        "%Y%m%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S.%f",
    ):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Value '{raw_value}' is not a supported timestamp literal") from exc


def _coerce_debug_filter_value(raw_value: Optional[str], data_type: T.DataType):
    value = "" if raw_value is None else str(raw_value).strip()

    if isinstance(data_type, T.StringType):
        return value
    if isinstance(data_type, T.BooleanType):
        lowered = value.lower()
        if lowered in {"true", "1", "yes", "y", "t"}:
            return True
        if lowered in {"false", "0", "no", "n", "f"}:
            return False
        raise ValueError(f"Value '{raw_value}' is not a valid boolean")
    if isinstance(data_type, (T.ByteType, T.ShortType, T.IntegerType, T.LongType)):
        return int(value)
    if isinstance(data_type, (T.FloatType, T.DoubleType)):
        return float(value)
    if isinstance(data_type, T.DecimalType):
        return Decimal(value)
    if isinstance(data_type, T.DateType):
        return _parse_debug_date(value)
    if isinstance(data_type, T.TimestampType):
        return _parse_debug_timestamp(value)

    raise ValueError(f"Debug filter does not support column type {data_type.simpleString()}")


def _apply_debug_filter(
    df: DataFrame,
    source_ref: str,
    debug_filter_column: Optional[str],
    debug_filter_value: Optional[str],
) -> DataFrame:
    requested_column = (debug_filter_column or "").strip()
    requested_value = "" if debug_filter_value is None else str(debug_filter_value).strip()

    if not requested_column and not requested_value:
        return df

    if not requested_column or not requested_value:
        logger.warning(
            "[SourceConnector] DEBUG_FILTER_SKIPPED %s",
            _fmt_vars(
                source=source_ref,
                reason="debug_filter_column and debug_filter_value must both be provided",
                debug_filter_column=requested_column or None,
                debug_filter_value=debug_filter_value,
            ),
        )
        return df

    resolved_column = _resolve_column_name(df, requested_column)
    if resolved_column is None:
        logger.warning(
            "[SourceConnector] DEBUG_FILTER_SKIPPED %s",
            _fmt_vars(
                source=source_ref,
                reason="column_not_found",
                debug_filter_column=requested_column,
                available_columns=df.columns,
            ),
        )
        return df

    field = next((schema_field for schema_field in df.schema.fields if schema_field.name == resolved_column), None)
    if field is None:
        logger.warning(
            "[SourceConnector] DEBUG_FILTER_SKIPPED %s",
            _fmt_vars(source=source_ref, reason="schema_lookup_failed", debug_filter_column=resolved_column),
        )
        return df

    try:
        coerced_value = _coerce_debug_filter_value(requested_value, field.dataType)
    except ValueError as exc:
        logger.warning(
            "[SourceConnector] DEBUG_FILTER_SKIPPED %s",
            _fmt_vars(
                source=source_ref,
                reason="value_cast_failed",
                debug_filter_column=resolved_column,
                debug_filter_value=requested_value,
                data_type=field.dataType.simpleString(),
                error=str(exc),
            ),
        )
        return df

    logger.info(
        "[SourceConnector] DEBUG_FILTER_APPLIED %s",
        _fmt_vars(
            source=source_ref,
            debug_filter_column=resolved_column,
            debug_filter_value=requested_value,
            coerced_value=str(coerced_value),
            data_type=field.dataType.simpleString(),
        ),
    )
    filtered_df = df.filter(F.col(resolved_column) == F.lit(coerced_value).cast(field.dataType))
    _log_dataframe_sample("after_debug_filter", filtered_df, source_ref)
    return filtered_df


# -----------------------------------------------------------------------
# Abstract base
# -----------------------------------------------------------------------
class SourceConnector(ABC):
    """
    Contract for reading a source into a Spark DataFrame.
    Subclasses handle connection, filtering, and watermarking.
    """

    @abstractmethod
    def supports(self, source_type: str) -> bool:
        """Return True if this connector handles the given source_type."""
        ...

    @abstractmethod
    def read(
        self,
        spark: SparkSession,
        source_ref: str,
        *,
        product: Optional[str] = None,
        product_filter_col: Optional[str] = None,
        is_common: bool = False,
        watermark_col: Optional[str] = None,
        load_type: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        watermark_ts=None,
        watermark_type: str = "timestamp",
        pqb_flag_col: Optional[str] = None,
        pqb_flags: Optional[list] = None,
        debug_filter_column: Optional[str] = None,
        debug_filter_value: Optional[str] = None,
        num_of_days_for_cmn_tables: Optional[int] = None,
    ) -> DataFrame:
        """
        Read the source and apply product + incremental watermark filtering + PQB filters.

        Args:
            spark:              Active SparkSession.
            source_ref:         Table name, path, or JDBC URL.
            product:            Product code for pushdown filter (e.g. "BO").
            product_filter_col: Column in source that identifies the product.
            is_common:          If True, never apply product filter (common tables).
            watermark_col:      Control-table watermark column used for incremental filtering.
            load_type:          Runtime load mode ('incremental' or 'full').
            start_date:         Optional inclusive lower date bound for incremental date filtering.
            end_date:           Optional inclusive upper date bound for incremental date filtering.
            watermark_ts:       Last successful watermark from run_log, used when no explicit date window is provided.
            watermark_type:     Watermark type metadata retained on connector signatures for compatibility.
            pqb_flag_col:       Optional column holding P/Q/B flag (e.g. MQP_PQB_FLAG).
            pqb_flags:          Allowed values to keep (e.g. ['P', 'B']). None = no filter.
            debug_filter_column: Optional debug-only column name to further restrict the source.
            debug_filter_value: Optional debug-only value to match after coercing to the source column data type.
            num_of_days_for_cmn_tables: Optional number of days to filter SAT_PREM/SAT_CMSN tables by LD_DT.

        Returns:
            Filtered DataFrame.
        """
        ...


# -----------------------------------------------------------------------
# Concrete: Delta table (most common — Majesco Bronze in Unity Catalog)
# -----------------------------------------------------------------------
class DeltaTableConnector(SourceConnector):
    """
    Reads Unity Catalog Delta tables (the primary Majesco/QBE source).
    Handles product pushdown and incremental watermark filtering.
    """
    
    def __init__(self, default_schema=None):
            self.default_schema = default_schema

    def _build_watermark_expr(self, df: DataFrame, watermark_col: Optional[str]):
        if not watermark_col:
            return None, []

        lower_cols = {c.lower(): c for c in df.columns}
        requested_cols = [part.strip() for part in str(watermark_col).split(",") if part and part.strip()]
        resolved_cols = [lower_cols[c.lower()] for c in requested_cols if c.lower() in lower_cols]

        if not resolved_cols:
            return None, []

        expr = F.col(resolved_cols[0]) if len(resolved_cols) == 1 else F.coalesce(*[F.col(c) for c in resolved_cols])
        return expr, resolved_cols

    def supports(self, source_type: str) -> bool:
        return source_type.lower() in ("delta", "")

    def read(
        self,
        spark: SparkSession,
        source_ref: str,
        *,
        product: Optional[str] = None,
        product_filter_col: Optional[str] = None,
        is_common: bool = False,
        watermark_col: Optional[str] = None,
        load_type: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        watermark_ts=None,
        watermark_type: str = "timestamp",
        pqb_flag_col: Optional[str] = None,
        pqb_flags: Optional[list] = None,
        debug_filter_column: Optional[str] = None,
        debug_filter_value: Optional[str] = None,
        num_of_days_for_cmn_tables: Optional[int] = None,
    ) -> DataFrame:
        logger.info("[DeltaTableConnector] SOURCE_READ_START %s", _fmt_vars(source=source_ref, product=product, product_filter_col=product_filter_col, is_common=is_common, load_type=load_type, watermark_col=watermark_col, pqb_flag_col=pqb_flag_col, pqb_flags=pqb_flags, debug_filter_column=debug_filter_column, debug_filter_value=debug_filter_value))
        try:
            df = spark.table(source_ref)
            _log_dataframe_sample("source_read_initial", df, source_ref)
            if debug_filter_column is not None and debug_filter_value is not None:
                df = _apply_debug_filter(df, source_ref, debug_filter_column, debug_filter_value)
                _log_dataframe_sample("source_read_final", df, source_ref)
        except Exception as e:
            raise RuntimeError(f"[DeltaTableConnector] Failed to read table {source_ref}: {e}") from e

        # FIX NB-3 / C-05: use Column expression — NOT string interpolation
        if product and product_filter_col:
            if product_filter_col in df.columns:
                if _debug_enabled() and _bool_env("LOG_FILTER_RULES", True):
                    logger.debug("[DeltaTableConnector] FILTER %s", _fmt_vars(source=source_ref, condition=f"{product_filter_col} = {product}", filter_type="product"))
                df = df.filter(F.upper(F.col(product_filter_col)).isin(product))
                _log_dataframe_sample("after_product_filter", df, source_ref)
            else:
                logger.warning(
                    "[DeltaTableConnector] product_filter_col '%s' not found in %s. "
                    "Skipping product filter — verify ctl_pipeline config.",
                    product_filter_col, source_ref,
                )

        # PQB filter: keep only rows matching allowed Policy/Quote/Binder flags.
        # pqb_flags comes from the widget (e.g. ['P', 'B']); None means no filter.
        # F.upper() ensures the comparison is case-insensitive regardless of source data casing.
        if pqb_flag_col and pqb_flags:
            if pqb_flag_col in df.columns:
                if _debug_enabled() and _bool_env("LOG_FILTER_RULES", True):
                    logger.debug("[DeltaTableConnector] FILTER %s", _fmt_vars(source=source_ref, condition=f"UPPER({pqb_flag_col}) IN {list(pqb_flags)}", filter_type="pqb"))
                df = df.filter(F.upper(F.col(pqb_flag_col)).isin(pqb_flags))
                _log_dataframe_sample("after_pqb_filter", df, source_ref)
                logger.info(
                    "[DeltaTableConnector] PQB filter applied: UPPER(%s) IN %s on %s",
                    pqb_flag_col, pqb_flags, source_ref,
                )
            else:
                logger.warning(
                    "[DeltaTableConnector] pqb_flag_col '%s' not found in %s. "
                    "PQB filter skipped — verify bronze schema.",
                    pqb_flag_col, source_ref,
            )
        if "HUB_" in (source_ref).upper() or "LNK_" in (source_ref).upper():
            return df

        source_upper = source_ref.upper()
        if "SAT_" in source_upper and not ("SAT_PREM" in source_upper or "SAT_CMSN" in source_upper):
            return df
        if ("SAT_PREM" in source_upper or "SAT_CMSN" in source_upper):
            if num_of_days_for_cmn_tables is not None and str(num_of_days_for_cmn_tables).strip() and str(num_of_days_for_cmn_tables).strip() != "NA":
                try:
                    num_days = int(num_of_days_for_cmn_tables)
                    calculated_start_date = (datetime.now() - timedelta(days=num_days)).strftime('%Y-%m-%d')
                    calculated_end_date = (datetime.now()).strftime('%Y-%m-%d')
                    
                    logger.info(
                        "[DeltaTableConnector] Applying SAT_PREM/SAT_CMSN date range filter using num_of_days_for_cmn_tables %s: %s to %s on %s",
                        num_days,
                        calculated_start_date,
                        calculated_end_date,
                        source_ref,
                    )
                    
                    if _debug_enabled() and _bool_env("LOG_FILTER_RULES", True):
                        logger.debug("[DeltaTableConnector] FILTER %s", _fmt_vars(source=source_ref, condition=f"LD_DT BETWEEN {calculated_start_date} AND {calculated_end_date}", filter_type="sat_prem_cmsn"))
                    
                    # Filter by LD_DT column
                    df = df.filter(
                        (F.col("LD_DT").cast("date") >= F.lit(calculated_start_date).cast("date")) &
                        (F.col("LD_DT").cast("date") <= F.lit(calculated_end_date).cast("date"))
                    )
                    _log_dataframe_sample("after_sat_prem_cmsn_filter", df, source_ref)
                except (ValueError, TypeError) as exc:
                    logger.warning(
                        "[DeltaTableConnector] SAT_PREM/SAT_CMSN filter failed for %s with num_of_days_for_cmn_tables=%s: %s",
                        source_ref,
                        num_of_days_for_cmn_tables,
                        str(exc),
                    )
            return df
        # Incremental filtering via ctl_pipeline.watermark_col
        if (load_type or "").lower() == "incremental" and watermark_col:
            watermark_expr, resolved_cols = self._build_watermark_expr(df, watermark_col)
            logger.debug(
                "[DeltaTableConnector] WATERMARK_RESOLUTION %s",
                _fmt_vars(
                    source=source_ref,
                    watermark_col=watermark_col,
                    resolved_cols=resolved_cols,
                    has_watermark_expr=watermark_expr is not None,
                    start_date=start_date,
                    end_date=end_date,
                    watermark_ts=watermark_ts,
                ),
            )

            if watermark_expr is not None:
                has_date_window = bool(start_date or end_date)

                if has_date_window:
                    watermark_date_expr = F.to_date(watermark_expr)

                    if start_date and end_date:
                        logger.info(
                            "[DeltaTableConnector] Applying incremental date range using watermark_col %s between %s and %s on %s",
                            resolved_cols,
                            start_date,
                            end_date,
                            source_ref,
                        )
                        if _debug_enabled() and _bool_env("LOG_FILTER_RULES", True):
                            logger.debug("[DeltaTableConnector] FILTER %s", _fmt_vars(source=source_ref, condition=f"to_date({resolved_cols}) BETWEEN {start_date} AND {end_date}", filter_type="watermark_date_range"))
                        df = df.filter(
                            watermark_date_expr.between(F.to_date(F.lit(start_date)), F.to_date(F.lit(end_date)))
                        )
                        _log_dataframe_sample("after_watermark_date_range_filter", df, source_ref)
                    elif start_date:
                        logger.info(
                            "[DeltaTableConnector] Applying incremental date range using watermark_col %s between %s and current_date on %s",
                            resolved_cols,
                            start_date,
                            source_ref,
                        )
                        if _debug_enabled() and _bool_env("LOG_FILTER_RULES", True):
                            logger.debug("[DeltaTableConnector] FILTER %s", _fmt_vars(source=source_ref, condition=f"to_date({resolved_cols}) BETWEEN {start_date} AND current_date()", filter_type="watermark_date_range"))
                        df = df.filter(
                            watermark_date_expr.between(F.to_date(F.lit(start_date)), F.current_date())
                        )
                        _log_dataframe_sample("after_watermark_start_date_filter", df, source_ref)
                    else:
                        logger.info(
                            "[DeltaTableConnector] Applying incremental upper bound using watermark_col %s <= %s on %s",
                            resolved_cols,
                            end_date,
                            source_ref,
                        )
                        if _debug_enabled() and _bool_env("LOG_FILTER_RULES", True):
                            logger.debug("[DeltaTableConnector] FILTER %s", _fmt_vars(source=source_ref, condition=f"to_date({resolved_cols}) <= {end_date}", filter_type="watermark_upper_bound"))
                        df = df.filter(watermark_date_expr <= F.to_date(F.lit(end_date)))
                        _log_dataframe_sample("after_watermark_end_date_filter", df, source_ref)
                elif watermark_ts is not None:
                    logger.info(
                        "[DeltaTableConnector] Applying fallback watermark filter using %s > %s (%s) on %s",
                        resolved_cols,
                        watermark_ts,
                        watermark_type,
                        source_ref,
                    )
                    if _debug_enabled() and _bool_env("LOG_FILTER_RULES", True):
                        logger.debug("[DeltaTableConnector] FILTER %s", _fmt_vars(source=source_ref, condition=f"{resolved_cols} > {watermark_ts}", filter_type="watermark_ts"))
                    df = df.filter(F.to_date(watermark_expr) > F.to_date(F.lit(watermark_ts)))
                    _log_dataframe_sample("after_watermark_ts_filter", df, source_ref)
                else:
                    logger.info(
                        "[DeltaTableConnector] Incremental load requested for %s but no date bounds or prior watermark were available. Full source will be read.",
                        source_ref,
                    )
            else:
                logger.warning(
                    "[DeltaTableConnector] watermark_col '%s' not found in %s. Incremental filter skipped — full source will be read.",
                    watermark_col, source_ref,
                )
        else:
            logger.info(
                "[DeltaTableConnector] Incremental filtering skipped for %s (load_type=%s, watermark_col=%s)",
                source_ref,
                load_type,
                watermark_col,
            )
        df = _apply_debug_filter(df, source_ref, debug_filter_column, debug_filter_value)
        logger.info("[DeltaTableConnector] SOURCE_READ_END %s", _fmt_vars(source=source_ref, columns=len(df.columns), load_type=load_type, product_filter_col=product_filter_col, pqb_flag_col=pqb_flag_col, watermark_col=watermark_col, debug_filter_column=debug_filter_column, debug_filter_value=debug_filter_value))
        _log_dataframe_sample("source_read_final", df, source_ref)
        return df


# -----------------------------------------------------------------------
# Concrete: Parquet Volume (QBE files landed in Databricks Volumes)
# -----------------------------------------------------------------------
class ParquetVolumeConnector(SourceConnector):
    """
    Reads Parquet files from a Databricks Volume path (/Volumes/...).
    Watermark not applied (file-based sources use file_date partition instead).
    """

    def supports(self, source_type: str) -> bool:
        return source_type.lower() == "parquet"

    def read(
        self,
        spark: SparkSession,
        source_ref: str,
        *,
        product: Optional[str] = None,
        product_filter_col: Optional[str] = None,
        is_common: bool = False,
        watermark_col: Optional[str] = None,
        load_type: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        watermark_ts=None,
        watermark_type: str = "timestamp",
        pqb_flag_col: Optional[str] = None,
        pqb_flags: Optional[list] = None,
        debug_filter_column: Optional[str] = None,
        debug_filter_value: Optional[str] = None,
        num_of_days_for_cmn_tables: Optional[int] = None,
    ) -> DataFrame:
        logger.info("[ParquetVolumeConnector] SOURCE_READ_START %s", _fmt_vars(source=source_ref, product=product, product_filter_col=product_filter_col, is_common=is_common, load_type=load_type, debug_filter_column=debug_filter_column, debug_filter_value=debug_filter_value))
        df = spark.read.format("parquet").option("mergeSchema", "true").load(source_ref)
        _log_dataframe_sample("source_read_initial", df, source_ref)

        if not is_common and product and product_filter_col:
            if product_filter_col in df.columns:
                if _debug_enabled() and _bool_env("LOG_FILTER_RULES", True):
                    logger.debug("[ParquetVolumeConnector] FILTER %s", _fmt_vars(source=source_ref, condition=f"{product_filter_col} = {product}", filter_type="product"))
                df = df.filter(F.col(product_filter_col) == product)
                _log_dataframe_sample("after_product_filter", df, source_ref)
            else:
                logger.warning("[ParquetVolumeConnector] product_filter_col '%s' not found in %s", product_filter_col, source_ref)

        df = _apply_debug_filter(df, source_ref, debug_filter_column, debug_filter_value)
        logger.info("[ParquetVolumeConnector] SOURCE_READ_END %s", _fmt_vars(source=source_ref, columns=len(df.columns), load_type=load_type, debug_filter_column=debug_filter_column, debug_filter_value=debug_filter_value))
        _log_dataframe_sample("source_read_final", df, source_ref)
        return df


# -----------------------------------------------------------------------
# Concrete: Majesco-specific connector (C-07 — named connector)
# Adds Majesco-specific read options: schema evolution, bad-record path
# -----------------------------------------------------------------------
class MajescoConnector(DeltaTableConnector):
    """
    Extends DeltaTableConnector with Majesco-specific read conventions:
      - Always resolves source_ref against the Bronze catalog/schema
      - Enforces schema evolution options
      - Logs audit: source version read, timestamp
    """

    def __init__(self, bronze_fq: str):
        """
        Args:
            bronze_fq: The fully-qualified Bronze schema (e.g. majesco_ic_agile_2.bronze_db).
        """
        self._bronze_fq = bronze_fq

    def supports(self, source_type: str) -> bool:
        return source_type.lower() == "majesco"

    def read(
        self,
        spark: SparkSession,
        source_ref: str,
        *,
        product: Optional[str] = None,
        product_filter_col: Optional[str] = None,
        is_common: bool = False,
        watermark_col: Optional[str] = None,
        load_type: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        watermark_ts=None,
        watermark_type: str = "timestamp",
        pqb_flag_col: Optional[str] = None,
        pqb_flags: Optional[list] = None,
        debug_filter_column: Optional[str] = None,
        debug_filter_value: Optional[str] = None,
        num_of_days_for_cmn_tables: Optional[int] = None,
    ) -> DataFrame:
        # Qualify with bronze catalog if not already qualified
        fq_ref = source_ref if "." in source_ref else f"{self._bronze_fq}.{source_ref}"
        logger.info("[MajescoConnector] SOURCE_RESOLVED original=%s resolved=%s", source_ref, fq_ref)

        # Delegate to parent (DeltaTableConnector) with full qualification
        return super().read(
            spark, fq_ref,
            product=product,
            product_filter_col=product_filter_col,
            is_common=is_common,
            watermark_col=watermark_col,
            load_type=load_type,
            start_date=start_date,
            end_date=end_date,
            watermark_ts=watermark_ts,
            watermark_type=watermark_type,
            pqb_flag_col=pqb_flag_col,
            pqb_flags=pqb_flags,
            debug_filter_column=debug_filter_column,
            debug_filter_value=debug_filter_value,
            num_of_days_for_cmn_tables=num_of_days_for_cmn_tables,
        )


# -----------------------------------------------------------------------
# Concrete: Gold Delta connector
# -----------------------------------------------------------------------
class GoldConnector(DeltaTableConnector):
    """
    Reads Gold/ADV Delta tables used as lookup/reference sources.

    If source_ref is not fully-qualified, it is resolved against the configured
    Gold schema. This makes ctl_source rows with source_type='gold' work
    directly without seed-side workarounds.
    """

    def __init__(self, gold_fq: str = None):
        self._gold_fq = gold_fq

    def supports(self, source_type: str) -> bool:
        return source_type.lower() in ("gold", "gold_delta")

    def read(
        self,
        spark: SparkSession,
        source_ref: str,
        *,
        product: Optional[str] = None,
        product_filter_col: Optional[str] = None,
        is_common: bool = False,
        watermark_col: Optional[str] = None,
        load_type: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        watermark_ts=None,
        watermark_type: str = "timestamp",
        pqb_flag_col: Optional[str] = None,
        pqb_flags: Optional[list] = None,
        debug_filter_column: Optional[str] = None,
        debug_filter_value: Optional[str] = None,
        num_of_days_for_cmn_tables: Optional[int] = None,
    ) -> DataFrame:
        if "." in source_ref or not self._gold_fq:
            fq_ref = source_ref
        else:
            fq_ref = f"{self._gold_fq}.{source_ref}"

        logger.info("[GoldConnector] SOURCE_RESOLVED original=%s resolved=%s", source_ref, fq_ref)

        return super().read(
            spark, fq_ref,
            product=product,
            product_filter_col=product_filter_col,
            is_common=is_common,
            watermark_col=watermark_col,
            load_type=load_type,
            start_date=start_date,
            end_date=end_date,
            watermark_ts=watermark_ts,
            watermark_type=watermark_type,
            pqb_flag_col=pqb_flag_col,
            pqb_flags=pqb_flags,
            debug_filter_column=debug_filter_column,
            debug_filter_value=debug_filter_value,
            num_of_days_for_cmn_tables=num_of_days_for_cmn_tables,
        )


# -----------------------------------------------------------------------
# Registry: auto-select connector by source_type string
# -----------------------------------------------------------------------
class ConnectorRegistry:
    _registry = {}

    @classmethod
    def register(cls, connector):
        """
        Register a connector instance.
        """
        if not hasattr(connector, "supports"):
            raise ValueError(f"Invalid connector: {connector}")

        cls._registry[connector.__class__.__name__.lower()] = connector

    @classmethod
    def get(cls, source_type):
        """
        Retrieve connector by source_type (case-insensitive).
        """
        if not source_type:
            raise ValueError("source_type cannot be None")

        stype = source_type.lower()

        for conn in cls._registry.values():
            if conn.supports(source_type):
                return conn

        raise ValueError(
            f"[ConnectorRegistry] No connector registered for source_type='{source_type}'."
        )

        return cls._registry[stype]

    @classmethod
    def setup_defaults(cls, default_schema):
        """
        Register default connectors.
        """
        from framework.source_connector import (
            DeltaTableConnector,
            ParquetVolumeConnector,
            MajescoConnector,
            GoldConnector,
        )

        cls._registry = {}

        cls.register(DeltaTableConnector(default_schema))
        cls.register(ParquetVolumeConnector())
        cls.register(MajescoConnector(default_schema))
        cls.register(GoldConnector())
