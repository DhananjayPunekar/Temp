# =====================================================================
# framework/silver_connector.py   (Silver -> Gold/ADV extension, v1.0)
#
# Source connector for the Gold layer. The Gold (ADV Data Vault) pipelines
# read their inputs from the SILVER canonical schema, NOT from Bronze.
#
# It mirrors MajescoConnector but resolves bare table names against the
# Silver schema (settings.silver_fq) and adds the standard QBE record-source
# filter REC_SRC_NM = '109' is applied later by the rule engine, not here.
#
# Register it at startup ALONGSIDE the defaults:
#     from framework.source_connector import ConnectorRegistry
#     from framework.silver_connector import SilverConnector
#     ConnectorRegistry.setup_defaults(S.silver_fq)     # bronze fallback -> silver
#     ConnectorRegistry.register(SilverConnector(S.silver_fq))
#
# NOTE: In the seed SQL the source_type is set to 'silver'. Bare table names
#       (e.g. plcy_dtl) are auto-qualified to <catalog>.<silver_schema>.plcy_dtl.
#       Fully-qualified names and /Volumes/ paths are passed through untouched.
# =====================================================================
import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from framework.source_connector import DeltaTableConnector

logger = logging.getLogger(__name__)
logging.getLogger("py4j").setLevel(logging.ERROR)



class SilverConnector(DeltaTableConnector):
    """
    Reads Silver canonical Delta tables for the Gold/ADV Data Vault build.

    - Resolves bare source_ref against the Silver schema.
    - Honors product pushdown (when the silver table carries a product column)
      and the incremental watermark (LD_DT / _ingest_ts), inherited from
      DeltaTableConnector.
    """

    def __init__(self, silver_fq: str):
        """
        Args:
            silver_fq: Fully-qualified Silver schema, e.g. dsi_dev.silver_db.
        """
        self._silver_fq = silver_fq

    def supports(self, source_type: str) -> bool:
        return source_type.lower() in ("silver", "silver_delta")

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
        # Qualify with the Silver schema unless already qualified or a Volume path
        if "." in source_ref or source_ref.startswith("/"):
            fq_ref = source_ref
        else:
            fq_ref = f"{self._silver_fq}.{source_ref}"

        logger.info("[SilverConnector] Reading Silver source  : %s", fq_ref)
        logger.info("[SilverConnector]   product             : %s", product or "ALL")
        logger.info("[SilverConnector]   product_filter_col  : %s", product_filter_col)
        logger.info("[SilverConnector]   is_common           : %s", is_common)
        logger.info("[SilverConnector]   watermark_col       : %s", watermark_col)
        logger.info("[SilverConnector]   load_type           : %s", load_type)
        logger.info("[SilverConnector]   start_date          : %s", start_date)
        logger.info("[SilverConnector]   end_date            : %s", end_date)
        logger.info("[SilverConnector]   watermark_ts        : %s", watermark_ts)
        logger.info("[SilverConnector]   watermark_type      : %s", watermark_type)
        logger.info("[SilverConnector]   debug_filter_column : %s", debug_filter_column)
        logger.info("[SilverConnector]   debug_filter_value  : %s", debug_filter_value)

        df = super().read(
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
            num_of_days_for_cmn_tables=num_of_days_for_cmn_tables
        )
        logger.info("[SilverConnector] Source loaded: %s  columns=%s", fq_ref, df.columns)
        return df
