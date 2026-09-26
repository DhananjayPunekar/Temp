
import logging
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, TimestampType,
)

logger = logging.getLogger(__name__)
logging.getLogger("py4j").setLevel(logging.ERROR)


def fmt_vars(**kwargs) -> str:
    return " ".join(f"{k}={v}" for k, v in kwargs.items())


class RunLogger:
    SCHEMA = StructType([
        StructField("run_id", StringType(), True),
        StructField("batch_id", StringType(), True),
        StructField("pipeline_id", StringType(), True),
        StructField("product_code", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("load_type", StringType(), True),
        StructField("rows_inserted", LongType(), True),
        StructField("rows_updated", LongType(), True),
        StructField("rows_affected", LongType(), True),
        StructField("watermark_ts", TimestampType(), True),
        StructField("new_watermark_ts", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("start_ts", TimestampType(), True),
        StructField("end_ts", TimestampType(), True)
    ])

    def __init__(self, spark: SparkSession, settings, run_id: str):
        self.spark = spark
        self.settings = settings
        self.run_id = run_id

    def log(self, **kwargs: Any) -> None:
        record = {
            "run_id": self.run_id,
            "batch_id": kwargs.get("batch_id"),
            "pipeline_id": kwargs.get("pipeline_id"),
            "product_code": kwargs.get("product_code"),
            "target_table": kwargs.get("target_table"),
            "load_type": kwargs.get("load_type"),
            "rows_inserted": kwargs.get("rows_inserted"),
            "rows_updated": kwargs.get("rows_updated"),
            "rows_affected": kwargs.get("rows_affected"),
            "watermark_ts": kwargs.get("watermark_ts"),
            "new_watermark_ts": kwargs.get("new_watermark_ts"),
            "status": kwargs.get("status"),
            "error_message": kwargs.get("error_message"),
            "start_ts": kwargs.get("start_ts"),
            "end_ts": kwargs.get("end_ts"),
        }
        logger.info("[RUN_LOG_WRITE] %s", fmt_vars(status=record["status"], target_table=record["target_table"]))
        # Spark Connect (serverless) Arrow serialization rejects Python None for TimestampType;
        # use range(1).select with explicit casts so null timestamps are handled safely.
        df = self.spark.range(1).select(
            F.lit(record.get("run_id")).alias("run_id"),
            F.lit(record.get("batch_id")).alias("batch_id"),
            F.lit(record.get("pipeline_id")).alias("pipeline_id"),
            F.lit(record.get("product_code")).alias("product_code"),
            F.lit(record.get("target_table")).alias("target_table"),
            F.lit(record.get("load_type")).alias("load_type"),
            F.lit(record.get("rows_inserted")).cast("long").alias("rows_inserted"),
            F.lit(record.get("rows_updated")).cast("long").alias("rows_updated"),
            F.lit(record.get("rows_affected")).cast("long").alias("rows_affected"),
            F.lit(record.get("watermark_ts")).cast("timestamp").alias("watermark_ts"),
            F.lit(record.get("new_watermark_ts")).cast("timestamp").alias("new_watermark_ts"),
            F.lit(record.get("status")).alias("status"),
            F.lit(record.get("error_message")).alias("error_message"),
            F.lit(record.get("start_ts")).cast("timestamp").alias("start_ts"),
            F.lit(record.get("end_ts")).cast("timestamp").alias("end_ts"),
        )
        df.write.format("delta").mode("append").saveAsTable(f"{self.settings.control_fq}.run_log")
