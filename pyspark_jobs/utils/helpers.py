"""
Shared utility functions used across Bronze, Silver, Gold jobs.
"""

from pyspark.sql import DataFrame, SparkSession
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    """
    Read parquet file and log row count.
    """

    df = spark.read.parquet(path)

    logger.info(f"[READ] {path} → {df.count()} rows")

    return df


def write_parquet(
    df: DataFrame, path: str, partition_by: str = None, mode: str = "overwrite"
) -> int:
    """
    Write dataframe as parquet and return row count.
    """

    count = df.count()

    writer = df.write.mode(mode)

    if partition_by:
        writer = writer.partitionBy(partition_by)

    writer.parquet(path)

    logger.info(f"[WRITE] {path} ← {count} rows")

    return count


def log_pipeline_step(
    step: str, table: str, input_rows: int, output_rows: int, process_date: str
) -> None:
    """
    Standardized pipeline logging.
    """

    dropped = input_rows - output_rows

    pct = round(dropped / input_rows * 100, 1) if input_rows > 0 else 0

    print(
        f"[PIPELINE] {process_date} | "
        f"{step:10s} | "
        f"{table:20s} | "
        f"in={input_rows:,} "
        f"out={output_rows:,} "
        f"dropped={dropped:,} ({pct}%)"
    )


def get_process_date(date_arg: str = None) -> str:
    """
    Return processing date.
    """

    if date_arg:
        return date_arg

    return datetime.today().strftime("%Y-%m-%d")
