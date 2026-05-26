"""
Bronze layer ingestion.

Reads raw CSV files and converts them to Parquet.
No transformations are applied in Bronze layer.
"""

import argparse
import sys
import os

from pyspark.sql.functions import current_timestamp, input_file_name, lit

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from utils.spark_session import get_spark_session
from utils.config import RAW_PATH, BRONZE_PATH, TABLES
from utils.helpers import log_pipeline_step, get_process_date, write_parquet


def ingest_table(spark, table_name: str, ingest_date: str) -> int:
    """
    Ingest one table from raw CSV to Bronze parquet.
    """

    source_path = f"{RAW_PATH}/{table_name}.csv"

    output_path = f"{BRONZE_PATH}/{table_name}/" f"ingest_date={ingest_date}"

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(source_path)
    )

    input_rows = df.count()

    # Audit metadata columns

    df = (
        df.withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingest_date", lit(ingest_date))
        .withColumn("_pipeline_version", lit("1.0.0"))
    )

    output_rows = write_parquet(df=df, path=output_path, mode="overwrite")

    log_pipeline_step(
        step="BRONZE",
        table=table_name,
        input_rows=input_rows,
        output_rows=output_rows,
        process_date=ingest_date,
    )

    return output_rows


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--date", default=None, help="Process date YYYY-MM-DD")

    parser.add_argument(
        "--table", default=None, help="Run ingestion for one table only"
    )

    args = parser.parse_args()

    process_date = get_process_date(args.date)

    spark = get_spark_session("BronzeIngestion")

    tables_to_run = [args.table] if args.table else TABLES

    for table in tables_to_run:
        ingest_table(spark, table, process_date)

    spark.stop()

    print(f"[BRONZE] Ingestion completed " f"for {process_date}")


if __name__ == "__main__":
    main()
