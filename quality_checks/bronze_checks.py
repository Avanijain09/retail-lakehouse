# quality_checks/bronze_checks.py
"""
Bronze layer ke baad run hota hai.
Fail hone pe Airflow pipeline rok deta hai.
"""

import sys
import os
from pyspark.sql.functions import col

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sys.path.insert(0, os.path.join(PROJECT_ROOT, "pyspark_jobs"))

from utils.spark_session import get_spark_session
from utils.config import BRONZE_PATH, TABLES


def run_bronze_checks(ingest_date: str) -> bool:
    spark = get_spark_session("BronzeQualityChecks")
    all_passed = True

    for table in TABLES:
        path = os.path.join(BRONZE_PATH, table, f"ingest_date={ingest_date}")

        # Check 1: Partition exists
        if not os.path.exists(path):
            print(f"[QC-BRONZE] FAIL {table}: partition not found at {path}")
            all_passed = False
            continue

        df = spark.read.parquet(path)
        row_count = df.count()

        # Check 2: Not empty
        if row_count == 0:
            print(f"[QC-BRONZE] FAIL {table}: 0 rows in partition!")
            all_passed = False
            continue

        # Check 3: Audit columns present
        required_cols = [
            "_ingest_timestamp",
            "_source_file",
            "_ingest_date",
            "_pipeline_version",
        ]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            print(f"[QC-BRONZE] FAIL {table}: missing audit cols {missing}")
            all_passed = False
            continue

        # Check 4: _ingest_date column matches expected date
        wrong_date = df.filter(col("_ingest_date") != ingest_date).count()
        if wrong_date > 0:
            print(
                f"[QC-BRONZE] FAIL {table}: "
                f"{wrong_date} rows have wrong _ingest_date!"
            )
            all_passed = False
            continue

        print(f"[QC-BRONZE] PASS {table}: " f"{row_count:,} rows, all checks passed")

    spark.stop()
    return all_passed


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    passed = run_bronze_checks(args.date)
    if not passed:
        print("[QC-BRONZE] Some checks FAILED — stopping pipeline")
        sys.exit(1)
    print("[QC-BRONZE] All Bronze checks PASSED")
