"""
Bronze Layer Ingestion Job
==========================
Responsibility: Raw CSV → Parquet, with audit metadata.
Rule: ZERO transformation. Data exactly as received from source.

Run: python pyspark_jobs/bronze/ingest_raw.py --date 2024-01-15
     python pyspark_jobs/bronze/ingest_raw.py --date 2024-01-15 --table orders
"""

import argparse
import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit, input_file_name

# ── Path setup so utils imports work ───────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import get_spark_session
from utils.config import RAW_PATH, BRONZE_PATH, TABLES

# ════════════════════════════════════════════════════════
# CORE FUNCTION — ingest one table
# ════════════════════════════════════════════════════════


def ingest_table_to_bronze(
    spark: SparkSession, table_name: str, ingest_date: str, source_path: str = None
) -> dict:
    """
    Ek table ko Raw CSV se Bronze Parquet mein convert karo.

    Parameters:
        spark       : SparkSession
        table_name  : e.g. 'orders', 'order_items'
        ingest_date : 'YYYY-MM-DD' format
        source_path : override default path (testing ke liye)

    Returns:
        dict with status, row_count, output_path
    """

    # ── 1. Source path decide karo ──────────────────────
    if source_path is None:
        source_path = os.path.join(RAW_PATH, f"{table_name}.csv")

    output_path = os.path.join(BRONZE_PATH, table_name, f"ingest_date={ingest_date}")

    print(f"\n[BRONZE] ── Starting {table_name} ──")
    print(f"[BRONZE]   Source : {source_path}")
    print(f"[BRONZE]   Target : {output_path}")

    # ── 2. Source file exist karta hai? ─────────────────
    # Production mein yeh FileSensor handle karta hai
    # Lekin ek extra check rakhna good practice hai
    if not os.path.exists(source_path):
        raise FileNotFoundError(
            f"[BRONZE] Source file not found: {source_path}\n"
            f"  Ensure Airflow FileSensor waited for file arrival."
        )

    # ── 3. CSV padhho — NO schema enforcement ────────────
    # Bronze mein inferSchema=True isliye rakha hai:
    # Agar source system schema change kare, Bronze capture kar le
    # Silver mein enforce karenge
    df: DataFrame = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("escape", '"')  # quoted fields handle karo
        .option("multiLine", "true")  # multi-line strings handle karo
        .csv(source_path)
    )

    raw_count = df.count()
    print(f"[BRONZE]   Raw rows read : {raw_count:,}")

    # ── 4. Empty file check ──────────────────────────────
    if raw_count == 0:
        raise ValueError(
            f"[BRONZE] EMPTY FILE: {source_path} has 0 rows!\n"
            f"  This is likely a source system issue. Failing pipeline."
        )

    # ── 5. Audit metadata columns add karo ──────────────
    # Yeh 4 columns har Bronze table mein hamesha hote hain
    # Inhe kabhi remove mat karna
    df_with_audit = (
        df.withColumn(
            "_ingest_timestamp",
            current_timestamp(),
            # kab Bronze mein aaya — exact datetime
        )
        .withColumn(
            "_source_file",
            input_file_name(),
            # exact file path jisse aaya — audit trail
        )
        .withColumn(
            "_ingest_date",
            lit(ingest_date),
            # partition key — Spark isse folder name mein use karega
        )
        .withColumn(
            "_pipeline_version",
            lit("1.0.0"),
            # agar code change ho toh version bump karo
            # debugging mein helpful: "yeh row v1.0.0 code se bani thi"
        )
    )

    # ── 6. Write Parquet — partitioned by ingest_date ───
    # mode="overwrite": agar aaj ka partition already hai
    # (rerun case mein) toh overwrite karo — idempotent
    (
        df_with_audit.write.mode("overwrite").parquet(output_path)
        # Note: partitionBy yahan nahi kiya
        # kyunki hum already specific partition path pe likh rahe hain
        # output_path mein ingest_date=YYYY-MM-DD already hai
    )

    # ── 7. Verify write ──────────────────────────────────
    # Read back karo confirm karne ke liye
    written_count = spark.read.parquet(output_path).count()
    print(f"[BRONZE]   Written rows : {written_count:,}")

    if written_count != raw_count:
        raise ValueError(
            f"[BRONZE] ROW COUNT MISMATCH!\n"
            f"  Read: {raw_count:,} | Written: {written_count:,}\n"
            f"  Possible disk issue or Spark write error."
        )

    print(f"[BRONZE]   Status : SUCCESS")

    return {
        "table": table_name,
        "ingest_date": ingest_date,
        "status": "success",
        "raw_rows": raw_count,
        "written_rows": written_count,
        "output_path": output_path,
    }


# ════════════════════════════════════════════════════════
# METADATA LOGGING — audit.pipeline_runs mein save karo
# ════════════════════════════════════════════════════════


def log_bronze_run(result: dict, error: str = None):
    """
    Har Bronze run ka record PostgreSQL audit table mein daalo.
    Yeh monitoring ke liye hota hai — kitni rows aayein, kab, status kya.
    """
    try:
        import psycopg2
        from utils.config import DATABASE_URL

        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO audit.pipeline_runs
                (dag_id, run_date, table_name, layer,
                 input_rows, output_rows, status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
            (
                "retail_daily_pipeline",
                result.get("ingest_date"),
                result.get("table"),
                "bronze",
                result.get("raw_rows", 0),
                result.get("written_rows", 0),
                result.get("status", "failed"),
                error,
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        # Audit logging fail hone se pipeline fail nahi honi chahiye
        print(f"[BRONZE] Warning: Audit log failed: {e}")


# ════════════════════════════════════════════════════════
# MAIN — CLI entry point
# ════════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(
        description="Bronze Layer: Ingest raw CSVs to Parquet"
    )
    parser.add_argument(
        "--date",
        default=datetime.today().strftime("%Y-%m-%d"),
        help="Ingest date YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--table", default=None, help="Single table to ingest (default: all tables)"
    )
    args = parser.parse_args()

    ingest_date = args.date
    tables_to_run = [args.table] if args.table else TABLES

    print(f"\n{'='*55}")
    print(f"BRONZE INGESTION — {ingest_date}")
    print(f"Tables: {tables_to_run}")
    print(f"{'='*55}")

    spark = get_spark_session("BronzeIngestion")

    results = []
    failed_tables = []

    for table in tables_to_run:
        try:
            result = ingest_table_to_bronze(spark, table, ingest_date)
            log_bronze_run(result)
            results.append(result)

        except FileNotFoundError as e:
            print(f"[BRONZE] SKIP {table}: {e}")
            # Dimension tables like 'stores' might not have daily files
            # Only fail for transactional tables
            if table in ["orders", "order_items"]:
                failed_tables.append(table)
            log_bronze_run(
                {
                    "table": table,
                    "ingest_date": ingest_date,
                    "raw_rows": 0,
                    "written_rows": 0,
                },
                error=str(e),
            )

        except Exception as e:
            print(f"[BRONZE] FAILED {table}: {e}")
            failed_tables.append(table)
            log_bronze_run(
                {
                    "table": table,
                    "ingest_date": ingest_date,
                    "raw_rows": 0,
                    "written_rows": 0,
                    "status": "failed",
                },
                error=str(e),
            )

    # ── Summary ──────────────────────────────────────────
    print(f"\n{'='*55}")
    print(f"BRONZE SUMMARY — {ingest_date}")
    for r in results:
        print(f"  {r['table']:<20} {r['written_rows']:>10,} rows  [{r['status']}]")

    if failed_tables:
        print(f"\n  FAILED: {failed_tables}")
        spark.stop()
        sys.exit(1)  # Airflow ko signal karo ki DAG fail kare
    else:
        print(f"\n  All tables ingested successfully.")

    spark.stop()


if __name__ == "__main__":
    main()
