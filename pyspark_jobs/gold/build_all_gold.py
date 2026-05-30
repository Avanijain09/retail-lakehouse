"""
Gold Layer Orchestrator — build_all_gold.py
=============================================
Builds all four Gold tables for a given process_date.

Execution order:
  1. daily_orders_kpi          (orders + customers)
  2. category_revenue_kpi      (order_items + orders + products)
  3. seller_performance_kpi    (order_items + orders + stores)
  4. delivery_performance_kpi  (orders + customers)

Tables 1 and 4 share the same Silver sources (orders + customers).
Tables 2 and 3 share order_items and orders as sources.
No inter-Gold dependencies — all four can run in any order or in
parallel with a Celery executor in Airflow.

Run (all tables):
  python3 pyspark_jobs/gold/build_all_gold.py --date 2024-01-15

Run (single table):
  python3 pyspark_jobs/gold/build_all_gold.py --date 2024-01-15 --table category_revenue_kpi
"""

import argparse
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import get_spark_session

from gold.daily_orders_kpi import run_daily_orders_kpi
from gold.category_revenue_kpi import run_category_revenue_kpi
from gold.seller_performance_kpi import run_seller_performance_kpi
from gold.delivery_performance_kpi import run_delivery_performance_kpi

GOLD_TABLES = [
    "daily_orders_kpi",
    "category_revenue_kpi",
    "seller_performance_kpi",
    "delivery_performance_kpi",
]

RUNNERS = {
    "daily_orders_kpi": run_daily_orders_kpi,
    "category_revenue_kpi": run_category_revenue_kpi,
    "seller_performance_kpi": run_seller_performance_kpi,
    "delivery_performance_kpi": run_delivery_performance_kpi,
}


def main():
    parser = argparse.ArgumentParser(
        description="Gold layer orchestrator — builds all four Gold tables."
    )
    parser.add_argument(
        "--date",
        default=datetime.today().strftime("%Y-%m-%d"),
        help="Process date YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--table",
        default=None,
        choices=GOLD_TABLES,
        help="Build a single Gold table only (default: all)",
    )
    args = parser.parse_args()

    tables_to_run = [args.table] if args.table else GOLD_TABLES

    print(f"\n{'='*60}")
    print(f"GOLD BUILD ALL — {args.date}")
    print(f"Tables: {tables_to_run}")
    print(f"{'='*60}")

    spark = get_spark_session(app_name=f"GoldBuildAll_{args.date}", memory="4g")

    results = {}
    failed_tables = []

    for table in tables_to_run:
        try:
            row_count = RUNNERS[table](spark, args.date)
            results[table] = {"rows": row_count, "status": "success"}
        except Exception as e:
            print(f"\n[GOLD] FAILED: {table} — {e}")
            results[table] = {"rows": 0, "status": "failed", "error": str(e)}
            failed_tables.append(table)

    # ── Summary ──────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"GOLD BUILD SUMMARY — {args.date}")
    print(f"{'─'*60}")
    for table, result in results.items():
        status = result["status"].upper()
        rows = result.get("rows", 0)
        print(f"  {table:<38} {rows:>8,} rows  [{status}]")
    print(f"{'='*60}")

    spark.stop()

    if failed_tables:
        print(f"\nFailed tables: {failed_tables}")
        print("Fix failures before loading to PostgreSQL.")
        sys.exit(1)

    print(f"\nAll Gold tables built successfully for {args.date}.")
    sys.exit(0)


if __name__ == "__main__":
    main()
