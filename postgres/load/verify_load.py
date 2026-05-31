"""
PostgreSQL Load Verification
=============================
After load_gold_to_postgres.py runs, this script verifies:
  1. Each Gold table has rows for the given process_date
  2. Row counts in PostgreSQL match Gold Parquet row counts
  3. Key columns have no unexpected NULLs
  4. Basic sanity on numeric ranges (no negative revenue etc.)

Run:
  python3 postgres/load/verify_load.py --date 2024-01-15
"""

import argparse
import os
import sys
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
from pyspark_jobs.utils.config import DATABASE_URL, GOLD_PATH

CHECKS = {
    "daily_orders_kpi": {
        "date_col": "_process_date",
        "not_null_cols": ["purchase_date", "customer_state", "total_orders"],
        "positive_cols": ["total_orders"],
        "pct_cols": ["delivery_rate_pct", "on_time_rate_pct"],
    },
    "category_revenue_kpi": {
        "date_col": "_process_date",
        "not_null_cols": ["purchase_month", "product_category_name", "total_revenue"],
        "positive_cols": ["total_revenue", "total_gmv"],
        "pct_cols": ["freight_pct_of_gmv"],
    },
    "seller_performance_kpi": {
        "date_col": "_process_date",
        "not_null_cols": ["purchase_month", "seller_id", "total_revenue"],
        "positive_cols": ["total_revenue", "total_gmv"],
        "pct_cols": ["freight_pct_of_gmv"],
    },
    "delivery_performance_kpi": {
        "date_col": "_process_date",
        "not_null_cols": ["purchase_month", "customer_state", "total_orders"],
        "positive_cols": ["total_orders"],
        "pct_cols": ["delivery_rate_pct", "on_time_rate_pct"],
    },
}


def run_verification(process_date: str) -> bool:
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    all_pass = True
    results = []

    for table, cfg in CHECKS.items():
        date_col = cfg["date_col"]
        parquet_dir = os.path.join(GOLD_PATH, table)

        # ── Check 1: Rows exist in PostgreSQL ────────────
        cur.execute(
            f"SELECT COUNT(*) AS cnt FROM gold.{table} " f"WHERE {date_col} = %s",
            (process_date,),
        )
        pg_count = cur.fetchone()["cnt"]
        passed = pg_count > 0
        results.append((table, "row_count_gt_0", passed, pg_count, "> 0"))
        if not passed:
            all_pass = False

        # ── Check 2: PostgreSQL rows match Parquet ────────
        if os.path.exists(parquet_dir):
            df = pd.read_parquet(parquet_dir)
            parquet_count = len(df[df[date_col].astype(str) == str(process_date)])
            match = pg_count == parquet_count
            results.append(
                (
                    table,
                    "pg_matches_parquet",
                    match,
                    f"pg={pg_count:,} parquet={parquet_count:,}",
                    "equal",
                )
            )
            if not match:
                all_pass = False

        # ── Check 3: No NULLs in mandatory columns ────────
        for col in cfg["not_null_cols"]:
            cur.execute(
                f"SELECT COUNT(*) AS cnt FROM gold.{table} "
                f"WHERE {date_col} = %s AND {col} IS NULL",
                (process_date,),
            )
            null_count = cur.fetchone()["cnt"]
            passed = null_count == 0
            results.append((table, f"no_null_{col}", passed, null_count, "0"))
            if not passed:
                all_pass = False

        # ── Check 4: Positive numeric columns ─────────────
        for col in cfg["positive_cols"]:
            cur.execute(
                f"SELECT COUNT(*) AS cnt FROM gold.{table} "
                f"WHERE {date_col} = %s AND {col} < 0",
                (process_date,),
            )
            neg_count = cur.fetchone()["cnt"]
            passed = neg_count == 0
            results.append((table, f"positive_{col}", passed, neg_count, "0"))
            if not passed:
                all_pass = False

        # ── Check 5: Percentage columns in 0-100 range ────
        for col in cfg["pct_cols"]:
            cur.execute(
                f"SELECT COUNT(*) AS cnt FROM gold.{table} "
                f"WHERE {date_col} = %s "
                f"  AND {col} IS NOT NULL "
                f"  AND ({col} < 0 OR {col} > 100)",
                (process_date,),
            )
            bad_count = cur.fetchone()["cnt"]
            passed = bad_count == 0
            results.append((table, f"pct_range_{col}", passed, bad_count, "0"))
            if not passed:
                all_pass = False

    conn.close()

    # ── Print results ──────────────────────────────────────
    col_w = 48
    print(f"\n{'='*80}")
    print(f"  POSTGRES LOAD VERIFICATION — {process_date}")
    print(f"{'='*80}")

    prev_table = None
    for table, check, passed, value, expected in results:
        if table != prev_table:
            print(f"\n  TABLE: {table.upper()}")
            prev_table = table

        status = "PASS" if passed else "FAIL"
        print(f"    [{status}] {check:<{col_w}} value={value}")

    overall = "PASS" if all_pass else "FAIL"
    print(f"\n{'='*80}")
    print(f"  OVERALL: [{overall}]")
    print(f"{'='*80}\n")

    return all_pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=datetime.today().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    passed = run_verification(args.date)
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
