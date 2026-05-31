"""
Gold → PostgreSQL Loader
=========================
Reads all four Gold Parquet tables and loads them into the
PostgreSQL gold schema using an idempotent DELETE + INSERT pattern.

Idempotent pattern:
  1. DELETE FROM gold.{table} WHERE _process_date = '{process_date}'
  2. INSERT all rows with that _process_date

Running this script twice for the same date produces the same result.
No duplicates are created because Step 1 always clears existing data first.

Why Pandas and not Spark JDBC?
  Gold tables are small (aggregated data — thousands of rows, not millions).
  Pandas .to_sql() is simpler and faster for this volume.
  For Gold tables > 1M rows, switch to the Spark JDBC approach
  shown in the commented-out section at the bottom.

Run:
  python3 postgres/load/load_gold_to_postgres.py --date 2024-01-15
  python3 postgres/load/load_gold_to_postgres.py --date 2024-01-15 --table daily_orders_kpi
"""

import argparse
import os
import sys
from datetime import datetime
from typing import Optional

print(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text

sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
from pyspark_jobs.utils.config import DATABASE_URL, GOLD_PATH

# ════════════════════════════════════════════════════════
# TABLE REGISTRY
# Maps Gold table name → Parquet path + delete key column
# ════════════════════════════════════════════════════════

GOLD_TABLES = {
    "daily_orders_kpi": {
        "parquet_path": os.path.join(GOLD_PATH, "daily_orders_kpi"),
        "schema": "gold",
        "delete_col": "_process_date",
    },
    "category_revenue_kpi": {
        "parquet_path": os.path.join(GOLD_PATH, "category_revenue_kpi"),
        "schema": "gold",
        "delete_col": "_process_date",
    },
    "seller_performance_kpi": {
        "parquet_path": os.path.join(GOLD_PATH, "seller_performance_kpi"),
        "schema": "gold",
        "delete_col": "_process_date",
    },
    "delivery_performance_kpi": {
        "parquet_path": os.path.join(GOLD_PATH, "delivery_performance_kpi"),
        "schema": "gold",
        "delete_col": "_process_date",
    },
}


# ════════════════════════════════════════════════════════
# CORE LOADER FUNCTION
# ════════════════════════════════════════════════════════


def load_table(
    table_name: str,
    process_date: str,
    engine,
) -> dict:
    """
    Load one Gold table for the given process_date.

    Steps:
      1. Read Gold Parquet (all partitions, filter by _process_date)
      2. Type-cast columns to match PostgreSQL DDL
      3. DELETE existing rows for this process_date (idempotent)
      4. INSERT fresh rows using pandas to_sql
      5. Verify row counts match

    Returns a summary dict.
    """
    config = GOLD_TABLES[table_name]
    parquet_dir = config["parquet_path"]
    schema = config["schema"]
    delete_col = config["delete_col"]

    print(f"\n[LOAD] {table_name}")

    # ── Step 1: Read Gold Parquet ──────────────────────
    if not os.path.exists(parquet_dir):
        raise FileNotFoundError(
            f"[LOAD] Gold path not found: {parquet_dir}\n"
            f"  Run build_all_gold.py --date {process_date} first."
        )

    # Read all partitions, then filter to this process_date only.
    # Gold partitions are by business date (purchase_date/purchase_month),
    # not by _process_date, so we read all and filter.
    df = pd.read_parquet(parquet_dir)

    # Filter to rows produced by this pipeline run
    df = df[df[delete_col].astype(str) == str(process_date)]

    if df.empty:
        print(
            f"[LOAD] {table_name}: no rows for _process_date={process_date}. "
            f"Nothing to load."
        )
        return {
            "table": table_name,
            "inserted": 0,
            "deleted": 0,
            "status": "skipped",
        }

    print(f"[LOAD] {table_name}: {len(df):,} rows read from Parquet")

    # ── Step 2: Type casting ──────────────────────────
    df = _cast_types(df, table_name)

    # ── Step 3: DROP auto-id column if present ────────
    # PostgreSQL BIGSERIAL manages 'id' — do not pass it from Parquet
    if "id" in df.columns:
        df = df.drop(columns=["id"])

    # ── Step 4: Idempotent delete ─────────────────────
    with engine.begin() as conn:
        delete_stmt = text(
            f"DELETE FROM {schema}.{table_name} " f"WHERE {delete_col} = :process_date"
        )
        result = conn.execute(delete_stmt, {"process_date": process_date})
        deleted = result.rowcount

    print(f"[LOAD] {table_name}: {deleted:,} existing rows deleted for {process_date}")

    # ── Step 5: Insert ────────────────────────────────
    df.to_sql(
        name=table_name,
        schema=schema,
        con=engine,
        if_exists="append",  # Table already exists from DDL
        index=False,  # Let PostgreSQL manage the BIGSERIAL id
        chunksize=5_000,  # Batch inserts — safe for all table sizes
        method="multi",  # Single multi-row INSERT per batch (faster)
    )

    inserted = len(df)
    print(f"[LOAD] {table_name}: {inserted:,} rows inserted → {schema}.{table_name}")

    # ── Step 6: Verification ──────────────────────────
    with engine.connect() as conn:
        result = conn.execute(
            text(
                f"SELECT COUNT(*) FROM {schema}.{table_name} "
                f"WHERE {delete_col} = :process_date"
            ),
            {"process_date": process_date},
        )
        pg_count = result.scalar()

    if pg_count != inserted:
        raise ValueError(
            f"[LOAD] VERIFICATION FAILED for {table_name}!\n"
            f"  Pandas inserted: {inserted:,}\n"
            f"  PostgreSQL count: {pg_count:,}"
        )

    print(f"[LOAD] {table_name}: verified {pg_count:,} rows in PostgreSQL ✓")

    return {
        "table": table_name,
        "inserted": inserted,
        "deleted": deleted,
        "status": "success",
    }


def _cast_types(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Cast Parquet column types to PostgreSQL-compatible types.

    Key issues prevented:
      - _process_date arrives as string in Gold Parquet → cast to date
      - purchase_date arrives as pandas Timestamp → cast to date
      - INTEGER cols may arrive as float64 due to NaN handling → cast to Int64
      - NUMERIC cols: pandas float64 is fine, PostgreSQL accepts it
    """

    # Date columns: cast string/Timestamp → datetime.date
    date_cols = ["_process_date", "purchase_date"]
    for c in date_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c]).dt.date

    # Integer columns that PySpark may write as float64 when NaN exists
    # (pd.to_numeric with downcast=integer fails silently — use Int64)
    integer_cols = [
        "total_orders",
        "total_unique_customers",
        "delivered_count",
        "cancelled_count",
        "shipped_count",
        "processing_count",
        "invoiced_count",
        "approved_count",
        "on_time_count",
        "total_items_sold",
        "total_orders",
        "delivered_orders",
        "cancelled_orders",
        "on_time_orders",
    ]
    for c in integer_cols:
        if c in df.columns:
            # Use pandas nullable Int64 to handle NaN without casting to float
            df[c] = df[c].round(0).astype("Int64")

    return df


# ════════════════════════════════════════════════════════
# ORCHESTRATOR
# ════════════════════════════════════════════════════════


def run_load(
    process_date: str,
    tables: Optional[list] = None,
) -> bool:
    """
    Load all (or specified) Gold tables into PostgreSQL.
    Returns True if all tables loaded successfully.
    """
    tables_to_load = tables if tables else list(GOLD_TABLES.keys())

    print(f"\n{'='*60}")
    print(f"GOLD → POSTGRES LOAD — {process_date}")
    print(f"Tables: {tables_to_load}")
    print(f"{'='*60}")

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    results = {}
    failed = []

    for table in tables_to_load:
        if table not in GOLD_TABLES:
            print(f"[LOAD] Unknown table '{table}' — skipping")
            continue
        try:
            summary = load_table(table, process_date, engine)
            results[table] = summary
        except Exception as e:
            print(f"[LOAD] FAILED {table}: {e}")
            results[table] = {"table": table, "status": "failed", "error": str(e)}
            failed.append(table)

    engine.dispose()

    # ── Summary ──────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"LOAD SUMMARY — {process_date}")
    print(f"{'─'*60}")
    for table, r in results.items():
        status = r.get("status", "unknown").upper()
        inserted = r.get("inserted", 0)
        print(f"  {table:<38} {inserted:>8,} rows  [{status}]")
    print(f"{'='*60}")

    if failed:
        print(f"\nFailed tables: {failed}")
        return False

    print(f"\nAll tables loaded successfully.")
    return True


# ════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(
        description="Load Gold Parquet tables into PostgreSQL gold schema."
    )
    parser.add_argument(
        "--date",
        default=datetime.today().strftime("%Y-%m-%d"),
        help="Process date YYYY-MM-DD (default: today)",
    )
    parser.add_argument(
        "--table",
        default=None,
        choices=list(GOLD_TABLES.keys()),
        help="Load a single table only (default: all)",
    )
    args = parser.parse_args()

    tables = [args.table] if args.table else None

    success = run_load(args.date, tables)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
