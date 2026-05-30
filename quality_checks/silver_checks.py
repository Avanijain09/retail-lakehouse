"""
Silver Layer Quality Checks — silver_checks.py
================================================
Validates all five Silver table partitions for a given process date.

Checks performed per table:
  1. Silver partition exists on disk
  2. Row count > 0
  3. Surrogate key column is present and fully unique
  4. Mandatory columns contain zero null values
  5. All required Silver metadata columns are present
  6. Schema — required columns exist in the DataFrame
  7. Quarantine partition row count (informational)
  8. Retention percentage vs Bronze partition (warn if < threshold)
  9. _quality_score column — all values are 1.0

Final output:
  - Per-check PASS / FAIL with values
  - Per-table summary
  - Overall PASS / FAIL across all tables
  - Exit code 0 (all passed) or 1 (any failure) for Airflow integration

Run:
  python3 quality_checks/silver_checks.py --date 2024-01-15
  python3 quality_checks/silver_checks.py --date 2024-01-15 --table orders
"""

import argparse
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, isnull

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pyspark_jobs.utils.config import (
    BRONZE_PATH,
    DATABASE_URL,
    QUARANTINE_PATH,
    SILVER_PATH,
)
from pyspark_jobs.utils.spark_session import get_spark_session

# ════════════════════════════════════════════════════════
# TABLE CONFIGURATIONS
# ════════════════════════════════════════════════════════

# Each entry defines the contract for one Silver table.
# silver_checks.py reads these configs — no import from Silver jobs.
#
# surrogate_key      : column name of the SHA-256 SK generated in Silver
# mandatory_cols     : columns that must have zero nulls in Silver output
# required_cols      : minimum set of columns that must exist in the DataFrame
#                      (subset check — additional columns are acceptable)
# retention_warn_pct : warn if Silver retains fewer rows than this % of Bronze
# quality_score_col  : column holding the per-row quality score (always 1.0)

TABLE_CONFIGS: Dict[str, Dict[str, Any]] = {
    "orders": {
        "surrogate_key": "order_sk",
        "mandatory_cols": [
            "order_id",
            "customer_id",
        ],
        "required_cols": [
            "order_id",
            "customer_id",
            "order_status",
            "order_sk",
            "_silver_timestamp",
            "_silver_date",
            "_quality_score",
            "_ingest_timestamp",
            "_source_file",
            "_ingest_date",
        ],
        "retention_warn_pct": 90.0,
        "quality_score_col": "_quality_score",
    },
    "order_items": {
        "surrogate_key": "item_sk",
        "mandatory_cols": [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "price",
        ],
        "required_cols": [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "price",
            "freight_value",
            "total_item_value",
            "item_sk",
            "_silver_timestamp",
            "_silver_date",
            "_quality_score",
            "_ingest_timestamp",
            "_source_file",
            "_ingest_date",
        ],
        "retention_warn_pct": 90.0,
        "quality_score_col": "_quality_score",
    },
    "customers": {
        "surrogate_key": "customer_sk",
        "mandatory_cols": [
            "customer_id",
            "customer_unique_id",
            "customer_state",
        ],
        "required_cols": [
            "customer_id",
            "customer_unique_id",
            "customer_state",
            "customer_city",
            "customer_zip_code_prefix",
            "customer_sk",
            "_silver_timestamp",
            "_silver_date",
            "_quality_score",
            "_ingest_timestamp",
            "_source_file",
            "_ingest_date",
        ],
        "retention_warn_pct": 90.0,
        "quality_score_col": "_quality_score",
    },
    "products": {
        "surrogate_key": "product_sk",
        "mandatory_cols": [
            "product_id",
        ],
        "required_cols": [
            "product_id",
            "product_category_name",
            "product_photos_qty",
            "product_sk",
            "_silver_timestamp",
            "_silver_date",
            "_quality_score",
            "_ingest_timestamp",
            "_source_file",
            "_ingest_date",
        ],
        "retention_warn_pct": 90.0,
        "quality_score_col": "_quality_score",
    },
    "stores": {
        "surrogate_key": "seller_sk",
        "mandatory_cols": [
            "seller_id",
            "seller_state",
        ],
        "required_cols": [
            "seller_id",
            "seller_state",
            "seller_city",
            "seller_zip_code_prefix",
            "seller_sk",
            "_silver_timestamp",
            "_silver_date",
            "_quality_score",
            "_ingest_timestamp",
            "_source_file",
            "_ingest_date",
        ],
        "retention_warn_pct": 90.0,
        "quality_score_col": "_quality_score",
    },
}


# ════════════════════════════════════════════════════════
# CHECK RESULT DATACLASS (plain dict for simplicity)
# ════════════════════════════════════════════════════════


def make_result(
    table: str,
    check_name: str,
    passed: bool,
    value: Any,
    expected: Any = None,
    warning: bool = False,
) -> Dict[str, Any]:
    """
    Construct a uniform check result dictionary.

    warning=True means the check did not FAIL the pipeline but
    the value is worth investigating (e.g. retention < threshold).
    """
    return {
        "table": table,
        "check": check_name,
        "passed": passed,
        "warning": warning,
        "value": value,
        "expected": expected,
    }


# ════════════════════════════════════════════════════════
# INDIVIDUAL CHECK FUNCTIONS
# ════════════════════════════════════════════════════════


def check_partition_exists(
    table: str,
    process_date: str,
) -> Tuple[bool, str, Dict]:
    """
    Check 1: Silver partition folder exists on disk.

    This is the first gate — if the partition folder does not exist,
    the Silver cleaning job did not complete for this date. All
    subsequent checks for this table are skipped.

    Returns (partition_exists, silver_path, result_dict).
    """
    silver_path = os.path.join(SILVER_PATH, table, f"date={process_date}")
    exists = (
        os.path.exists(silver_path)
        and len([f for f in os.listdir(silver_path) if f.endswith(".parquet")]) > 0
    )

    result = make_result(
        table,
        "partition_exists",
        passed=exists,
        value=silver_path,
        expected="directory with *.parquet files",
    )
    return exists, silver_path, result


def check_row_count(
    df: DataFrame,
    table: str,
) -> Dict:
    """
    Check 2: Silver partition contains at least one row.

    Zero rows after Silver cleaning means either the source file
    was empty or every single row was quarantined — both indicate
    a serious upstream issue requiring immediate investigation.
    """
    count = df.count()
    passed = count > 0

    return make_result(
        table,
        "row_count_gt_0",
        passed=passed,
        value=count,
        expected="> 0",
    )


def check_surrogate_key_uniqueness(
    df: DataFrame,
    table: str,
    sk_col: str,
) -> Dict:
    """
    Check 3: Surrogate key column exists and every value is unique.

    A duplicate surrogate key means deduplication failed upstream
    or the SK generation formula collided — either case corrupts
    Gold layer joins on this dimension/fact table.
    """
    if sk_col not in df.columns:
        return make_result(
            table,
            f"sk_unique ({sk_col})",
            passed=False,
            value=f"column '{sk_col}' not found in Silver schema",
            expected="column present, all values unique",
        )

    total_rows = df.count()
    unique_keys = df.select(sk_col).distinct().count()
    collisions = total_rows - unique_keys
    passed = collisions == 0

    return make_result(
        table,
        f"sk_unique ({sk_col})",
        passed=passed,
        value=f"total={total_rows:,} unique={unique_keys:,} collisions={collisions:,}",
        expected="collisions = 0",
    )


def check_mandatory_nulls(
    df: DataFrame,
    table: str,
    mandatory_cols: List[str],
) -> List[Dict]:
    """
    Check 4: Each mandatory column contains zero null values.

    Nulls in mandatory columns after Silver cleaning mean the
    quarantine logic failed to catch them — a critical bug that
    would silently corrupt Gold KPIs.

    Returns one result dict per mandatory column.
    """
    results = []
    actual_cols = df.columns

    for col_name in mandatory_cols:
        if col_name not in actual_cols:
            results.append(
                make_result(
                    table,
                    f"mandatory_not_null ({col_name})",
                    passed=False,
                    value=f"column '{col_name}' missing from Silver schema",
                    expected="column present, 0 nulls",
                )
            )
            continue

        null_count = df.filter(isnull(col(col_name))).count()
        passed = null_count == 0

        results.append(
            make_result(
                table,
                f"mandatory_not_null ({col_name})",
                passed=passed,
                value=f"{null_count:,} nulls",
                expected="0 nulls",
            )
        )

    return results


def check_required_columns_present(
    df: DataFrame,
    table: str,
    required_cols: List[str],
) -> Dict:
    """
    Check 5: All required columns are present in the Silver DataFrame.

    Missing required columns indicate a schema drift in the Silver
    cleaning job — a column was renamed, dropped, or not generated.
    This would cause runtime errors in downstream Gold jobs.
    """
    actual_cols = set(df.columns)
    missing = [c for c in required_cols if c not in actual_cols]
    passed = len(missing) == 0

    return make_result(
        table,
        "required_columns_present",
        passed=passed,
        value=f"missing={missing}" if missing else "all present",
        expected=f"{len(required_cols)} required columns all present",
    )


def check_quality_score(
    df: DataFrame,
    table: str,
    score_col: str,
) -> Dict:
    """
    Check 6: _quality_score column — all values must be 1.0.

    Every row reaching Silver output passed all quality checks and
    received _quality_score = 1.0. Any other value would indicate
    a partial-quality row that bypassed quarantine unexpectedly.
    """
    if score_col not in df.columns:
        return make_result(
            table,
            f"quality_score_all_1 ({score_col})",
            passed=False,
            value=f"column '{score_col}' not found",
            expected="all rows = 1.0",
        )

    non_perfect = df.filter(col(score_col) != 1.0).count()
    passed = non_perfect == 0

    return make_result(
        table,
        f"quality_score_all_1 ({score_col})",
        passed=passed,
        value=f"{non_perfect:,} rows with score != 1.0",
        expected="0 rows with score != 1.0",
    )


def check_quarantine_count(
    spark: SparkSession,
    table: str,
    process_date: str,
) -> Dict:
    """
    Check 7: Quarantine partition row count (informational / warning only).

    Quarantine rows are expected and healthy — the quarantine framework
    is working. This check is WARNING severity, not FAIL.
    It surfaces the quarantine count for operational awareness.

    The check FAILS (not just warns) only if the quarantine path
    exists but contains zero readable Parquet files, which would
    suggest a write error in the quarantine step of the Silver job.
    """
    q_path = os.path.join(QUARANTINE_PATH, table, f"date={process_date}")

    if not os.path.exists(q_path):
        return make_result(
            table,
            "quarantine_count",
            passed=True,
            value="0 (no quarantine partition — all rows valid)",
            warning=False,
        )

    parquet_files = [f for f in os.listdir(q_path) if f.endswith(".parquet")]
    if len(parquet_files) == 0:
        return make_result(
            table,
            "quarantine_count",
            passed=False,
            value=f"quarantine path exists but contains no parquet files: {q_path}",
            expected="either no path or path with valid parquet files",
        )

    try:
        q_df = spark.read.parquet(q_path)
        q_count = q_df.count()

        # Show breakdown by reason — informational
        if q_count > 0:
            breakdown = {
                row["_quarantine_reason"]: row["count"]
                for row in q_df.groupBy("_quarantine_reason").count().collect()
            }
        else:
            breakdown = {}

        return make_result(
            table,
            "quarantine_count",
            passed=True,
            value=f"{q_count:,} quarantined rows | breakdown={breakdown}",
            warning=q_count > 0,
        )

    except Exception as e:
        return make_result(
            table,
            "quarantine_count",
            passed=False,
            value=f"failed to read quarantine path: {e}",
        )


def check_retention_vs_bronze(
    df: DataFrame,
    spark: SparkSession,
    table: str,
    process_date: str,
    warn_threshold: float,
) -> Dict:
    """
    Check 8: Silver retention percentage relative to Bronze.

    Computes: silver_rows / bronze_rows * 100.
    Emits a WARNING (not FAIL) if below warn_threshold.
    Emits a FAIL if Bronze partition does not exist (upstream issue).

    Healthy retention depends on the table:
      - orders, order_items: expect > 95% (low quarantine rate)
      - customers, products, stores: expect > 95%
      A drop below 90% always warrants investigation.
    """
    bronze_path = os.path.join(BRONZE_PATH, table, f"ingest_date={process_date}")

    if not os.path.exists(bronze_path):
        return make_result(
            table,
            "retention_vs_bronze",
            passed=False,
            value=f"Bronze partition not found: {bronze_path}",
            expected=f">= {warn_threshold}%",
        )

    try:
        bronze_count = spark.read.parquet(bronze_path).count()
        silver_count = df.count()

        if bronze_count == 0:
            return make_result(
                table,
                "retention_vs_bronze",
                passed=False,
                value="Bronze partition has 0 rows",
                expected=f">= {warn_threshold}%",
            )

        retention_pct = round(silver_count / bronze_count * 100, 2)
        below_thresh = retention_pct < warn_threshold

        return make_result(
            table,
            "retention_vs_bronze",
            passed=True,  # retention is always informational
            value=f"{retention_pct}% "
            f"(silver={silver_count:,} bronze={bronze_count:,})",
            expected=f">= {warn_threshold}%",
            warning=below_thresh,
        )

    except Exception as e:
        return make_result(
            table,
            "retention_vs_bronze",
            passed=False,
            value=f"failed to compute retention: {e}",
            expected=f">= {warn_threshold}%",
        )


# ════════════════════════════════════════════════════════
# TABLE-LEVEL CHECK RUNNER
# ════════════════════════════════════════════════════════


def run_checks_for_table(
    spark: SparkSession,
    table: str,
    process_date: str,
) -> Tuple[bool, List[Dict]]:
    """
    Run all checks for a single Silver table.

    Returns (table_passed, list_of_result_dicts).

    If the Silver partition does not exist, all subsequent checks
    for this table are skipped and the table is marked FAIL.
    """
    config = TABLE_CONFIGS[table]
    results = []

    # ── Check 1: Partition exists ─────────────────────────
    exists, silver_path, r1 = check_partition_exists(table, process_date)
    results.append(r1)

    if not exists:
        # Cannot run any further checks without the partition
        print(
            f"\n[QC-SILVER] {table}: PARTITION NOT FOUND — "
            f"skipping all remaining checks for this table"
        )
        return False, results

    # Load Silver DataFrame once — reused for all remaining checks
    try:
        df = spark.read.parquet(silver_path)
    except Exception as e:
        results.append(
            make_result(
                table,
                "parquet_readable",
                passed=False,
                value=f"failed to read parquet: {e}",
                expected="readable parquet files",
            )
        )
        return False, results

    results.append(
        make_result(
            table,
            "parquet_readable",
            passed=True,
            value=f"schema cols={len(df.columns)}",
        )
    )

    # ── Check 2: Row count > 0 ────────────────────────────
    results.append(check_row_count(df, table))

    # ── Check 3: Surrogate key uniqueness ─────────────────
    results.append(check_surrogate_key_uniqueness(df, table, config["surrogate_key"]))

    # ── Check 4: Mandatory columns not null ───────────────
    results.extend(check_mandatory_nulls(df, table, config["mandatory_cols"]))

    # ── Check 5: Required columns present ─────────────────
    results.append(check_required_columns_present(df, table, config["required_cols"]))

    # ── Check 6: Quality score all 1.0 ───────────────────
    results.append(check_quality_score(df, table, config["quality_score_col"]))

    # ── Check 7: Quarantine count (informational) ─────────
    results.append(check_quarantine_count(spark, table, process_date))

    # ── Check 8: Retention vs Bronze ─────────────────────
    results.append(
        check_retention_vs_bronze(
            df,
            spark,
            table,
            process_date,
            config["retention_warn_pct"],
        )
    )

    # ── Table pass/fail: any non-warning failure fails table
    table_passed = all(r["passed"] or r.get("warning", False) for r in results) and all(
        r["passed"] for r in results if not r.get("warning", False)
    )

    return table_passed, results


# ════════════════════════════════════════════════════════
# SUMMARY PRINTER
# ════════════════════════════════════════════════════════


def print_check_results(
    all_results: Dict[str, Tuple[bool, List[Dict]]],
    process_date: str,
) -> bool:
    """
    Print formatted check results for all tables.

    Layout:
      - Per-table block with each check on one line
      - PASS / FAIL / WARN status for each check
      - Per-table PASS / FAIL summary
      - Final overall PASS / FAIL with counts

    Returns overall_passed boolean.
    """
    col_w_check = 46
    col_w_val = 50
    separator = "─" * 100

    print(f"\n{'='*100}")
    print(f"  SILVER LAYER QUALITY CHECK REPORT — {process_date}")
    print(f"{'='*100}")

    overall_passed = True
    tables_passed = 0
    tables_failed = 0
    total_checks = 0
    checks_passed = 0
    checks_failed = 0
    checks_warned = 0

    for table, (table_passed, results) in all_results.items():

        status_label = "PASS" if table_passed else "FAIL"
        print(f"\n  TABLE: {table.upper():<20}  [{status_label}]")
        print(f"  {separator}")
        print(f"  {'CHECK':<{col_w_check}} " f"{'STATUS':<8} " f"{'VALUE / DETAIL'}")
        print(f"  {separator}")

        for r in results:
            if r.get("warning") and r["passed"]:
                status = "WARN"
                checks_warned += 1
            elif r["passed"]:
                status = "PASS"
                checks_passed += 1
            else:
                status = "FAIL"
                checks_failed += 1
                overall_passed = False

            total_checks += 1

            value_str = str(r["value"])
            if len(value_str) > col_w_val:
                value_str = value_str[: col_w_val - 3] + "..."

            print(f"  {r['check']:<{col_w_check}} " f"{status:<8} " f"{value_str}")

        if table_passed:
            tables_passed += 1
        else:
            tables_failed += 1

    # ── Overall summary ───────────────────────────────────
    overall_label = "PASS" if overall_passed else "FAIL"

    print(f"\n{'='*100}")
    print(f"  OVERALL RESULT : [{overall_label}]")
    print(f"  {'─'*60}")
    print(f"  Tables  : {tables_passed} passed  |  {tables_failed} failed")
    print(
        f"  Checks  : {checks_passed} passed  |  "
        f"{checks_failed} failed  |  "
        f"{checks_warned} warnings"
    )
    print(f"  Total   : {total_checks} checks across {len(all_results)} tables")
    print(f"{'='*100}\n")

    return overall_passed


# ════════════════════════════════════════════════════════
# AUDIT LOG — write results to PostgreSQL
# ════════════════════════════════════════════════════════


def log_check_results_to_postgres(
    process_date: str,
    all_results: Dict[str, Tuple[bool, List[Dict]]],
) -> None:
    """
    Write all check results to audit.quality_results in PostgreSQL.

    Non-fatal — a logging failure must never block the pipeline.
    Each check is written as a separate row for queryability.
    """
    try:
        import psycopg2

        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        rows_inserted = 0
        for table, (_, results) in all_results.items():
            for r in results:
                cur.execute(
                    """
                    INSERT INTO audit.quality_results
                        (run_date, table_name, check_name,
                         passed, value, threshold, checked_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (
                        process_date,
                        table,
                        r["check"],
                        r["passed"] and not r.get("warning", False),
                        str(r["value"])[:500],
                        str(r.get("expected", ""))[:200],
                    ),
                )
                rows_inserted += 1

        conn.commit()
        conn.close()

        print(
            f"[QC-SILVER] Audit log: {rows_inserted} check results "
            f"written to audit.quality_results"
        )

    except Exception as e:
        print(f"[QC-SILVER] Warning: Audit log failed (non-fatal): {e}")


# ════════════════════════════════════════════════════════
# ORCHESTRATOR
# ════════════════════════════════════════════════════════


def run_all_silver_checks(
    spark: SparkSession,
    process_date: str,
    tables: Optional[List[str]] = None,
) -> bool:
    """
    Run Silver quality checks for all specified tables.

    tables=None → run all five tables defined in TABLE_CONFIGS.
    tables=[...] → run only the listed tables.

    Returns overall_passed boolean.
    Exit code is set in main() based on this return value.
    """
    tables_to_check = tables if tables else list(TABLE_CONFIGS.keys())

    print(f"\n[QC-SILVER] Starting Silver checks for date={process_date}")
    print(f"[QC-SILVER] Tables: {tables_to_check}")

    all_results: Dict[str, Tuple[bool, List[Dict]]] = {}

    for table in tables_to_check:
        if table not in TABLE_CONFIGS:
            print(f"[QC-SILVER] WARNING: '{table}' not in TABLE_CONFIGS — skipping")
            continue

        print(f"\n[QC-SILVER] Checking table: {table} ...")
        table_passed, results = run_checks_for_table(spark, table, process_date)
        all_results[table] = (table_passed, results)

        status = "PASS" if table_passed else "FAIL"
        print(
            f"[QC-SILVER] {table}: {status} "
            f"({sum(1 for r in results if r['passed'])} / "
            f"{len(results)} checks passed)"
        )

    overall_passed = print_check_results(all_results, process_date)
    log_check_results_to_postgres(process_date, all_results)

    return overall_passed


# ════════════════════════════════════════════════════════
# MAIN — CLI entry point
# ════════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Silver layer quality checks for all five Olist Silver tables.\n"
            "Validates partitions, row counts, surrogate keys, null rules,\n"
            "schema, quality scores, quarantine counts, and retention."
        )
    )
    parser.add_argument(
        "--date",
        default=datetime.today().strftime("%Y-%m-%d"),
        help="Process date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--table",
        default=None,
        choices=list(TABLE_CONFIGS.keys()),
        help="Run checks for a single table only (default: all tables)",
    )
    args = parser.parse_args()

    tables = [args.table] if args.table else None

    spark = get_spark_session(
        app_name=f"SilverQualityChecks_{args.date}",
        memory="4g",
    )

    try:
        overall_passed = run_all_silver_checks(
            spark,
            process_date=args.date,
            tables=tables,
        )

    except Exception as e:
        print(f"\n[QC-SILVER] UNEXPECTED ERROR: {e}")
        spark.stop()
        sys.exit(1)

    finally:
        spark.stop()

    if overall_passed:
        print("[QC-SILVER] All Silver checks PASSED — safe to proceed to Gold layer.")
        sys.exit(0)
    else:
        print(
            "[QC-SILVER] One or more Silver checks FAILED — "
            "Gold layer build is BLOCKED. Investigate failures above."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
