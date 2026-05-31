"""
Gold Layer — daily_orders_kpi.py
==================================
Grain     : 1 row = 1 purchase_date × 1 customer_state
Partition : purchase_date
Source    : silver/orders + silver/customers
Target    : data/gold/daily_orders_kpi/purchase_date={date}/

All Silver column references verified against:
  - clean_orders.py (Olist schema): order_purchase_timestamp,
    order_approved_at, order_delivered_customer_date,
    order_estimated_delivery_date, order_status, customer_id
  - clean_customers.py: customer_id, customer_unique_id, customer_state

Design note: grain excludes order_status from the group key.
Instead, all status counts are columns in a single row per
date/state. This produces a compact, dashboard-ready table
with one row per business day per state.

Run:
  python3 pyspark_jobs/gold/daily_orders_kpi.py --date 2024-01-15
"""

import argparse
import os
import sys
from datetime import datetime
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    datediff,
    date_format,
    isnull,
    lit,
    round as F_round,
    sum as F_sum,
    to_date,
    unix_timestamp,
    when,
)
from pyspark.sql.functions import coalesce

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import DATABASE_URL, GOLD_PATH, SILVER_PATH
from utils.spark_session import get_spark_session

TABLE_NAME = "daily_orders_kpi"
PARTITION_COL = "purchase_date"

# Order statuses used in Silver (uppercased by clean_orders.py)
STATUS_DELIVERED = "DELIVERED"
STATUS_CANCELED = "CANCELED"
STATUS_SHIPPED = "SHIPPED"
STATUS_PROCESSING = "PROCESSING"
STATUS_INVOICED = "INVOICED"
STATUS_APPROVED = "APPROVED"


# ════════════════════════════════════════════════════════
# STEP 1 — Read Silver tables
# ════════════════════════════════════════════════════════


def read_silver(
    spark: SparkSession,
    process_date: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Read orders and customers Silver partitions.

    orders    : transactional — read specific process_date partition.
    customers : dimension snapshot — read specific process_date partition.
                Full customer set is present in each day's partition since
                clean_customers.py processes the complete CSV on every run.
    """
    orders_path = os.path.join(SILVER_PATH, "orders", f"date={process_date}")
    customers_path = os.path.join(SILVER_PATH, "customers", f"date={process_date}")

    for path, name in [(orders_path, "orders"), (customers_path, "customers")]:
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"[GOLD:{TABLE_NAME}] Silver partition not found: {path}\n"
                f"  Run clean_{name}.py --date {process_date} first."
            )

    orders = spark.read.parquet(orders_path)
    customers = spark.read.parquet(customers_path)

    print(f"[GOLD:{TABLE_NAME}] silver/orders    : {orders.count():,} rows")
    print(f"[GOLD:{TABLE_NAME}] silver/customers : {customers.count():,} rows")

    return orders, customers


# ════════════════════════════════════════════════════════
# STEP 2 — Join and derive pre-aggregation columns
# ════════════════════════════════════════════════════════


def build_enriched_fact(
    orders: DataFrame,
    customers: DataFrame,
) -> DataFrame:
    """
    Join orders to customers to get customer_state and customer_unique_id.

    Join type: LEFT on customer_id.
      - Left preserves all orders even if customer is missing from
        the customers dimension (data quality edge case).
      - Rows with null customer_state after join are still counted
        in total_orders but excluded from state-level aggregations.

    Pre-aggregate derived columns added here (not in groupBy) so
    that conditional aggregations in Step 3 reference simple columns.
    """
    # Join orders → customers (get customer_state, customer_unique_id)
    df = orders.join(
        customers.select(
            "customer_id",
            "customer_unique_id",
            "customer_state",
        ),
        on="customer_id",
        how="left",
    )

    # purchase_date: extracted from order_purchase_timestamp
    # Verified: order_purchase_timestamp exists in clean_orders.py (Olist)
    df = df.withColumn(
        "purchase_date",
        to_date(col("order_purchase_timestamp")),
    )

    # approval_hours: null when order_approved_at is null (e.g. created-only)
    # Verified: order_approved_at exists in clean_orders.py (Olist)
    df = df.withColumn(
        "_approval_hours",
        when(
            col("order_approved_at").isNotNull(),
            F_round(
                (
                    unix_timestamp(col("order_approved_at"))
                    - unix_timestamp(col("order_purchase_timestamp"))
                )
                / 3600.0,
                2,
            ),
        ).otherwise(lit(None).cast("double")),
    )

    # delivery_days: null unless order_delivered_customer_date present
    # Verified: order_delivered_customer_date exists in clean_orders.py (Olist)
    df = df.withColumn(
        "_delivery_days",
        when(
            col("order_delivered_customer_date").isNotNull(),
            datediff(
                to_date(col("order_delivered_customer_date")),
                to_date(col("order_purchase_timestamp")),
            ).cast("double"),
        ).otherwise(lit(None).cast("double")),
    )

    # estimated_days: null unless order_estimated_delivery_date present
    # Verified: order_estimated_delivery_date exists in clean_orders.py (Olist)
    df = df.withColumn(
        "_estimated_days",
        when(
            col("order_estimated_delivery_date").isNotNull(),
            datediff(
                to_date(col("order_estimated_delivery_date")),
                to_date(col("order_purchase_timestamp")),
            ).cast("double"),
        ).otherwise(lit(None).cast("double")),
    )

    # is_on_time: 1 when delivered on or before estimated, else 0, else null
    df = df.withColumn(
        "_is_on_time",
        when(
            (col("order_status") == STATUS_DELIVERED)
            & col("order_delivered_customer_date").isNotNull()
            & col("order_estimated_delivery_date").isNotNull(),
            when(
                col("order_delivered_customer_date")
                <= col("order_estimated_delivery_date"),
                lit(1),
            ).otherwise(lit(0)),
        ).otherwise(lit(None).cast("int")),
    )

    # Status indicator columns for conditional SUM aggregations
    df = (
        df.withColumn(
            "_is_delivered",
            when(col("order_status") == STATUS_DELIVERED, lit(1)).otherwise(lit(0)),
        )
        .withColumn(
            "_is_canceled",
            when(col("order_status") == STATUS_CANCELED, lit(1)).otherwise(lit(0)),
        )
        .withColumn(
            "_is_shipped",
            when(col("order_status") == STATUS_SHIPPED, lit(1)).otherwise(lit(0)),
        )
        .withColumn(
            "_is_processing",
            when(col("order_status") == STATUS_PROCESSING, lit(1)).otherwise(lit(0)),
        )
        .withColumn(
            "_is_invoiced",
            when(col("order_status") == STATUS_INVOICED, lit(1)).otherwise(lit(0)),
        )
        .withColumn(
            "_is_approved",
            when(col("order_status") == STATUS_APPROVED, lit(1)).otherwise(lit(0)),
        )
    )

    print(f"[GOLD:{TABLE_NAME}] Enriched fact rows : {df.count():,}")
    return df


# ════════════════════════════════════════════════════════
# STEP 3 — Aggregate KPIs
# ════════════════════════════════════════════════════════


def aggregate_kpis(
    df: DataFrame,
    process_date: str,
) -> DataFrame:
    """
    Grain: 1 row = 1 purchase_date × 1 customer_state.

    All status counts are columns in a single row — not separate rows per status.
    This produces a compact table suitable for direct Tableau connection.

    Division-by-zero guards applied on all rate calculations.
    avg() on nullable columns automatically ignores nulls — correct
    for delivery_days (null for non-delivered), approval_hours (null
    for non-approved), estimated_days (null if absent in source).
    """
    gold_df = df.groupBy("purchase_date", "customer_state").agg(
        # ── Volume metrics ────────────────────────────────
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_unique_id").alias("total_unique_customers"),
        # ── Status breakdown (SUM of indicator cols) ─────
        F_sum("_is_delivered").alias("delivered_count"),
        F_sum("_is_canceled").alias("cancelled_count"),
        F_sum("_is_shipped").alias("shipped_count"),
        F_sum("_is_processing").alias("processing_count"),
        F_sum("_is_invoiced").alias("invoiced_count"),
        F_sum("_is_approved").alias("approved_count"),
        # ── Time metrics (AVG ignores nulls automatically) ─
        F_round(avg("_approval_hours"), 2).alias("avg_approval_time_hrs"),
        F_round(avg("_delivery_days"), 2).alias("avg_delivery_days"),
        F_round(avg("_estimated_days"), 2).alias("avg_estimated_days"),
        # ── On-time delivery ──────────────────────────────
        coalesce(F_sum("_is_on_time"), lit(0)).alias("on_time_count"),
    )

    # Derived rate metrics — guarded against division by zero
    gold_df = (
        gold_df.withColumn(
            "delivery_rate_pct",
            F_round(
                when(
                    col("total_orders") > 0,
                    col("delivered_count") / col("total_orders") * 100,
                ).otherwise(lit(None)),
                2,
            ),
        )
        .withColumn(
            "cancellation_rate_pct",
            F_round(
                when(
                    col("total_orders") > 0,
                    col("cancelled_count") / col("total_orders") * 100,
                ).otherwise(lit(None)),
                2,
            ),
        )
        .withColumn(
            "on_time_rate_pct",
            F_round(
                when(
                    col("delivered_count") > 0,
                    col("on_time_count") / col("delivered_count") * 100,
                ).otherwise(lit(None)),
                2,
            ),
        )
        .withColumn("_process_date", lit(process_date))
    )

    print(f"[GOLD:{TABLE_NAME}] Gold rows          : {gold_df.count():,}")
    return gold_df


# ════════════════════════════════════════════════════════
# STEP 4 — Write Gold partition
# ════════════════════════════════════════════════════════


def write_gold(df: DataFrame, process_date: str) -> int:
    """
    Write to Gold partitioned by purchase_date.
    partitionOverwriteMode=dynamic ensures only partitions present
    in this write are overwritten — other dates' data is untouched.
    """
    output_path = os.path.join(GOLD_PATH, TABLE_NAME)
    row_count = df.count()

    (
        df.write.partitionBy(PARTITION_COL)
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .parquet(output_path)
    )

    print(
        f"[GOLD:{TABLE_NAME}] Written             : "
        f"{row_count:,} rows → {output_path}/"
    )
    return row_count


# ════════════════════════════════════════════════════════
# AUDIT LOG
# ════════════════════════════════════════════════════════


def log_run_to_postgres(
    process_date: str,
    row_count: int,
    status: str = "success",
    error_msg: str = None,
) -> None:
    try:
        import psycopg2

        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO audit.pipeline_runs
               (dag_id, run_date, table_name, layer,
                output_rows, status, error_message, completed_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())""",
            (
                "retail_daily_pipeline",
                process_date,
                TABLE_NAME,
                "gold",
                row_count,
                status,
                error_msg,
            ),
        )
        conn.commit()
        conn.close()
        print(f"[GOLD:{TABLE_NAME}] Audit log           : written")
    except Exception as e:
        print(f"[GOLD:{TABLE_NAME}] Warning: Audit log failed (non-fatal): {e}")


# ════════════════════════════════════════════════════════
# ORCHESTRATOR
# ════════════════════════════════════════════════════════


def run_daily_orders_kpi(
    spark: SparkSession,
    process_date: str,
) -> int:
    print(f"\n{'='*60}")
    print(f"GOLD — {TABLE_NAME} — {process_date}")
    print(f"{'='*60}")

    orders, customers = read_silver(spark, process_date)
    fact = build_enriched_fact(orders, customers)
    gold_df = aggregate_kpis(fact, process_date)
    row_count = write_gold(gold_df, process_date)
    log_run_to_postgres(process_date, row_count)

    print(f"\n[GOLD:{TABLE_NAME}] Completed successfully.")
    return row_count


# ════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(
        description="Gold: daily_orders_kpi using Olist Silver schema."
    )
    parser.add_argument(
        "--date",
        default=datetime.today().strftime("%Y-%m-%d"),
        help="Process date YYYY-MM-DD",
    )
    args = parser.parse_args()

    spark = get_spark_session(app_name=f"Gold_DailyOrdersKPI_{args.date}", memory="4g")
    try:
        run_daily_orders_kpi(spark, args.date)
    except Exception as e:
        print(f"\n[GOLD:{TABLE_NAME}] FAILED: {e}")
        log_run_to_postgres(args.date, 0, "failed", str(e))
        spark.stop()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
