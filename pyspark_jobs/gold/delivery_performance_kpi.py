"""
Gold Layer — delivery_performance_kpi.py
==========================================
Grain     : 1 row = 1 purchase_month × 1 customer_state
Partition : purchase_month  (format: yyyy-MM)
Source    : silver/orders + silver/customers
Target    : data/gold/delivery_performance_kpi/purchase_month={month}/

All Silver column references verified against:
  - clean_orders.py (Olist): order_id, customer_id, order_status,
    order_purchase_timestamp, order_delivered_customer_date,
    order_estimated_delivery_date
  - clean_customers.py: customer_id, customer_state

All orders included (no status filter) — cancellation and delivery
rates are the KPIs, so all statuses must be counted.

Run:
  python3 pyspark_jobs/gold/delivery_performance_kpi.py --date 2024-01-15
"""

import argparse
import os
import sys
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    countDistinct,
    date_format,
    datediff,
    lit,
    round as F_round,
    sum as F_sum,
    to_date,
    when,
)
from pyspark.sql.functions import coalesce

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import DATABASE_URL, GOLD_PATH, SILVER_PATH
from utils.spark_session import get_spark_session

TABLE_NAME = "delivery_performance_kpi"
PARTITION_COL = "purchase_month"


def read_silver(spark: SparkSession, process_date: str):
    orders_path = os.path.join(SILVER_PATH, "orders", f"date={process_date}")
    customers_path = os.path.join(SILVER_PATH, "customers", f"date={process_date}")

    for path, name in [(orders_path, "orders"), (customers_path, "customers")]:
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"[GOLD:{TABLE_NAME}] Silver/{name} partition not found: {path}"
            )

    orders = spark.read.parquet(orders_path)
    customers = spark.read.parquet(customers_path)

    print(f"[GOLD:{TABLE_NAME}] silver/orders    : {orders.count():,} rows")
    print(f"[GOLD:{TABLE_NAME}] silver/customers : {customers.count():,} rows")

    return orders, customers


def build_enriched_fact(
    orders: DataFrame,
    customers: DataFrame,
) -> DataFrame:
    """
    Join orders → customers to get customer_state.

    All orders included regardless of status — this table measures
    delivery quality (on-time rate, delay) AND order health
    (cancellation rate, delivery rate).

    Pre-computed columns:
      purchase_month : for grouping.
      _delivery_days : datediff(delivered_customer, purchase_ts) — null for
                       non-delivered orders. avg() ignores nulls. ✓
      _estimated_days: datediff(estimated_delivery, purchase_ts). ✓
      _delay_days    : datediff(delivered, estimated) where late only.
      _is_on_time    : 1 when delivered ≤ estimated, else 0. null if not delivered.
      _is_delivered  : 1/0 indicator.
      _is_canceled   : 1/0 indicator.

    Column verification:
      order_purchase_timestamp      → clean_orders.py (Olist) ✓
      order_delivered_customer_date → clean_orders.py (Olist) ✓
      order_estimated_delivery_date → clean_orders.py (Olist) ✓
      customer_state                → clean_customers.py ✓
    """
    df = orders.join(
        customers.select("customer_id", "customer_state"),
        on="customer_id",
        how="left",
    )

    df = df.withColumn(
        "purchase_month",
        date_format(to_date(col("order_purchase_timestamp")), "yyyy-MM"),
    )

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

    # delay_days: positive = late, only for delivered orders that were late
    df = df.withColumn(
        "_delay_days",
        when(
            (col("order_status") == "DELIVERED")
            & col("order_delivered_customer_date").isNotNull()
            & col("order_estimated_delivery_date").isNotNull()
            & (
                col("order_delivered_customer_date")
                > col("order_estimated_delivery_date")
            ),
            datediff(
                to_date(col("order_delivered_customer_date")),
                to_date(col("order_estimated_delivery_date")),
            ).cast("double"),
        ).otherwise(lit(None).cast("double")),
    )

    df = df.withColumn(
        "_is_on_time",
        when(
            (col("order_status") == "DELIVERED")
            & col("order_delivered_customer_date").isNotNull()
            & col("order_estimated_delivery_date").isNotNull(),
            when(
                col("order_delivered_customer_date")
                <= col("order_estimated_delivery_date"),
                lit(1),
            ).otherwise(lit(0)),
        ).otherwise(lit(None).cast("int")),
    )

    df = df.withColumn(
        "_is_delivered",
        when(col("order_status") == "DELIVERED", lit(1)).otherwise(lit(0)),
    ).withColumn(
        "_is_canceled",
        when(col("order_status") == "CANCELED", lit(1)).otherwise(lit(0)),
    )

    print(f"[GOLD:{TABLE_NAME}] Enriched fact rows : {df.count():,}")
    return df


def aggregate_kpis(
    df: DataFrame,
    process_date: str,
) -> DataFrame:
    """
    Grain: 1 row = 1 purchase_month × 1 customer_state.

    avg() on nullable _delivery_days, _estimated_days, _delay_days
    automatically ignores nulls — correct behaviour here:
      - _delivery_days null for non-delivered orders (don't distort avg)
      - _delay_days null for on-time orders (don't distort avg)
    """
    gold_df = df.groupBy("purchase_month", "customer_state").agg(
        countDistinct("order_id").alias("total_orders"),
        F_sum("_is_delivered").alias("delivered_orders"),
        F_sum("_is_canceled").alias("cancelled_orders"),
        coalesce(F_sum("_is_on_time"), lit(0)).alias("on_time_orders"),
        F_round(avg("_delivery_days"), 2).alias("avg_delivery_days"),
        F_round(avg("_estimated_days"), 2).alias("avg_estimated_days"),
        F_round(avg("_delay_days"), 2).alias("avg_delay_days"),
    )

    # Derived rate metrics — all guarded against division by zero
    gold_df = (
        gold_df.withColumn(
            "delivery_rate_pct",
            F_round(
                when(
                    col("total_orders") > 0,
                    col("delivered_orders") / col("total_orders") * 100,
                ).otherwise(lit(None)),
                2,
            ),
        )
        .withColumn(
            "cancellation_rate_pct",
            F_round(
                when(
                    col("total_orders") > 0,
                    col("cancelled_orders") / col("total_orders") * 100,
                ).otherwise(lit(None)),
                2,
            ),
        )
        .withColumn(
            "on_time_rate_pct",
            F_round(
                when(
                    col("delivered_orders") > 0,
                    col("on_time_orders") / col("delivered_orders") * 100,
                ).otherwise(lit(None)),
                2,
            ),
        )
        .withColumn("_process_date", lit(process_date))
    )

    print(f"[GOLD:{TABLE_NAME}] Gold rows           : {gold_df.count():,}")
    return gold_df


def write_gold(df: DataFrame) -> int:
    output_path = os.path.join(GOLD_PATH, TABLE_NAME)
    row_count = df.count()

    (
        df.write.partitionBy(PARTITION_COL)
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .parquet(output_path)
    )

    print(
        f"[GOLD:{TABLE_NAME}] Written             : {row_count:,} rows → {output_path}/"
    )
    return row_count


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


def run_delivery_performance_kpi(
    spark: SparkSession,
    process_date: str,
) -> int:
    print(f"\n{'='*60}")
    print(f"GOLD — {TABLE_NAME} — {process_date}")
    print(f"{'='*60}")

    orders, customers = read_silver(spark, process_date)
    fact = build_enriched_fact(orders, customers)
    gold_df = aggregate_kpis(fact, process_date)
    count = write_gold(gold_df)
    log_run_to_postgres(process_date, count)

    print(f"\n[GOLD:{TABLE_NAME}] Completed successfully.")
    return count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=datetime.today().strftime("%Y-%m-%d"))
    args = parser.parse_args()

    spark = get_spark_session(
        app_name=f"Gold_DeliveryPerformanceKPI_{args.date}", memory="4g"
    )
    try:
        run_delivery_performance_kpi(spark, args.date)
    except Exception as e:
        print(f"\n[GOLD:{TABLE_NAME}] FAILED: {e}")
        log_run_to_postgres(args.date, 0, "failed", str(e))
        spark.stop()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
