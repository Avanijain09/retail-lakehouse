"""
Gold Layer — category_revenue_kpi.py
=======================================
Grain     : 1 row = 1 purchase_month × 1 product_category_name
Partition : purchase_month  (format: yyyy-MM)
Source    : silver/order_items + silver/orders + silver/products
Target    : data/gold/category_revenue_kpi/purchase_month={month}/

All Silver column references verified against:
  - clean_order_items.py: order_id, product_id, price,
    freight_value, total_item_value
  - clean_orders.py (Olist): order_id, order_status,
    order_purchase_timestamp
  - clean_products.py: product_id, product_category_name
    (lowercase, "unknown" sentinel for nulls)

Filter: CANCELED and UNAVAILABLE orders excluded from all
revenue metrics — only orders that generated real revenue.

Run:
  python3 pyspark_jobs/gold/category_revenue_kpi.py --date 2024-01-15
"""

import argparse
import os
import sys
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    date_format,
    lit,
    round as F_round,
    sum as F_sum,
    to_date,
    when,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import DATABASE_URL, GOLD_PATH, SILVER_PATH
from utils.spark_session import get_spark_session

TABLE_NAME = "category_revenue_kpi"
PARTITION_COL = "purchase_month"

# Statuses that do not represent real revenue — excluded from aggregations
EXCLUDED_STATUSES = {"CANCELED", "UNAVAILABLE"}


# ════════════════════════════════════════════════════════
# STEP 1 — Read Silver tables
# ════════════════════════════════════════════════════════


def read_silver(
    spark: SparkSession,
    process_date: str,
):
    paths = {
        "order_items": os.path.join(SILVER_PATH, "order_items", f"date={process_date}"),
        "orders": os.path.join(SILVER_PATH, "orders", f"date={process_date}"),
        "products": os.path.join(SILVER_PATH, "products", f"date={process_date}"),
    }

    for name, path in paths.items():
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"[GOLD:{TABLE_NAME}] Silver/{name} partition not found: {path}"
            )

    items = spark.read.parquet(paths["order_items"])
    orders = spark.read.parquet(paths["orders"])
    products = spark.read.parquet(paths["products"])

    print(f"[GOLD:{TABLE_NAME}] silver/order_items : {items.count():,} rows")
    print(f"[GOLD:{TABLE_NAME}] silver/orders      : {orders.count():,} rows")
    print(f"[GOLD:{TABLE_NAME}] silver/products    : {products.count():,} rows")

    return items, orders, products


# ════════════════════════════════════════════════════════
# STEP 2 — Build and filter fact
# ════════════════════════════════════════════════════════


def build_filtered_fact(
    items: DataFrame,
    orders: DataFrame,
    products: DataFrame,
) -> DataFrame:
    """
    Join order_items → orders → products.

    items JOIN orders (inner):
      Gets order_purchase_timestamp and order_status per item.
      Inner join: items without a parent order are orphaned and
      should have been caught in Silver referential integrity checks.

    fact JOIN products (left):
      Gets product_category_name per item.
      Left join: discontinued products may not be in the products
      dimension; their items still count in revenue under "unknown".

    Filter: Remove CANCELED and UNAVAILABLE orders.
      Verified: order_status column exists in clean_orders.py (Olist).
      Verified: EXCLUDED_STATUSES match uppercased Olist values.
    """
    fact = items.join(
        orders.select(
            "order_id",
            "order_status",
            col("order_purchase_timestamp"),
        ),
        on="order_id",
        how="inner",
    )

    # Filter revenue-generating orders only
    fact = fact.filter(~col("order_status").isin(list(EXCLUDED_STATUSES)))

    fact = fact.join(
        products.select("product_id", "product_category_name"),
        on="product_id",
        how="left",
    )

    # purchase_month for grouping
    # Verified: order_purchase_timestamp exists in clean_orders.py (Olist)
    fact = fact.withColumn(
        "purchase_month",
        date_format(to_date(col("order_purchase_timestamp")), "yyyy-MM"),
    )

    print(f"[GOLD:{TABLE_NAME}] Fact rows (filtered): {fact.count():,}")
    return fact


# ════════════════════════════════════════════════════════
# STEP 3 — Aggregate KPIs
# ════════════════════════════════════════════════════════


def aggregate_kpis(
    fact: DataFrame,
    process_date: str,
) -> DataFrame:
    """
    Grain: 1 row = 1 purchase_month × 1 product_category_name.

    Column verification:
      price           → verified in clean_order_items.py
      freight_value   → verified (null→0.0 default in Silver)
      total_item_value→ verified (derived col: price + freight_value)
      product_category_name → verified (lowercase, "unknown" for nulls)

    COUNT(*) = total_items_sold because grain of order_items is
    1 row = 1 unit sold (no quantity column in Olist).
    """
    gold_df = fact.groupBy(
        "purchase_month",
        "product_category_name",
    ).agg(
        F_round(F_sum("price"), 2).alias("total_revenue"),
        F_round(F_sum("freight_value"), 2).alias("total_freight"),
        F_round(F_sum("total_item_value"), 2).alias("total_gmv"),
        count(lit(1)).alias("total_items_sold"),
        countDistinct("order_id").alias("total_orders"),
    )

    # Derived metrics — division-by-zero guarded
    gold_df = (
        gold_df.withColumn(
            "avg_item_price",
            F_round(
                when(
                    col("total_items_sold") > 0,
                    col("total_revenue") / col("total_items_sold"),
                ).otherwise(lit(None)),
                2,
            ),
        )
        .withColumn(
            "freight_pct_of_gmv",
            F_round(
                when(
                    col("total_gmv") > 0, col("total_freight") / col("total_gmv") * 100
                ).otherwise(lit(None)),
                2,
            ),
        )
        .withColumn("_process_date", lit(process_date))
    )

    print(f"[GOLD:{TABLE_NAME}] Gold rows           : {gold_df.count():,}")
    return gold_df


# ════════════════════════════════════════════════════════
# STEP 4 — Write Gold
# ════════════════════════════════════════════════════════


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


def run_category_revenue_kpi(
    spark: SparkSession,
    process_date: str,
) -> int:
    print(f"\n{'='*60}")
    print(f"GOLD — {TABLE_NAME} — {process_date}")
    print(f"{'='*60}")

    items, orders, products = read_silver(spark, process_date)
    fact = build_filtered_fact(items, orders, products)
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
        app_name=f"Gold_CategoryRevenueKPI_{args.date}", memory="4g"
    )
    try:
        run_category_revenue_kpi(spark, args.date)
    except Exception as e:
        print(f"\n[GOLD:{TABLE_NAME}] FAILED: {e}")
        log_run_to_postgres(args.date, 0, "failed", str(e))
        spark.stop()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
