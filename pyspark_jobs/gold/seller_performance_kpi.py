"""
Gold Layer — seller_performance_kpi.py
========================================
Grain     : 1 row = 1 purchase_month × 1 seller_id
Partition : purchase_month  (format: yyyy-MM)
Source    : silver/order_items + silver/orders + silver/stores
Target    : data/gold/seller_performance_kpi/purchase_month={month}/

All Silver column references verified against:
  - clean_order_items.py: order_id, seller_id, price,
    freight_value, total_item_value
  - clean_orders.py (Olist): order_id, order_status,
    order_purchase_timestamp
  - clean_stores.py: seller_id, seller_city, seller_state

Filter: CANCELED and UNAVAILABLE orders excluded.

Run:
  python3 pyspark_jobs/gold/seller_performance_kpi.py --date 2024-01-15
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

TABLE_NAME = "seller_performance_kpi"
PARTITION_COL = "purchase_month"
EXCLUDED_STATUSES = {"CANCELED", "UNAVAILABLE"}


def read_silver(spark: SparkSession, process_date: str):
    paths = {
        "order_items": os.path.join(SILVER_PATH, "order_items", f"date={process_date}"),
        "orders": os.path.join(SILVER_PATH, "orders", f"date={process_date}"),
        "stores": os.path.join(SILVER_PATH, "stores", f"date={process_date}"),
    }

    for name, path in paths.items():
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"[GOLD:{TABLE_NAME}] Silver/{name} partition not found: {path}"
            )

    items = spark.read.parquet(paths["order_items"])
    orders = spark.read.parquet(paths["orders"])
    stores = spark.read.parquet(paths["stores"])

    print(f"[GOLD:{TABLE_NAME}] silver/order_items : {items.count():,} rows")
    print(f"[GOLD:{TABLE_NAME}] silver/orders      : {orders.count():,} rows")
    print(f"[GOLD:{TABLE_NAME}] silver/stores      : {stores.count():,} rows")

    return items, orders, stores


def build_filtered_fact(
    items: DataFrame,
    orders: DataFrame,
    stores: DataFrame,
) -> DataFrame:
    """
    Join order_items → orders (for date and status) → stores (for geography).

    items JOIN orders (inner): get purchase_month and filter by status.
    fact JOIN stores (left): get seller_city, seller_state.
      Left join: a seller_id in order_items may not be in stores
      dimension if the seller was removed from Olist. Their revenue
      still counts but geography columns will be null.

    Column verification:
      seller_id    → in clean_order_items.py and clean_stores.py ✓
      seller_city  → in clean_stores.py (INITCAP, "UNKNOWN" for nulls) ✓
      seller_state → in clean_stores.py (UPPER+TRIM, validated) ✓
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

    fact = fact.filter(~col("order_status").isin(list(EXCLUDED_STATUSES)))

    fact = fact.join(
        stores.select("seller_id", "seller_city", "seller_state"),
        on="seller_id",
        how="left",
    )

    # purchase_month for grouping
    # Verified: order_purchase_timestamp in clean_orders.py (Olist)
    fact = fact.withColumn(
        "purchase_month",
        date_format(to_date(col("order_purchase_timestamp")), "yyyy-MM"),
    )

    print(f"[GOLD:{TABLE_NAME}] Fact rows (filtered): {fact.count():,}")
    return fact


def aggregate_kpis(
    fact: DataFrame,
    process_date: str,
) -> DataFrame:
    """
    Grain: 1 row = 1 purchase_month × 1 seller_id.

    seller_city and seller_state are included via first() equivalent
    approach: since a seller has one city/state in the stores dimension,
    grouping by seller_id and taking MAX gives the single value.
    """
    from pyspark.sql.functions import max as F_max

    gold_df = fact.groupBy(
        "purchase_month",
        "seller_id",
    ).agg(
        # Seller geography — single value per seller_id
        # MAX of a single consistent string = the string itself
        F_max("seller_city").alias("seller_city"),
        F_max("seller_state").alias("seller_state"),
        # Revenue metrics
        # Verified: price, freight_value, total_item_value in clean_order_items.py
        F_round(F_sum("price"), 2).alias("total_revenue"),
        F_round(F_sum("freight_value"), 2).alias("total_freight"),
        F_round(F_sum("total_item_value"), 2).alias("total_gmv"),
        # Volume metrics
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
            "avg_freight_per_item",
            F_round(
                when(
                    col("total_items_sold") > 0,
                    col("total_freight") / col("total_items_sold"),
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


def run_seller_performance_kpi(
    spark: SparkSession,
    process_date: str,
) -> int:
    print(f"\n{'='*60}")
    print(f"GOLD — {TABLE_NAME} — {process_date}")
    print(f"{'='*60}")

    items, orders, stores = read_silver(spark, process_date)
    fact = build_filtered_fact(items, orders, stores)
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
        app_name=f"Gold_SellerPerformanceKPI_{args.date}", memory="4g"
    )
    try:
        run_seller_performance_kpi(spark, args.date)
    except Exception as e:
        print(f"\n[GOLD:{TABLE_NAME}] FAILED: {e}")
        log_run_to_postgres(args.date, 0, "failed", str(e))
        spark.stop()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
