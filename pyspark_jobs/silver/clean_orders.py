"""
Silver Layer — Orders Cleaning
==============================
Responsibility:
- schema enforcement
- null handling
- deduplication
- quarantine invalid rows
- date standardization

Bronze → Silver trusted layer
"""

import argparse
import os
import sys

from pyspark.sql import DataFrame

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    to_date,
    row_number,
    trim,
    upper,
    when,
    sha2,
    concat_ws,
)

from pyspark.sql.window import Window

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)

# ─────────────────────────────────────────────────────
# Project imports
# ─────────────────────────────────────────────────────

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

sys.path.insert(0, os.path.join(PROJECT_ROOT, "pyspark_jobs"))

from utils.spark_session import get_spark_session
from utils.config import (
    BRONZE_PATH,
    SILVER_PATH,
    QUARANTINE_PATH,
)

# ─────────────────────────────────────────────────────
# Silver schema enforcement
# ─────────────────────────────────────────────────────

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=True),
        StructField("order_status", StringType(), nullable=True),
        # Bronze parquet already stores timestamps properly
        StructField("order_purchase_timestamp", TimestampType(), nullable=True),
        StructField("order_approved_at", TimestampType(), nullable=True),
        StructField("order_delivered_carrier_date", TimestampType(), nullable=True),
        StructField("order_delivered_customer_date", TimestampType(), nullable=True),
        StructField("order_estimated_delivery_date", TimestampType(), nullable=True),
        # ── Bronze audit columns ─────────────────
        StructField("_ingest_timestamp", TimestampType(), nullable=True),
        StructField("_source_file", StringType(), nullable=True),
        StructField("_ingest_date", StringType(), nullable=True),
        StructField("_pipeline_version", StringType(), nullable=True),
    ]
)

# ─────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────


def remove_duplicates(df: DataFrame) -> DataFrame:
    """
    Keep latest version of each order_id
    using Bronze ingest timestamp.
    """

    window = Window.partitionBy("order_id").orderBy(col("_ingest_timestamp").desc())

    deduped = (
        df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")
    )

    duplicates_removed = df.count() - deduped.count()

    print(f"[SILVER] Duplicates removed: " f"{duplicates_removed:,}")

    return deduped


def separate_nulls(df: DataFrame, process_date: str):
    """
    Separate rows having NULLs
    in mandatory business columns.
    """

    MANDATORY_COLS = [
        "order_id",
        "order_date",
    ]

    # Build dynamic null condition
    null_condition = col(MANDATORY_COLS[0]).isNull()

    for c in MANDATORY_COLS[1:]:

        null_condition = null_condition | col(c).isNull()

    # ── Valid rows ─────────────────────────

    valid_df = df.filter(~null_condition)

    # ── Quarantine rows ────────────────────

    quarantine_df = (
        df.filter(null_condition)
        .withColumn("_quarantine_reason", lit("NULL in mandatory field"))
        .withColumn("_quarantine_date", lit(process_date))
    )

    print(f"[SILVER] Null quarantine rows: " f"{quarantine_df.count():,}")

    return valid_df, quarantine_df


def clean_strings(df: DataFrame) -> DataFrame:
    """
    Standardize strings:
    - trim whitespace
    - normalize casing
    - standardize statuses
    """

    cleaned = (
        df
        # ── Trim IDs ────────────────────────
        .withColumn("customer_id", trim(col("customer_id"))).withColumn(
            "order_id", trim(col("order_id"))
        )
        # ── Normalize order status ─────────
        .withColumn("order_status", trim(upper(col("order_status"))))
        # ── Status standardization ─────────
        .withColumn(
            "order_status",
            when(
                col("order_status").isin("DELIVERED", "COMPLETE", "COMPLETED"),
                lit("DELIVERED"),
            )
            .when(
                col("order_status").isin("CANCEL", "CANCELLED", "CANCELED"),
                lit("CANCELLED"),
            )
            .when(
                col("order_status").isin("RETURN", "RETURNED", "REFUNDED"),
                lit("RETURNED"),
            )
            .otherwise(col("order_status")),
        )
    )

    print("[SILVER] String cleaning complete")

    return cleaned


def generate_surrogate_keys(df: DataFrame, process_date: str) -> DataFrame:
    """
    Generate deterministic surrogate keys.
    """

    with_keys = df.withColumn(
        "order_sk", sha2(concat_ws("|", col("order_id"), lit(process_date)), 256)
    )

    # ── Verify uniqueness ─────────────────

    sk_count = with_keys.select("order_sk").distinct().count()

    total_count = with_keys.count()

    assert sk_count == total_count, (
        f"Surrogate key collision! " f"{total_count - sk_count} duplicates"
    )

    print(f"[SILVER] Surrogate keys generated: " f"{sk_count:,} unique")

    return with_keys


def validate_dates(df: DataFrame, process_date: str):
    """
    Validate and standardize dates.
    Catch:
    - invalid formats
    - impossible delivery logic
    """

    # ── Create standardized order_date ─────

    converted = df.withColumn(
        "order_date", to_date(col("order_purchase_timestamp"))
    ).withColumn("delivery_date", to_date(col("order_delivered_customer_date")))

    # ── Invalid order dates ─────────────────

    bad_dates = converted.filter(col("order_date").isNull())

    # ── Impossible delivery logic ───────────

    impossible_delivery = converted.filter(
        col("delivery_date").isNotNull() & (col("delivery_date") < col("order_date"))
    )

    # ── Valid rows ──────────────────────────

    valid = converted.filter(col("order_date").isNotNull()).filter(
        ~(col("delivery_date").isNotNull() & (col("delivery_date") < col("order_date")))
    )

    # ── Quarantine invalid dates ───────────

    quarantine_bad_dates = bad_dates.withColumn(
        "_quarantine_reason", lit("INVALID_DATE_FORMAT")
    ).withColumn("_quarantine_date", lit(process_date))

    # ── Quarantine impossible delivery ─────

    quarantine_impossible = impossible_delivery.withColumn(
        "_quarantine_reason", lit("DELIVERY_BEFORE_ORDER")
    ).withColumn("_quarantine_date", lit(process_date))

    # ── Combine quarantine rows ────────────

    quarantine = quarantine_bad_dates.unionByName(
        quarantine_impossible, allowMissingColumns=True
    )

    print(f"[SILVER] Invalid dates: " f"{bad_dates.count():,}")

    print(f"[SILVER] Impossible deliveries: " f"{impossible_delivery.count():,}")

    return valid, quarantine


# ─────────────────────────────────────────────────────
# Main cleaning logic
# ─────────────────────────────────────────────────────


def clean_orders_df(df: DataFrame, process_date: str):

    raw_count = df.count()

    # ── 1. Deduplication ─────────────────

    deduped = remove_duplicates(df)

    # ── 2. String cleaning ───────────────────

    cleaned_strings = clean_strings(deduped)

    # ── 3. Date validation ───────────────────

    valid_1, quarantine_1 = validate_dates(cleaned_strings, process_date)

    # ── 4. Null checks ─────────────

    valid_2, quarantine_2 = separate_nulls(valid_1, process_date)

    # ── 5. Generate surrogate keys ─────────

    silver_ready = generate_surrogate_keys(valid_2, process_date)

    # ── Combine quarantine rows ─────────────

    quarantine = quarantine_1.unionByName(quarantine_2, allowMissingColumns=True)

    # ── Add Silver metadata ─────────────────

    final_df = (
        silver_ready
        # ── Silver processing timestamp ───────
        .withColumn("_silver_timestamp", current_timestamp())
        # ── Processing partition date ─────────
        .withColumn("_silver_date", lit(process_date))
        # ── Quality score ─────────────────────
        .withColumn("_quality_score", lit(1.0))
    )

    print(
        f"[SILVER] Orders: "
        f"raw={raw_count:,} "
        f"clean={final_df.count():,} "
        f"quarantine={quarantine.count():,}"
    )

    return final_df, quarantine


# ─────────────────────────────────────────────────────
# Pipeline runner
# ─────────────────────────────────────────────────────


def run_orders_cleaning(process_date: str):

    spark = get_spark_session("SilverOrdersCleaning")

    bronze_path = os.path.join(BRONZE_PATH, "orders", f"ingest_date={process_date}")

    silver_path = os.path.join(SILVER_PATH, "orders", f"date={process_date}")

    quarantine_path = os.path.join(QUARANTINE_PATH, "orders", f"date={process_date}")

    print(f"[SILVER] Reading Bronze: {bronze_path}")

    # ── Read Bronze with enforced schema ─────

    bronze_df = spark.read.schema(ORDERS_SCHEMA).parquet(bronze_path)

    # ── Run cleaning ─────────────────────────

    clean_df, quarantine_df = clean_orders_df(bronze_df, process_date)

    # ── Write Silver clean data ─────────────

    (clean_df.write.mode("overwrite").parquet(silver_path))

    # ── Write quarantine data ───────────────

    if quarantine_df.count() > 0:

        (quarantine_df.write.mode("append").parquet(quarantine_path))

    print(f"[SILVER] Silver write complete")
    print(f"[SILVER] Clean path: {silver_path}")

    spark.stop()


# ─────────────────────────────────────────────────────
# CLI entrypoint
# ─────────────────────────────────────────────────────

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--date", required=True, help="Processing date YYYY-MM-DD")

    args = parser.parse_args()

    run_orders_cleaning(args.date)
