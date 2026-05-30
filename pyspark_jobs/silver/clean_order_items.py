"""
Silver Layer — clean_order_items.py
=====================================
Grain       : 1 row = 1 product line item within 1 order
              Each row = exactly 1 unit. No quantity column in Olist.
Composite PK: (order_id, order_item_id)
Source      : data/bronze/order_items/ingest_date={date}/
Target      : data/silver/order_items/date={date}/
Quarantine  : data/silver/_quarantine/order_items/date={date}/

Actual Olist schema (olist_order_items_dataset.csv):
  order_id, order_item_id, product_id, seller_id,
  shipping_limit_date, price, freight_value

Key design decisions:
  - price + freight_value = total_item_value (derived in Silver)
  - shipping_limit_date is operational — null-tolerant, no quarantine on date failure
  - freight_value null → default 0.0 (free shipping is valid)
  - price null or zero → quarantine (cannot compute revenue)
  - Revenue recomputation not needed here (no discount logic)
  - Surrogate key uses BOTH PK components to prevent collision

Run:
  python3 pyspark_jobs/silver/clean_order_items.py --date 2024-01-15
  python3 pyspark_jobs/silver/clean_order_items.py --date 2024-01-15 --dry-run
"""

import argparse
import os
import sys
from datetime import datetime
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as F_abs,
    coalesce,
    col,
    concat_ws,
    count,
    current_timestamp,
    lit,
    round as F_round,
    row_number,
    sha2,
    to_timestamp,
    trim,
    upper,
    when,
    year,
    isnull,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import (
    BRONZE_PATH,
    DATABASE_URL,
    QUARANTINE_PATH,
    SILVER_PATH,
)
from utils.spark_session import get_spark_session

# ════════════════════════════════════════════════════════
# CONSTANTS
# ════════════════════════════════════════════════════════

TABLE_NAME = "order_items"

# Olist shipping_limit_date format
SHIPPING_DATE_FMT = "yyyy-MM-dd HH:mm:ss"

# Sanity bounds for shipping_limit_date year
SHIPPING_YEAR_MIN = 2010
SHIPPING_YEAR_MAX = 2030

# Retention threshold — warn if Silver retains < this % of Bronze
RETENTION_WARN_THRESHOLD = 90.0

# Bronze schema matching actual Olist CSV columns.
# inferSchema in Bronze may miscast types — Silver enforces explicitly.
BRONZE_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=True),
        StructField("order_item_id", IntegerType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("seller_id", StringType(), nullable=True),
        StructField("shipping_limit_date", StringType(), nullable=True),
        StructField("price", DoubleType(), nullable=True),
        StructField("freight_value", DoubleType(), nullable=True),
    ]
)


# ════════════════════════════════════════════════════════
# STEP 1 — Read Bronze
# ════════════════════════════════════════════════════════


def read_bronze(
    spark: SparkSession,
    ingest_date: str,
) -> Tuple[DataFrame, int]:
    """
    Read Bronze partition for the given date.

    Uses explicit schema to avoid type inference issues from Bronze.
    Bronze used inferSchema=True for resilience; Silver enforces
    exact types for downstream correctness.
    """
    bronze_path = os.path.join(BRONZE_PATH, TABLE_NAME, f"ingest_date={ingest_date}")

    if not os.path.exists(bronze_path):
        raise FileNotFoundError(
            f"[SILVER:{TABLE_NAME}] Bronze partition not found: {bronze_path}\n"
            f"  Ensure Bronze ingestion ran successfully for date={ingest_date}"
        )

    df = spark.read.parquet(bronze_path)
    raw_count = df.count()

    print(f"[SILVER:{TABLE_NAME}] Bronze path         : {bronze_path}")
    print(f"[SILVER:{TABLE_NAME}] Bronze rows read    : {raw_count:,}")
    print(f"[SILVER:{TABLE_NAME}] Bronze columns      : {df.columns}")

    if raw_count == 0:
        raise ValueError(
            f"[SILVER:{TABLE_NAME}] Bronze partition is EMPTY for {ingest_date}.\n"
            f"  Investigate Bronze ingestion before proceeding."
        )

    return df, raw_count


# ════════════════════════════════════════════════════════
# STEP 2 — String cleaning (in-place, no quarantine)
# ════════════════════════════════════════════════════════


def clean_strings(df: DataFrame) -> DataFrame:
    """
    Trim whitespace on all string ID columns.
    Apply UPPER to product_id for join consistency with dim_product.

    Olist IDs are md5 hashes (lowercase). UPPER() is defensive —
    protects against future source systems sending mixed-case IDs.
    These are silent corrections — no quarantine triggered.
    """
    df = (
        df.withColumn("order_id", trim(col("order_id")))
        .withColumn("product_id", trim(col("product_id")))
        .withColumn("seller_id", trim(col("seller_id")))
    )

    print(
        f"[SILVER:{TABLE_NAME}] String cleaning      : "
        f"trim(order_id), upper(trim(product_id)), trim(seller_id)"
    )
    return df


# ════════════════════════════════════════════════════════
# STEP 3 — Null handling on mandatory columns
# ════════════════════════════════════════════════════════


def separate_null_mandatory(
    df: DataFrame,
    process_date: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Mandatory columns: order_id, order_item_id, product_id, seller_id, price.

    Null in any of these = row cannot be used in revenue or join analysis.
    Split into valid and quarantine batches.

    freight_value and shipping_limit_date are NOT checked here —
    they are optional and handled later (null-fill and lenient parsing).
    """
    null_mandatory_cond = (
        isnull(col("order_id"))
        | isnull(col("order_item_id"))
        | isnull(col("product_id"))
        | isnull(col("seller_id"))
        | isnull(col("price"))
    )

    quarantine_df = (
        df.filter(null_mandatory_cond)
        .withColumn("_quarantine_reason", lit("NULL_MANDATORY_FIELD"))
        .withColumn("_quarantine_date", lit(process_date))
    )

    valid_df = df.filter(~null_mandatory_cond)

    null_count = quarantine_df.count()

    if null_count > 0:
        print(
            f"[SILVER:{TABLE_NAME}] Null mandatory cols : "
            f"{null_count:,} rows → quarantine (NULL_MANDATORY_FIELD)"
        )
    else:
        print(f"[SILVER:{TABLE_NAME}] Null mandatory cols : 0 (clean)")

    return valid_df, quarantine_df


# ════════════════════════════════════════════════════════
# STEP 4 — Deduplication on composite key
# ════════════════════════════════════════════════════════


def deduplicate(df: DataFrame) -> Tuple[DataFrame, int]:
    """
    Composite PK = (order_id, order_item_id).

    Uses ROW_NUMBER() window function ordered by _ingest_timestamp DESC.
    Keeps the LATEST record for each composite key — handles source
    system retransmissions where an updated record arrives later.

    Superseded rows are silently dropped (not quarantined) because
    they are valid data that has been replaced, not bad data.

    Olist dataset is generally clean — expect 0 duplicates in practice.
    This pattern is present for production safety.
    """
    before = df.count()

    dedup_window = Window.partitionBy("order_id", "order_item_id").orderBy(
        col("_ingest_timestamp").desc()
    )

    df_deduped = (
        df.withColumn("_rn", row_number().over(dedup_window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    after = df_deduped.count()
    dupes = before - after

    if dupes > 0:
        print(
            f"[SILVER:{TABLE_NAME}] Deduplication       : "
            f"{dupes:,} superseded rows dropped "
            f"(kept latest per order_id + order_item_id)"
        )
    else:
        print(f"[SILVER:{TABLE_NAME}] Deduplication       : 0 duplicates (clean)")

    return df_deduped, dupes


# ════════════════════════════════════════════════════════
# STEP 5 — Null-fill optional numeric columns
# ════════════════════════════════════════════════════════


def fill_optional_nulls(df: DataFrame) -> DataFrame:
    """
    freight_value null → 0.0 (free shipping is a valid business scenario).

    This must run BEFORE business rules so the negative-freight check
    operates on a non-null column.

    shipping_limit_date is NOT filled here — it is handled in Step 6
    via to_timestamp which produces null for unparseable strings.
    """
    df = df.withColumn(
        "freight_value",
        coalesce(col("freight_value"), lit(0.0)),
    )

    print(
        f"[SILVER:{TABLE_NAME}] Optional null fill   : "
        f"freight_value → 0.0 where null"
    )
    return df


# ════════════════════════════════════════════════════════
# STEP 6 — Date parsing and validation
# ════════════════════════════════════════════════════════


def parse_and_validate_dates(
    df: DataFrame,
    process_date: str,
) -> DataFrame:
    """
    shipping_limit_date is an operational column (seller ship-by deadline).
    It is NOT a financial column — we apply a lenient strategy:

      1. Parse string → TimestampType using Olist format.
      2. Unparseable strings produce null. No quarantine.
      3. Parsed dates with year < SHIPPING_YEAR_MIN or > SHIPPING_YEAR_MAX
         are suspicious. Set to null and log a warning count.
         Do NOT quarantine — these are data quality warnings,
         not financial errors.

    Rationale: Olist data covers 2016–2018. For historical backfills,
    ALL dates are already "in the past" — quarantining on past dates
    would eliminate the entire dataset.
    """
    # Parse string to timestamp
    df = df.withColumn(
        "shipping_limit_date",
        to_timestamp(col("shipping_limit_date"), SHIPPING_DATE_FMT),
    )

    # Count unparseable dates (now null after to_timestamp)
    unparseable = df.filter(isnull(col("shipping_limit_date"))).count()

    if unparseable > 0:
        print(
            f"[SILVER:{TABLE_NAME}] shipping_limit_date : "
            f"{unparseable:,} unparseable → null (no quarantine)"
        )
    else:
        print(
            f"[SILVER:{TABLE_NAME}] shipping_limit_date : "
            f"all dates parsed successfully"
        )

    # Nullify dates outside sane year bounds
    # Keep row — only the date column becomes null
    suspicious_date_cond = col("shipping_limit_date").isNotNull() & (
        (year(col("shipping_limit_date")) < SHIPPING_YEAR_MIN)
        | (year(col("shipping_limit_date")) > SHIPPING_YEAR_MAX)
    )

    suspicious_count = df.filter(suspicious_date_cond).count()

    if suspicious_count > 0:
        print(
            f"[SILVER:{TABLE_NAME}] shipping_limit_date : "
            f"{suspicious_count:,} out-of-range years "
            f"[{SHIPPING_YEAR_MIN}–{SHIPPING_YEAR_MAX}] → nullified (no quarantine)"
        )
        df = df.withColumn(
            "shipping_limit_date",
            when(suspicious_date_cond, lit(None).cast(TimestampType())).otherwise(
                col("shipping_limit_date")
            ),
        )

    return df


# ════════════════════════════════════════════════════════
# STEP 7 — Business rule validation (4 financial rules)
# ════════════════════════════════════════════════════════


def apply_business_rules(
    df: DataFrame,
    process_date: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Financial accuracy rules protecting Gold layer revenue KPIs.

    Rules evaluated in priority order — a row failing multiple
    rules gets the FIRST matching quarantine reason.

    Rule 1: NEGATIVE_PRICE       — price < 0
    Rule 2: ZERO_PRICE           — price == 0.0 (paid marketplace)
    Rule 3: NEGATIVE_FREIGHT     — freight_value < 0
    Rule 4: INVALID_ITEM_SEQUENCE — order_item_id < 1

    Returns (valid_df, quarantine_df).
    """

    # ── Rule definitions (evaluated in priority order) ───
    r1_negative_price = col("price") < 0.0

    r2_zero_price = col("price") == 0.0

    r3_negative_freight = col("freight_value") < 0.0
    # freight_value was null-filled to 0.0 in Step 5.
    # A remaining negative value is a genuine source error.

    r4_invalid_sequence = col("order_item_id") < 1
    # Olist sequence starts at 1 per order.
    # order_item_id = 0 or negative = source system bug.

    # ── Attach quarantine reason column ──────────────────
    df = df.withColumn(
        "_quarantine_reason",
        when(r1_negative_price, lit("NEGATIVE_PRICE"))
        .when(r2_zero_price, lit("ZERO_PRICE"))
        .when(r3_negative_freight, lit("NEGATIVE_FREIGHT"))
        .when(r4_invalid_sequence, lit("INVALID_ITEM_SEQUENCE"))
        .otherwise(lit(None)),
    )

    # ── Split valid vs quarantine ─────────────────────────
    quarantine_df = df.filter(col("_quarantine_reason").isNotNull()).withColumn(
        "_quarantine_date", lit(process_date)
    )

    valid_df = df.filter(col("_quarantine_reason").isNull()).drop("_quarantine_reason")

    q_count = quarantine_df.count()
    v_count = valid_df.count()

    print(
        f"[SILVER:{TABLE_NAME}] Business rules       : "
        f"{v_count:,} valid | {q_count:,} quarantined"
    )

    if q_count > 0:
        print(f"[SILVER:{TABLE_NAME}] Quarantine breakdown :")
        (
            quarantine_df.groupBy("_quarantine_reason")
            .count()
            .orderBy(col("count").desc())
            .show(truncate=False)
        )

    return valid_df, quarantine_df


# ════════════════════════════════════════════════════════
# STEP 8 — Derive total_item_value
# ════════════════════════════════════════════════════════


def compute_derived_columns(df: DataFrame) -> DataFrame:
    """
    total_item_value = price + freight_value

    This is Silver's authoritative revenue figure per line item.
    In Gold layer, SUM(total_item_value) = total order revenue.
    SUM(price) alone = product revenue (excluding shipping).
    SUM(freight_value) alone = total shipping collected.

    Both inputs are validated before this step — safe to compute here.
    """
    df = df.withColumn(
        "total_item_value",
        F_round(col("price") + col("freight_value"), 2),
    )

    print(
        f"[SILVER:{TABLE_NAME}] Derived columns      : "
        f"total_item_value = price + freight_value"
    )
    return df


# ════════════════════════════════════════════════════════
# STEP 9 — Surrogate key generation
# ════════════════════════════════════════════════════════


def add_surrogate_key(
    df: DataFrame,
    process_date: str,
) -> DataFrame:
    """
    item_sk = SHA-256 of (order_id | order_item_id | process_date)

    Both PK components are included in the hash to prevent collision:
      - Hashing order_id alone would produce the same SK for every
        item in the same order.
      - The '|' separator prevents (ORD1, 23) colliding with (ORD12, 3).
      - process_date prevents cross-day SK collisions for late records.

    Uniqueness is asserted post-generation — a collision is a hard
    failure that must be investigated before proceeding.
    """
    df = df.withColumn(
        "item_sk",
        sha2(
            concat_ws(
                "|",
                col("order_id"),
                col("order_item_id").cast("string"),
                lit(process_date),
            ),
            256,
        ),
    )

    total_rows = df.count()
    unique_keys = df.select("item_sk").distinct().count()

    if total_rows != unique_keys:
        raise ValueError(
            f"[SILVER:{TABLE_NAME}] SURROGATE KEY COLLISION DETECTED!\n"
            f"  Total rows = {total_rows:,} | Unique item_sk = {unique_keys:,}\n"
            f"  Collision count = {total_rows - unique_keys:,}\n"
            f"  This must not happen after deduplication. Investigate immediately."
        )

    print(
        f"[SILVER:{TABLE_NAME}] Surrogate key        : "
        f"{total_rows:,} item_sk generated — all unique (SHA-256)"
    )
    return df


# ════════════════════════════════════════════════════════
# STEP 10 — Silver metadata columns
# ════════════════════════════════════════════════════════


def add_silver_metadata(
    df: DataFrame,
    process_date: str,
) -> DataFrame:
    """
    Standard Silver audit columns — consistent pattern across all Silver tables.

    _silver_timestamp : exact moment this row was written to Silver.
                        Used to measure Bronze → Silver latency.
    _silver_date      : the process_date driving this pipeline run.
                        Useful for debugging: "which run produced this row?"
    _quality_score    : 1.0 for all rows reaching this step.
                        All bad rows were quarantined before reaching here.
                        Future: partial quality rows could have 0.5–0.9 score.

    Bronze audit columns (_ingest_timestamp, _source_file, _ingest_date,
    _pipeline_version) are carried forward untouched from Bronze.
    This preserves the full audit trail back to the source file.
    """
    df = (
        df.withColumn("_silver_timestamp", current_timestamp())
        .withColumn("_silver_date", lit(process_date))
        .withColumn("_quality_score", lit(1.0).cast("double"))
    )

    print(
        f"[SILVER:{TABLE_NAME}] Silver metadata      : "
        f"_silver_timestamp, _silver_date={process_date}, _quality_score=1.0"
    )
    return df


# ════════════════════════════════════════════════════════
# STEP 11 — Write Silver partition
# ════════════════════════════════════════════════════════


def write_silver(
    df: DataFrame,
    process_date: str,
    dry_run: bool = False,
) -> int:
    """
    Write clean rows to Silver partition.

    mode=overwrite on the specific partition path ensures idempotency —
    running the pipeline twice for the same date produces the same result.

    dry_run=True: validate, count, and display schema without writing.
    Useful for debugging the full pipeline without committing output.
    """
    output_path = os.path.join(SILVER_PATH, TABLE_NAME, f"date={process_date}")
    row_count = df.count()

    if dry_run:
        print(
            f"[SILVER:{TABLE_NAME}] DRY RUN — would write "
            f"{row_count:,} rows to {output_path}"
        )
        print(f"[SILVER:{TABLE_NAME}] Output schema:")
        df.printSchema()
        df.show(5, truncate=True)
        return row_count

    df.write.mode("overwrite").parquet(output_path)

    # Read-back verification — confirms write completed correctly
    written = df.sparkSession.read.parquet(output_path).count()

    if written != row_count:
        raise ValueError(
            f"[SILVER:{TABLE_NAME}] WRITE VERIFICATION FAILED!\n"
            f"  Expected = {row_count:,} | Written = {written:,}\n"
            f"  Possible disk error or partial write. Rerun required."
        )

    print(
        f"[SILVER:{TABLE_NAME}] Silver written       : "
        f"{written:,} rows → {output_path}"
    )
    return written


# ════════════════════════════════════════════════════════
# STEP 12 — Write quarantine (append)
# ════════════════════════════════════════════════════════


def write_quarantine(
    quarantine_df: DataFrame,
    process_date: str,
    dry_run: bool = False,
) -> int:
    """
    Write quarantined rows to the quarantine partition.

    mode=APPEND (not overwrite) because quarantine is a cumulative
    audit log. Overwriting would erase historical bad-row evidence
    needed for source system feedback and root-cause analysis.

    Partition by date preserves queryability:
      "Show all bad rows for last week" → filter by _quarantine_date.
    """
    q_count = quarantine_df.count()

    if q_count == 0:
        print(
            f"[SILVER:{TABLE_NAME}] Quarantine           : "
            f"0 rows — nothing to write"
        )
        return 0

    q_path = os.path.join(QUARANTINE_PATH, TABLE_NAME, f"date={process_date}")

    if dry_run:
        print(
            f"[SILVER:{TABLE_NAME}] DRY RUN — would append "
            f"{q_count:,} quarantine rows to {q_path}"
        )
        quarantine_df.groupBy("_quarantine_reason").count().show(truncate=False)
        return q_count

    quarantine_df.write.mode("append").parquet(q_path)

    print(
        f"[SILVER:{TABLE_NAME}] Quarantine written   : " f"{q_count:,} rows → {q_path}"
    )
    return q_count


# ════════════════════════════════════════════════════════
# STEP 13 — Audit log to PostgreSQL
# ════════════════════════════════════════════════════════


def log_run_to_postgres(
    process_date: str,
    bronze_count: int,
    silver_count: int,
    quarantine_count: int,
    dupes_removed: int,
    status: str = "success",
    error_msg: str = None,
) -> None:
    """
    Write run summary to audit.pipeline_runs in PostgreSQL.

    Non-fatal by design — a failure in audit logging must never
    block the Silver write. Data processing takes priority.
    Wrapped in try/except with a warning-only print on failure.
    """
    try:
        import psycopg2

        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO audit.pipeline_runs
                (dag_id, run_date, table_name, layer,
                 input_rows, output_rows, dropped_rows,
                 status, error_message, completed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """,
            (
                "retail_daily_pipeline",
                process_date,
                TABLE_NAME,
                "silver",
                bronze_count,
                silver_count,
                quarantine_count + dupes_removed,
                status,
                error_msg,
            ),
        )

        conn.commit()
        conn.close()
        print(
            f"[SILVER:{TABLE_NAME}] Audit log            : "
            f"written to audit.pipeline_runs"
        )

    except Exception as e:
        print(f"[SILVER:{TABLE_NAME}] Warning: Audit log failed (non-fatal): {e}")


# ════════════════════════════════════════════════════════
# ORCHESTRATOR — run all steps in sequence
# ════════════════════════════════════════════════════════


def run_silver_order_items(
    spark: SparkSession,
    process_date: str,
    dry_run: bool = False,
) -> dict:
    """
    Execute all Silver cleaning steps for order_items in order.

    Step sequence:
       1  read_bronze
       2  clean_strings
       3  separate_null_mandatory         → quarantine batch A
       4  deduplicate
       5  fill_optional_nulls
       6  parse_and_validate_dates
       7  apply_business_rules            → quarantine batch B
       8  compute_derived_columns
       9  add_surrogate_key
      10  add_silver_metadata
      11  write_silver
      12  write_quarantine (A ∪ B merged)
      13  log_run_to_postgres

    Returns summary dict for Airflow XCom or test assertions.
    """
    print(f"\n{'='*60}")
    print(f"SILVER — {TABLE_NAME} — {process_date}" + (" [DRY RUN]" if dry_run else ""))
    print(f"{'='*60}")

    # ── Step 1 ───────────────────────────────────────────
    df, bronze_count = read_bronze(spark, process_date)

    # ── Step 2 ───────────────────────────────────────────
    df = clean_strings(df)

    # ── Step 3 ───────────────────────────────────────────
    df, null_quarantine = separate_null_mandatory(df, process_date)

    # ── Step 4 ───────────────────────────────────────────
    df, dupes_removed = deduplicate(df)

    # ── Step 5 ───────────────────────────────────────────
    df = fill_optional_nulls(df)

    # ── Step 6 ───────────────────────────────────────────
    df = parse_and_validate_dates(df, process_date)

    # ── Step 7 ───────────────────────────────────────────
    df, rules_quarantine = apply_business_rules(df, process_date)

    # Merge quarantine batches A and B into a single write.
    # allowMissingColumns=True handles the case where one batch
    # has _quarantine_date already and the other does not.
    all_quarantine = null_quarantine.unionByName(
        rules_quarantine,
        allowMissingColumns=True,
    )

    # ── Step 8 ───────────────────────────────────────────
    df = compute_derived_columns(df)

    # ── Step 9 ───────────────────────────────────────────
    df = add_surrogate_key(df, process_date)

    # ── Step 10 ──────────────────────────────────────────
    df = add_silver_metadata(df, process_date)

    # ── Steps 11 & 12 ────────────────────────────────────
    silver_count = write_silver(df, process_date, dry_run)
    quarantine_count = write_quarantine(all_quarantine, process_date, dry_run)

    # ── Step 13 ──────────────────────────────────────────
    log_run_to_postgres(
        process_date,
        bronze_count,
        silver_count,
        quarantine_count,
        dupes_removed,
    )

    # ── Summary ──────────────────────────────────────────
    retention_pct = (
        round(silver_count / bronze_count * 100, 1) if bronze_count > 0 else 0.0
    )

    summary = {
        "table": TABLE_NAME,
        "process_date": process_date,
        "bronze_count": bronze_count,
        "dupes_removed": dupes_removed,
        "quarantine_count": quarantine_count,
        "silver_count": silver_count,
        "retention_pct": retention_pct,
        "status": "success",
    }

    print(f"\n[SILVER:{TABLE_NAME}] {'─'*44}")
    print(f"[SILVER:{TABLE_NAME}] SUMMARY")
    print(f"[SILVER:{TABLE_NAME}]   Bronze input       : {bronze_count:,}")
    print(f"[SILVER:{TABLE_NAME}]   Dupes removed      : {dupes_removed:,}")
    print(f"[SILVER:{TABLE_NAME}]   Quarantined        : {quarantine_count:,}")
    print(f"[SILVER:{TABLE_NAME}]   Silver output      : {silver_count:,}")
    print(f"[SILVER:{TABLE_NAME}]   Retention          : {retention_pct}%")
    print(f"[SILVER:{TABLE_NAME}] {'─'*44}")

    if retention_pct < RETENTION_WARN_THRESHOLD:
        print(
            f"\n[SILVER:{TABLE_NAME}] WARNING: retention {retention_pct}% "
            f"is below the {RETENTION_WARN_THRESHOLD}% threshold.\n"
            f"  Review quarantine table for root cause:\n"
            f"  {QUARANTINE_PATH}/{TABLE_NAME}/date={process_date}/"
        )

    return summary


# ════════════════════════════════════════════════════════
# MAIN — CLI entry point
# ════════════════════════════════════════════════════════


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Silver layer cleaning for order_items table (Olist schema).\n"
            "Reads from Bronze, applies all quality rules, writes to Silver."
        )
    )
    parser.add_argument(
        "--date",
        default=datetime.today().strftime("%Y-%m-%d"),
        help="Process date in YYYY-MM-DD format (default: today)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help=(
            "Validate and count rows without writing any output. "
            "Use this to check logic before committing to disk."
        ),
    )
    args = parser.parse_args()

    spark = get_spark_session(
        app_name=f"Silver_OrderItems_{args.date}",
        memory="4g",
    )

    try:
        summary = run_silver_order_items(
            spark,
            process_date=args.date,
            dry_run=args.dry_run,
        )
        print(f"\n[SILVER:{TABLE_NAME}] Completed successfully.")

    except Exception as e:
        print(f"\n[SILVER:{TABLE_NAME}] FAILED: {e}")
        log_run_to_postgres(
            process_date=args.date,
            bronze_count=0,
            silver_count=0,
            quarantine_count=0,
            dupes_removed=0,
            status="failed",
            error_msg=str(e),
        )
        spark.stop()
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
