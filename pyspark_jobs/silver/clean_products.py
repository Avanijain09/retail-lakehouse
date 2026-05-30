"""
Silver Layer — clean_products.py
==================================
Grain       : 1 row = 1 product keyed on product_id
PK          : product_id
Source      : data/bronze/products/ingest_date={date}/
Target      : data/silver/products/date={date}/
Quarantine  : data/silver/_quarantine/products/date={date}/

Actual Olist schema (olist_products_dataset.csv):
  product_id, product_category_name,
  product_name_lenght, product_description_lenght,
  product_photos_qty, product_weight_g,
  product_length_cm, product_height_cm, product_width_cm

Note: "product_name_lenght" and "product_description_lenght" are
Olist source typos — preserved exactly as-is to match the source schema.

Key design decisions:
  - Products is a DIMENSION table — not a daily transaction table.
  - Only product_id is mandatory (PK). All other columns are optional
    with appropriate defaults or null-tolerance.
  - product_category_name null → filled with "unknown" (many Olist
    products lack a category — cannot quarantine without losing inventory).
  - Physical dimension and weight columns (nullable in Olist) are
    validated ONLY when present — null does not quarantine.
  - Business rules catch physically impossible values: negatives,
    zeros, and values exceeding Olist's documented shipping limits.
  - Surrogate key built on product_id + process_date.

Run:
  python3 pyspark_jobs/silver/clean_products.py --date 2024-01-15
  python3 pyspark_jobs/silver/clean_products.py --date 2024-01-15 --dry-run
"""

import argparse
import os
import sys
from datetime import datetime
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    current_timestamp,
    isnull,
    lit,
    lower,
    regexp_replace,
    row_number,
    sha2,
    trim,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
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

TABLE_NAME = "products"

RETENTION_WARN_THRESHOLD = 90.0

# Olist shipping physical limits.
# Source: Olist seller documentation and Correios (Brazilian postal) limits.
# Rows with values exceeding these are data entry errors, not real products.
MAX_WEIGHT_G = 30_000.0  # 30 kg — Correios maximum for standard parcels
MAX_DIMENSION_CM = 200.0  # 200 cm — Correios maximum single dimension

# Sentinel value used when product_category_name is null.
# "unknown" (lowercase) is consistent with Olist's own lowercase category style.
CATEGORY_NULL_SENTINEL = "unknown"

# Bronze schema matching actual Olist CSV columns.
# Note deliberate preservation of Olist typos: "lenght" not "length".
BRONZE_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), nullable=True),
        StructField("product_category_name", StringType(), nullable=True),
        StructField("product_name_lenght", LongType(), nullable=True),
        StructField("product_description_lenght", LongType(), nullable=True),
        StructField("product_photos_qty", LongType(), nullable=True),
        StructField("product_weight_g", DoubleType(), nullable=True),
        StructField("product_length_cm", DoubleType(), nullable=True),
        StructField("product_height_cm", DoubleType(), nullable=True),
        StructField("product_width_cm", DoubleType(), nullable=True),
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

    Bronze carries audit columns (_ingest_timestamp, _source_file,
    _ingest_date, _pipeline_version) from ingestion — these are
    passed through unchanged to Silver to preserve full lineage.
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
    Standardise string columns before any validation.
    All corrections are silent — no quarantine triggered here.

    product_id:
      TRIM only. Olist product IDs are md5 hashes — already lowercase.
      Trimming removes any accidental leading/trailing whitespace from
      source CSV that would silently break join lookups in Gold layer.

    product_category_name:
      LOWER + TRIM + normalise internal whitespace.
      Olist categories are always lowercase (e.g. "cama_mesa_banho").
      Multiple spaces within the string are collapsed to a single space.
      This runs BEFORE null-fill in Step 5 so that whitespace-only
      strings ("   ") are treated identically to null in Step 5.
    """
    df = df.withColumn("product_id", trim(col("product_id"))).withColumn(
        "product_category_name",
        lower(
            trim(
                regexp_replace(
                    col("product_category_name"),
                    r"\s+",
                    " ",
                )
            )
        ),
    )

    print(
        f"[SILVER:{TABLE_NAME}] String cleaning      : "
        f"trim(product_id) | "
        f"lower(trim(normalise_spaces(product_category_name)))"
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
    Mandatory columns: product_id only.

    product_id → mandatory: PK, every order_items join on product_id
                 depends on this. A row without a product_id cannot
                 contribute to any Gold layer KPI.

    All other columns are optional:
      product_category_name  → null-filled with sentinel in Step 5
      product_name_lenght    → kept null (listing metadata, not analytical)
      product_description_lenght → kept null (listing metadata)
      product_photos_qty     → defaulted to 0 in Step 5
      product_weight_g       → kept null; validated if present in Step 6
      product_length_cm      → kept null; validated if present in Step 6
      product_height_cm      → kept null; validated if present in Step 6
      product_width_cm       → kept null; validated if present in Step 6
    """
    null_mandatory_cond = isnull(col("product_id"))

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
# STEP 4 — Deduplication on product_id (PK)
# ════════════════════════════════════════════════════════


def deduplicate(df: DataFrame) -> Tuple[DataFrame, int]:
    """
    PK = product_id. Each product_id must appear exactly once.

    ROW_NUMBER window keeps the row with the LATEST _ingest_timestamp
    for each product_id. This correctly handles source retransmissions
    where an updated product record (e.g. category correction, weight
    update) arrives in a later Bronze ingest.

    The older superseded record is silently dropped — not quarantined.
    It is valid data that has been replaced, not bad data.

    Olist product dataset is a full snapshot on each ingest —
    duplicates are rare in practice but the pattern provides
    production-grade safety for incremental or partial reloads.
    """
    before = df.count()

    dedup_window = Window.partitionBy("product_id").orderBy(
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
            f"(kept latest per product_id)"
        )
    else:
        print(f"[SILVER:{TABLE_NAME}] Deduplication       : 0 duplicates (clean)")

    return df_deduped, dupes


# ════════════════════════════════════════════════════════
# STEP 5 — Fill optional columns with safe defaults
# ════════════════════════════════════════════════════════


def fill_optional_nulls(df: DataFrame) -> DataFrame:
    """
    Apply safe defaults to optional columns before business validation.

    product_category_name:
      Null or empty string (after Step 2 cleaning) → CATEGORY_NULL_SENTINEL.
      Rationale: a significant portion of Olist products have no category.
      Quarantining them would remove valid products from all category KPIs.
      "unknown" sentinel allows Gold to count uncategorised products
      separately rather than discarding them.

    product_photos_qty:
      Null → 0 (a product listing with no photos uploaded is valid).

    product_name_lenght, product_description_lenght:
      Left as null — these are listing quality metadata columns used
      for seller analytics only, not for revenue or category analysis.
      No default is meaningful; null preservation avoids invented data.

    product_weight_g, product_length_cm, product_height_cm, product_width_cm:
      Left as null — physical attribute data is missing for some Olist
      products (digital goods, seller non-compliance). Null-filling with
      a default weight/dimension would corrupt logistics cost analysis.
      These columns are validated only when not null in Step 6.
    """
    df = df.withColumn(
        "product_category_name",
        when(
            isnull(col("product_category_name"))
            | (trim(col("product_category_name")) == ""),
            lit(CATEGORY_NULL_SENTINEL),
        ).otherwise(col("product_category_name")),
    ).withColumn(
        "product_photos_qty",
        coalesce(col("product_photos_qty"), lit(0).cast(LongType())),
    )

    print(
        f"[SILVER:{TABLE_NAME}] Optional null fill   : "
        f"category null/blank → '{CATEGORY_NULL_SENTINEL}' | "
        f"photos_qty null → 0"
    )
    return df


# ════════════════════════════════════════════════════════
# STEP 6 — Business rule validation
# ════════════════════════════════════════════════════════


def apply_business_rules(
    df: DataFrame,
    process_date: str,
) -> Tuple[DataFrame, DataFrame]:
    """
    Physical dimension and weight validation rules.

    All rules apply ONLY when the column is NOT NULL — a null physical
    attribute is tolerated (see Step 5 rationale). Only impossible
    values when the measurement is present are quarantined.

    Priority order (first matching rule wins):

    Rule 1 — NEGATIVE_WEIGHT:
      product_weight_g IS NOT NULL AND product_weight_g < 0
      A product cannot have negative mass.

    Rule 2 — ZERO_WEIGHT:
      product_weight_g IS NOT NULL AND product_weight_g == 0.0
      Zero-weight physical products are not valid in a shipping context.
      (Digital goods in Olist do not have weight records — their
       weight_g column is null, not zero.)

    Rule 3 — EXCESSIVE_WEIGHT:
      product_weight_g IS NOT NULL AND product_weight_g > MAX_WEIGHT_G
      Exceeds Correios standard parcel limit. Indicates a data entry
      error (e.g. weight entered in grams when kg was intended).

    Rule 4 — NEGATIVE_DIMENSION:
      Any of length_cm, height_cm, width_cm IS NOT NULL AND < 0
      A physical dimension cannot be negative.

    Rule 5 — ZERO_DIMENSION:
      Any of length_cm, height_cm, width_cm IS NOT NULL AND == 0.0
      A product with a recorded but zero dimension is a data error.

    Rule 6 — EXCESSIVE_DIMENSION:
      Any of length_cm, height_cm, width_cm IS NOT NULL AND > MAX_DIMENSION_CM
      Exceeds Correios single-dimension limit for standard parcels.

    Rule 7 — NEGATIVE_PHOTOS_QTY:
      product_photos_qty < 0 (after null-fill to 0 in Step 5)
      A count of photos cannot be negative.
    """

    # ── Helper: null-safe numeric condition ─────────────
    # col.isNotNull() & (col <operator> value)
    def nn(column, op, value):
        """Return condition that is True only when column is not null
        AND satisfies the given operator against value."""
        c = col(column)
        if op == "lt":
            return c.isNotNull() & (c < value)
        if op == "eq":
            return c.isNotNull() & (c == value)
        if op == "gt":
            return c.isNotNull() & (c > value)

    # ── Rule definitions ─────────────────────────────────
    r1_neg_weight = nn("product_weight_g", "lt", 0.0)

    r2_zero_weight = nn("product_weight_g", "eq", 0.0)

    r3_excess_weight = nn("product_weight_g", "gt", MAX_WEIGHT_G)

    # Any dimension negative — checked per column, OR'd together
    r4_neg_dim = (
        nn("product_length_cm", "lt", 0.0)
        | nn("product_height_cm", "lt", 0.0)
        | nn("product_width_cm", "lt", 0.0)
    )

    # Any dimension zero — checked per column, OR'd together
    r5_zero_dim = (
        nn("product_length_cm", "eq", 0.0)
        | nn("product_height_cm", "eq", 0.0)
        | nn("product_width_cm", "eq", 0.0)
    )

    # Any dimension excessive — checked per column, OR'd together
    r6_excess_dim = (
        nn("product_length_cm", "gt", MAX_DIMENSION_CM)
        | nn("product_height_cm", "gt", MAX_DIMENSION_CM)
        | nn("product_width_cm", "gt", MAX_DIMENSION_CM)
    )

    # product_photos_qty was null-filled to 0 in Step 5
    # A remaining negative value is a source system error
    r7_neg_photos = col("product_photos_qty") < 0

    # ── Assign quarantine reason in priority order ───────
    df = df.withColumn(
        "_quarantine_reason",
        when(r1_neg_weight, lit("NEGATIVE_WEIGHT"))
        .when(r2_zero_weight, lit("ZERO_WEIGHT"))
        .when(r3_excess_weight, lit("EXCESSIVE_WEIGHT"))
        .when(r4_neg_dim, lit("NEGATIVE_DIMENSION"))
        .when(r5_zero_dim, lit("ZERO_DIMENSION"))
        .when(r6_excess_dim, lit("EXCESSIVE_DIMENSION"))
        .when(r7_neg_photos, lit("NEGATIVE_PHOTOS_QTY"))
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
# STEP 7 — Surrogate key generation
# ════════════════════════════════════════════════════════


def add_surrogate_key(
    df: DataFrame,
    process_date: str,
) -> DataFrame:
    """
    product_sk = SHA-256 of (product_id | process_date)

    product_id is the PK — it alone uniquely identifies each row.
    process_date is included to prevent cross-day SK collisions for
    late-arriving or backfilled product records.

    The pipe '|' separator prevents hash collisions between different
    product_id + process_date combinations that would otherwise
    produce identical concatenated strings.

    Uniqueness is asserted immediately after generation. A collision
    indicates a deduplication failure upstream and must halt the
    pipeline — proceeding with duplicate SKs corrupts dim_product
    joins in Gold layer.
    """
    df = df.withColumn(
        "product_sk",
        sha2(
            concat_ws("|", col("product_id"), lit(process_date)),
            256,
        ),
    )

    total_rows = df.count()
    unique_keys = df.select("product_sk").distinct().count()

    if total_rows != unique_keys:
        raise ValueError(
            f"[SILVER:{TABLE_NAME}] SURROGATE KEY COLLISION DETECTED!\n"
            f"  Total rows    = {total_rows:,}\n"
            f"  Unique SKs    = {unique_keys:,}\n"
            f"  Collisions    = {total_rows - unique_keys:,}\n"
            f"  Deduplication failure upstream. "
            f"Investigate before proceeding."
        )

    print(
        f"[SILVER:{TABLE_NAME}] Surrogate key        : "
        f"{total_rows:,} product_sk generated — all unique (SHA-256)"
    )
    return df


# ════════════════════════════════════════════════════════
# STEP 8 — Silver metadata columns
# ════════════════════════════════════════════════════════


def add_silver_metadata(
    df: DataFrame,
    process_date: str,
) -> DataFrame:
    """
    Standard Silver audit columns — consistent pattern across all Silver tables.

    _silver_timestamp : exact moment Silver wrote this row.
                        Enables Bronze → Silver latency measurement.
    _silver_date      : process_date driving this pipeline run.
                        Answers "which run produced this row?"
    _quality_score    : 1.0 for all rows reaching this step.
                        All rows that failed any check were quarantined
                        before reaching here — score is always 1.0.

    Bronze audit columns (_ingest_timestamp, _source_file, _ingest_date,
    _pipeline_version) are carried forward from Bronze unchanged,
    preserving the complete audit trail back to the source CSV file.
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
# STEP 9 — Write Silver partition
# ════════════════════════════════════════════════════════


def write_silver(
    df: DataFrame,
    process_date: str,
    dry_run: bool = False,
) -> int:
    """
    Write clean rows to the Silver partition for this date.

    mode=overwrite on the specific partition path guarantees idempotency.
    Running the pipeline twice for the same date produces identical output.

    Read-back verification after write confirms the Parquet write
    completed fully and no rows were silently dropped by the writer.

    dry_run=True: count, print schema, show sample rows — no files written.
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

    written = df.sparkSession.read.parquet(output_path).count()

    if written != row_count:
        raise ValueError(
            f"[SILVER:{TABLE_NAME}] WRITE VERIFICATION FAILED!\n"
            f"  Expected = {row_count:,} | Written = {written:,}\n"
            f"  Possible partial write or disk error. Rerun required."
        )

    print(
        f"[SILVER:{TABLE_NAME}] Silver written       : "
        f"{written:,} rows → {output_path}"
    )
    return written


# ════════════════════════════════════════════════════════
# STEP 10 — Write quarantine (append)
# ════════════════════════════════════════════════════════


def write_quarantine(
    quarantine_df: DataFrame,
    process_date: str,
    dry_run: bool = False,
) -> int:
    """
    Append quarantined rows to the quarantine partition.

    mode=APPEND preserves cumulative quarantine history.
    Overwriting would erase evidence of previously quarantined rows
    needed for source system feedback and recurring quality issue tracking.

    Partition by date keeps the quarantine queryable:
      "Show all bad product rows this week" — filter _quarantine_date.
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
# STEP 11 — Audit log to PostgreSQL
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

    Intentionally non-fatal — an audit logging failure must never
    block the Silver data write. Data processing always takes priority.
    Failure produces a warning print only and does not propagate.
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


def run_silver_products(
    spark: SparkSession,
    process_date: str,
    dry_run: bool = False,
) -> dict:
    """
    Execute all Silver cleaning steps for products in sequence.

    Step sequence:
       1  read_bronze
       2  clean_strings
       3  separate_null_mandatory          → quarantine batch A
       4  deduplicate
       5  fill_optional_nulls
       6  apply_business_rules             → quarantine batch B
       7  add_surrogate_key
       8  add_silver_metadata
       9  write_silver
      10  write_quarantine (A ∪ B merged)
      11  log_run_to_postgres

    Returns a summary dict for Airflow XCom push or test assertions.
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
    df, rules_quarantine = apply_business_rules(df, process_date)

    # Merge both quarantine batches for a single write.
    # allowMissingColumns=True handles schema differences between
    # batches — null_quarantine may lack business-rule-specific cols.
    all_quarantine = null_quarantine.unionByName(
        rules_quarantine,
        allowMissingColumns=True,
    )

    # ── Step 7 ───────────────────────────────────────────
    df = add_surrogate_key(df, process_date)

    # ── Step 8 ───────────────────────────────────────────
    df = add_silver_metadata(df, process_date)

    # ── Steps 9 & 10 ─────────────────────────────────────
    silver_count = write_silver(df, process_date, dry_run)
    quarantine_count = write_quarantine(all_quarantine, process_date, dry_run)

    # ── Step 11 ──────────────────────────────────────────
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
        description="Silver layer cleaning for products table (Olist schema)."
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
            "Use to verify logic before committing to disk."
        ),
    )
    args = parser.parse_args()

    spark = get_spark_session(
        app_name=f"Silver_Products_{args.date}",
        memory="4g",
    )

    try:
        summary = run_silver_products(
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
