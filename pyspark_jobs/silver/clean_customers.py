"""
Silver Layer — clean_customers.py
===================================
Grain       : 1 row = 1 customer record keyed on customer_id
              Olist quirk: customer_id is unique per ORDER, not per person.
              customer_unique_id identifies the physical person across orders.
              Both columns are preserved — all customer_id rows are needed
              for order joins in Gold layer.
PK          : customer_id
Source      : data/bronze/customers/ingest_date={date}/
Target      : data/silver/customers/date={date}/
Quarantine  : data/silver/_quarantine/customers/date={date}/

Actual Olist schema (olist_customers_dataset.csv):
  customer_id, customer_unique_id, customer_zip_code_prefix,
  customer_city, customer_state

Key design decisions:
  - customer_id is PK (one row per customer_id — each maps to one order)
  - customer_unique_id identifies the physical person (can repeat across orders)
  - customer_zip_code_prefix stored as integer in Olist — lpad to 5-digit string
  - customer_state validated against all 27 valid Brazilian state codes
  - customer_city null or blank → filled with "UNKNOWN" (not quarantined)
  - Surrogate key built on customer_id (PK) + process_date

Run:
  python3 pyspark_jobs/silver/clean_customers.py --date 2024-01-15
  python3 pyspark_jobs/silver/clean_customers.py --date 2024-01-15 --dry-run
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
    initcap,
    isnull,
    length,
    lit,
    lpad,
    row_number,
    sha2,
    trim,
    upper,
    when,
)
from pyspark.sql.types import (
    IntegerType,
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

TABLE_NAME = "customers"

RETENTION_WARN_THRESHOLD = 90.0

# All 26 Brazilian states + Federal District (DF).
# customer_state values outside this set after UPPER+TRIM are invalid.
VALID_BRAZILIAN_STATES = {
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
}

# Bronze schema matching actual Olist CSV columns.
# customer_zip_code_prefix is stored as integer in Olist
# (leading zeros stripped at source — recovered via lpad in Silver).
BRONZE_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), nullable=True),
        StructField("customer_unique_id", StringType(), nullable=True),
        StructField("customer_zip_code_prefix", IntegerType(), nullable=True),
        StructField("customer_city", StringType(), nullable=True),
        StructField("customer_state", StringType(), nullable=True),
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

    Reads from Parquet — Bronze already converted CSV to Parquet
    and added audit metadata columns (_ingest_timestamp, _source_file,
    _ingest_date, _pipeline_version). These are carried forward
    unchanged to preserve the full lineage back to source.
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
    Standardise all string columns before any validation step.
    These are silent in-place corrections — no quarantine triggered.

    customer_id        → TRIM (Olist IDs are md5 hashes)
    customer_unique_id → TRIM
    customer_state     → UPPER + TRIM (must match 2-letter code exactly)
    customer_city      → INITCAP + TRIM (title case for display consistency)

    String cleaning runs BEFORE null checks so that whitespace-only
    strings are correctly caught as blank rather than non-null.
    """
    df = (
        df.withColumn("customer_id", trim(col("customer_id")))
        .withColumn("customer_unique_id", trim(col("customer_unique_id")))
        .withColumn("customer_state", upper(trim(col("customer_state"))))
        .withColumn("customer_city", initcap(trim(col("customer_city"))))
    )

    print(
        f"[SILVER:{TABLE_NAME}] String cleaning      : "
        f"trim(customer_id, customer_unique_id), "
        f"upper(trim(customer_state)), initcap(trim(customer_city))"
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
    Mandatory columns: customer_id, customer_unique_id, customer_state.

    customer_id        → mandatory: PK, every order join depends on this
    customer_unique_id → mandatory: true customer identity for analytics
    customer_state     → mandatory: all geographic KPIs require a valid state

    Optional columns handled separately:
    customer_city              → null-filled with "UNKNOWN" in Step 5
    customer_zip_code_prefix   → normalised in Step 5, "00000" sentinel if null
    """
    null_mandatory_cond = (
        isnull(col("customer_id"))
        | isnull(col("customer_unique_id"))
        | isnull(col("customer_state"))
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
# STEP 4 — Deduplication on customer_id (PK)
# ════════════════════════════════════════════════════════


def deduplicate(df: DataFrame) -> Tuple[DataFrame, int]:
    """
    PK = customer_id. Each customer_id should appear exactly once
    in the Olist customers table.

    Important Olist model note:
      customer_unique_id identifies the physical person and CAN appear
      multiple times (one customer_id per order per person). This is
      intentional in the Olist schema — we do NOT deduplicate on
      customer_unique_id. All customer_id records are valid and
      required for order joins.

    ROW_NUMBER window keeps the latest _ingest_timestamp for each
    customer_id — handles source retransmission edge cases gracefully.
    Olist is clean in practice; this pattern provides production safety.
    """
    before = df.count()

    dedup_window = Window.partitionBy("customer_id").orderBy(
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
            f"(kept latest per customer_id)"
        )
    else:
        print(f"[SILVER:{TABLE_NAME}] Deduplication       : 0 duplicates (clean)")

    return df_deduped, dupes


# ════════════════════════════════════════════════════════
# STEP 5 — Fill and normalise optional columns
# ════════════════════════════════════════════════════════


def fill_and_normalise_optional(df: DataFrame) -> DataFrame:
    """
    Handles the two optional columns gracefully without quarantining.

    customer_city:
      Null or empty string after INITCAP+TRIM → "UNKNOWN".
      City is a display and grouping field — "UNKNOWN" is a safe
      sentinel that prevents null propagation in Gold aggregations.

    customer_zip_code_prefix:
      Stored as INTEGER in Olist CSV (leading zeros lost at source).
      Silver recovers them: cast to string → LPAD to 5 digits.
        Example: 1037 → "01037"  |  99950 → "99950"
      Null integer (no zip on record) → "00000" sentinel.
      The "00000" sentinel is caught and quarantined in Step 6
      as INVALID_ZIP_PREFIX — it is not a valid Brazilian CEP prefix.
    """
    # City: null or whitespace-only string → "UNKNOWN"
    df = df.withColumn(
        "customer_city",
        when(
            isnull(col("customer_city")) | (trim(col("customer_city")) == ""),
            lit("UNKNOWN"),
        ).otherwise(col("customer_city")),
    )

    # ZIP: cast integer → string → left-pad to 5 digits with "0"
    # coalesce handles null integer → "0" before lpad → "00000"
    df = df.withColumn(
        "customer_zip_code_prefix",
        lpad(
            coalesce(
                col("customer_zip_code_prefix").cast("string"),
                lit("0"),
            ),
            5,
            "0",
        ),
    )

    print(
        f"[SILVER:{TABLE_NAME}] Optional normalise   : "
        f"city null/blank → 'UNKNOWN' | "
        f"zip cast to string + lpad to 5 digits"
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
    Two business rules protecting geographic analysis accuracy.

    Rule 1 — INVALID_STATE_CODE (priority 1):
      customer_state not in the 27 valid Brazilian state codes.
      Evaluated after UPPER+TRIM cleaning in Step 2.
      Rows with invalid states cannot be used for any state-level KPI.

    Rule 2 — INVALID_ZIP_PREFIX (priority 2):
      customer_zip_code_prefix is "00000" (null sentinel) or not
      exactly 5 characters after normalisation in Step 5.
      "00000" is not a valid Brazilian CEP prefix (range: 01000–99999).
      A non-5-digit string would indicate an unexpectedly large zip integer
      from source (e.g., 6-digit value due to source system error).

    Rows failing both rules receive Rule 1 reason (priority order).
    """
    valid_states_list = sorted(VALID_BRAZILIAN_STATES)

    r1_invalid_state = ~col("customer_state").isin(valid_states_list)

    r2_invalid_zip = (col("customer_zip_code_prefix") == "00000") | (
        length(col("customer_zip_code_prefix")) != 5
    )

    df = df.withColumn(
        "_quarantine_reason",
        when(r1_invalid_state, lit("INVALID_STATE_CODE"))
        .when(r2_invalid_zip, lit("INVALID_ZIP_PREFIX"))
        .otherwise(lit(None)),
    )

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
    customer_sk = SHA-256 of (customer_id | process_date)

    customer_id is the PK — it alone uniquely identifies each row.
    process_date is included to prevent cross-day SK collisions for
    late-arriving or backfilled customer records.

    The pipe '|' separator is critical: prevents hash collisions
    between different customer_id + process_date combinations that
    would otherwise produce identical concatenated strings.

    Uniqueness assertion runs post-generation. A collision indicates
    a deduplication failure and must halt the pipeline immediately.
    """
    df = df.withColumn(
        "customer_sk",
        sha2(
            concat_ws("|", col("customer_id"), lit(process_date)),
            256,
        ),
    )

    total_rows = df.count()
    unique_keys = df.select("customer_sk").distinct().count()

    if total_rows != unique_keys:
        raise ValueError(
            f"[SILVER:{TABLE_NAME}] SURROGATE KEY COLLISION DETECTED!\n"
            f"  Total rows    = {total_rows:,}\n"
            f"  Unique SKs    = {unique_keys:,}\n"
            f"  Collisions    = {total_rows - unique_keys:,}\n"
            f"  This must not occur after deduplication. "
            f"Investigate immediately before proceeding."
        )

    print(
        f"[SILVER:{TABLE_NAME}] Surrogate key        : "
        f"{total_rows:,} customer_sk generated — all unique (SHA-256)"
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
    Standard Silver audit columns — identical pattern across all Silver tables.

    _silver_timestamp : exact moment Silver wrote this row.
                        Enables Bronze → Silver latency measurement.
    _silver_date      : process_date for this pipeline run.
                        Answers "which run produced this row?"
    _quality_score    : 1.0 — all rows reaching this step passed every check.
                        Quarantined rows never reach this step.

    Bronze audit columns (_ingest_timestamp, _source_file, _ingest_date,
    _pipeline_version) are carried forward untouched from Bronze,
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

    mode=overwrite on the specific partition path ensures idempotency.
    Running the pipeline twice for the same date produces the same output.

    Read-back verification after write confirms the Parquet write
    completed fully and the row count matches expectation.

    dry_run=True: count, print schema, and show sample rows without
    writing any files — useful for logic validation before commit.
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

    mode=APPEND preserves cumulative quarantine history across all runs.
    Overwriting would destroy evidence needed for source system feedback
    and root-cause analysis of recurring data quality issues.

    Partition by date keeps the quarantine table queryable:
      "Show all bad customer rows from last week" — filter _quarantine_date.
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

    Intentionally non-fatal — a logging failure must never block
    the Silver data write. Wrapped in try/except with warning-only
    output on failure. Data processing always takes priority.
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


def run_silver_customers(
    spark: SparkSession,
    process_date: str,
    dry_run: bool = False,
) -> dict:
    """
    Execute all Silver cleaning steps for customers in sequence.

    Step sequence:
       1  read_bronze
       2  clean_strings
       3  separate_null_mandatory          → quarantine batch A
       4  deduplicate
       5  fill_and_normalise_optional
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
    df = fill_and_normalise_optional(df)

    # ── Step 6 ───────────────────────────────────────────
    df, rules_quarantine = apply_business_rules(df, process_date)

    # Merge both quarantine batches into a single write.
    # allowMissingColumns=True handles schema differences between
    # batches (e.g. null_quarantine lacks business-rule-specific cols).
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
        description="Silver layer cleaning for customers table (Olist schema)."
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
        app_name=f"Silver_Customers_{args.date}",
        memory="4g",
    )

    try:
        summary = run_silver_customers(
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
