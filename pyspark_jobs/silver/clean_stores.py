"""
Silver Layer — clean_stores.py
================================
Grain       : 1 row = 1 seller record keyed on seller_id
              Olist uses "sellers" terminology; this pipeline maps
              sellers to the "stores" dimension (TABLE_NAME = "stores").
PK          : seller_id
Source      : data/bronze/stores/ingest_date={date}/
Target      : data/silver/stores/date={date}/
Quarantine  : data/silver/_quarantine/stores/date={date}/

Actual Olist schema (olist_sellers_dataset.csv):
  seller_id, seller_zip_code_prefix,
  seller_city, seller_state

Key design decisions:
  - seller_id and seller_state are mandatory (PK + geographic analysis key).
  - seller_city null or blank → filled with "UNKNOWN" sentinel.
  - seller_zip_code_prefix stored as integer in Olist (leading zeros lost
    at source) — recovered via cast to string + lpad to 5 digits.
  - seller_state validated against all 27 valid Brazilian state codes.
  - "00000" zip prefix and non-5-digit zips → quarantine INVALID_ZIP_PREFIX.
  - Surrogate key = seller_sk built on seller_id + process_date.

Run:
  python3 pyspark_jobs/silver/clean_stores.py --date 2024-01-15
  python3 pyspark_jobs/silver/clean_stores.py --date 2024-01-15 --dry-run
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

TABLE_NAME = "stores"

RETENTION_WARN_THRESHOLD = 90.0

# All 26 Brazilian states + Federal District.
# Identical set used in clean_customers.py for consistency.
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

CITY_NULL_SENTINEL = "UNKNOWN"

# Bronze schema matching actual Olist sellers CSV columns.
# seller_zip_code_prefix is stored as INTEGER in Olist
# (leading zeros stripped at source — recovered via lpad in Silver).
BRONZE_SCHEMA = StructType(
    [
        StructField("seller_id", StringType(), nullable=True),
        StructField("seller_zip_code_prefix", IntegerType(), nullable=True),
        StructField("seller_city", StringType(), nullable=True),
        StructField("seller_state", StringType(), nullable=True),
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

    Bronze audit columns (_ingest_timestamp, _source_file,
    _ingest_date, _pipeline_version) are carried forward unchanged
    to Silver to preserve full lineage back to the source file.
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
    All corrections are silent — no quarantine triggered here.

    seller_id    : TRIM only. Olist IDs are md5 hashes — already
                   lowercase. Trimming removes accidental whitespace
                   that would silently break Gold layer joins.

    seller_state : UPPER + TRIM. Must match 2-letter Brazilian state
                   code exactly. Runs before null check so whitespace-
                   only strings are treated as blank, not as a state.

    seller_city  : INITCAP + TRIM. Title case for display consistency
                   across dashboards. Runs before null-fill so that
                   whitespace-only strings collapse to "" and are
                   caught by the UNKNOWN sentinel in Step 5.
    """
    df = (
        df.withColumn("seller_id", trim(col("seller_id")))
        .withColumn("seller_state", upper(trim(col("seller_state"))))
        .withColumn("seller_city", initcap(trim(col("seller_city"))))
    )

    print(
        f"[SILVER:{TABLE_NAME}] String cleaning      : "
        f"trim(seller_id) | "
        f"upper(trim(seller_state)) | "
        f"initcap(trim(seller_city))"
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
    Mandatory columns: seller_id, seller_state.

    seller_id    → mandatory: PK. Every order_items join on seller_id
                   requires this. A row without seller_id cannot be
                   used in any seller-level KPI in Gold layer.

    seller_state → mandatory: all geographic KPIs require a valid
                   state. A null state cannot be corrected with a
                   default — no meaningful sentinel exists for state.

    Optional columns (handled in Step 5):
      seller_city            → null-filled with CITY_NULL_SENTINEL
      seller_zip_code_prefix → normalised via lpad; "00000" caught
                               by business rules in Step 6
    """
    null_mandatory_cond = isnull(col("seller_id")) | isnull(col("seller_state"))

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
# STEP 4 — Deduplication on seller_id (PK)
# ════════════════════════════════════════════════════════


def deduplicate(df: DataFrame) -> Tuple[DataFrame, int]:
    """
    PK = seller_id. Each seller_id must appear exactly once.

    ROW_NUMBER window keeps the row with the LATEST _ingest_timestamp
    for each seller_id. This handles source retransmissions where an
    updated seller record (e.g. address correction, city update)
    arrives in a later Bronze ingest.

    Superseded rows are silently dropped — not quarantined — because
    they are valid data that has been replaced, not bad data.

    Olist sellers dataset is a full snapshot on each ingest.
    Duplicates are rare in practice; this pattern provides
    production-grade safety for incremental or partial reloads.
    """
    before = df.count()

    dedup_window = Window.partitionBy("seller_id").orderBy(
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
            f"(kept latest per seller_id)"
        )
    else:
        print(f"[SILVER:{TABLE_NAME}] Deduplication       : 0 duplicates (clean)")

    return df_deduped, dupes


# ════════════════════════════════════════════════════════
# STEP 5 — Fill and normalise optional columns
# ════════════════════════════════════════════════════════


def fill_and_normalise_optional(df: DataFrame) -> DataFrame:
    """
    Apply safe defaults to optional columns before business validation.

    seller_city:
      Null or whitespace-only string (after INITCAP+TRIM in Step 2)
      → CITY_NULL_SENTINEL ("UNKNOWN"). City is a grouping and display
      field; "UNKNOWN" prevents null propagation in Gold aggregations
      while making uncategorised sellers clearly identifiable.

    seller_zip_code_prefix:
      Stored as INTEGER in Olist CSV (leading zeros stripped at source).
      Silver recovers them: cast integer to string → LPAD to 5 digits.
        Example:  1310 → "01310"   |   99900 → "99900"
      Null integer (no zip recorded) → "0" before lpad → "00000".
      The "00000" sentinel is physically impossible as a Brazilian CEP
      prefix (valid range: 01000–99999) and is caught by business
      rule INVALID_ZIP_PREFIX in Step 6.
    """
    df = df.withColumn(
        "seller_city",
        when(
            isnull(col("seller_city")) | (trim(col("seller_city")) == ""),
            lit(CITY_NULL_SENTINEL),
        ).otherwise(col("seller_city")),
    ).withColumn(
        "seller_zip_code_prefix",
        lpad(
            coalesce(
                col("seller_zip_code_prefix").cast("string"),
                lit("0"),
            ),
            5,
            "0",
        ),
    )

    print(
        f"[SILVER:{TABLE_NAME}] Optional normalise   : "
        f"city null/blank → '{CITY_NULL_SENTINEL}' | "
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
    Evaluated after string cleaning (Step 2) and normalisation (Step 5).

    Rule 1 — INVALID_STATE_CODE (priority 1):
      seller_state not in the 27 valid Brazilian state codes after
      UPPER+TRIM. Rows with invalid states cannot participate in any
      state-level or regional KPI in Gold layer.

    Rule 2 — INVALID_ZIP_PREFIX (priority 2):
      seller_zip_code_prefix is "00000" (null sentinel from Step 5) or
      not exactly 5 characters after normalisation. "00000" is not a
      valid Brazilian CEP prefix (range: 01000–99999). A non-5-digit
      string would indicate a source value exceeding expected range.

    A row failing both rules receives Rule 1's reason (priority order).
    """
    valid_states_list = sorted(VALID_BRAZILIAN_STATES)

    r1_invalid_state = ~col("seller_state").isin(valid_states_list)

    r2_invalid_zip = (col("seller_zip_code_prefix") == "00000") | (
        length(col("seller_zip_code_prefix")) != 5
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
    seller_sk = SHA-256 of (seller_id | process_date)

    seller_id is the PK — it alone uniquely identifies each row.
    process_date is included to prevent cross-day SK collisions for
    late-arriving or backfilled seller records.

    The pipe '|' separator prevents hash collisions between different
    seller_id + process_date combinations that would otherwise produce
    identical concatenated strings.

    Uniqueness is asserted immediately after generation. A collision
    indicates a deduplication failure upstream and must halt the
    pipeline — proceeding with duplicate SKs corrupts seller dimension
    joins in Gold layer.
    """
    df = df.withColumn(
        "seller_sk",
        sha2(
            concat_ws("|", col("seller_id"), lit(process_date)),
            256,
        ),
    )

    total_rows = df.count()
    unique_keys = df.select("seller_sk").distinct().count()

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
        f"{total_rows:,} seller_sk generated — all unique (SHA-256)"
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
    _pipeline_version) are carried forward unchanged from Bronze,
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

    Read-back verification confirms the Parquet write completed fully
    and no rows were silently dropped by the writer.

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

    mode=APPEND preserves cumulative quarantine history across all runs.
    Overwriting would erase evidence needed for source system feedback
    and tracking of recurring data quality issues over time.

    Partition by date keeps quarantine queryable:
      "Show all bad seller rows this week" — filter _quarantine_date.
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


def run_silver_stores(
    spark: SparkSession,
    process_date: str,
    dry_run: bool = False,
) -> dict:
    """
    Execute all Silver cleaning steps for stores (sellers) in sequence.

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

    # Merge both quarantine batches for a single write.
    # allowMissingColumns=True handles schema differences between
    # batches generated at different pipeline stages.
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
        description="Silver layer cleaning for stores/sellers table (Olist schema)."
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
        app_name=f"Silver_Stores_{args.date}",
        memory="4g",
    )

    try:
        summary = run_silver_stores(
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
