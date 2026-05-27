"""
Centralized SparkSession factory.
Ek jagah se manage karo — baaki sab import karein.
"""

from pyspark.sql import SparkSession
import os


def get_spark_session(
    app_name: str = "RetailLakehouse", memory: str = "4g"
) -> SparkSession:
    """
    Configure aur return SparkSession.

    local[*]  = laptop pe sab cores use karo
    Production mein: .master("yarn") ya .master("spark://host:7077")
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # Memory settings — laptop ke hisaab se adjust karo
        .config("spark.driver.memory", memory)
        .config("spark.driver.maxResultSize", "2g")
        # Shuffle partitions — local ke liye 200 bahut zyada hai
        # Production cluster pe 200-400 theek hai
        .config("spark.sql.shuffle.partitions", "8")
        # Parquet compression
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Parquet schema merging — Bronze ke liye off (faster)
        # Silver mein on karo agar schema evolves
        .config("spark.sql.parquet.mergeSchema", "false")
        # Timezone consistent rakhna — bahut important!
        # Source system aur Spark ka timezone alag hone se
        # date columns wrong convert hoti hain
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        # UI off in dev — clutter se bachao
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    # INFO logs bahut zyada hote hain — WARN kaafi hai
    spark.sparkContext.setLogLevel("WARN")

    print(f"[SPARK] Session started: {app_name}")
    print(f"[SPARK] Version: {spark.version}")

    return spark
