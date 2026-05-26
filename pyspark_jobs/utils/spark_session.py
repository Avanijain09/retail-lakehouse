"""
Centralized SparkSession factory.

Har PySpark job isi function ko use karegi.
Ek hi jagah Spark configs maintain karna easy hota hai.
"""

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "RetailLakehouse") -> SparkSession:
    """
    Create and return a configured SparkSession.
    """

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark
