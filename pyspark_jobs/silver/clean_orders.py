"""
Silver layer transformation for orders table.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from pyspark_jobs.utils.spark_session import get_spark_session


def remove_duplicates(df: DataFrame) -> DataFrame:
    """
    Remove duplicate orders.
    """

    return df.dropDuplicates(["order_id"])


def separate_nulls(df: DataFrame, column_name: str):
    """
    Separate rows with null values.
    """

    valid_df = df.filter(col(column_name).isNotNull())

    quarantine_df = df.filter(col(column_name).isNull())

    return valid_df, quarantine_df


def clean_orders_df(df: DataFrame) -> DataFrame:
    """
    Full cleaning pipeline for orders dataframe.
    """

    df = remove_duplicates(df)

    valid_df, quarantine_df = separate_nulls(df, "customer_id")

    return valid_df


if __name__ == "__main__":

    spark = get_spark_session("SilverOrdersCleaning")

    print("[SILVER] Orders cleaning module ready.")

    spark.stop()
