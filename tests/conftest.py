"""
Shared pytest fixtures.
"""

import pytest

from pyspark_jobs.utils.spark_session import get_spark_session


@pytest.fixture(scope="session")
def spark():
    """
    Shared SparkSession for all tests.
    """

    spark = get_spark_session(app_name="RetailLakehouseTests")

    yield spark

    spark.stop()


@pytest.fixture
def sample_orders(spark):
    """
    Sample orders dataframe.
    """

    data = [
        ("ORD001", "CUST001", "S001", "2024-01-15", "DELIVERED", 1500.0),
        ("ORD002", "CUST002", "S001", "2024-01-15", "PENDING", 800.0),
        ("ORD003", None, "S002", "2024-01-15", "DELIVERED", 2000.0),
        ("ORD001", "CUST001", "S001", "2024-01-15", "DELIVERED", 1500.0),
    ]

    return spark.createDataFrame(
        data, ["order_id", "customer_id", "store_id", "order_date", "status", "revenue"]
    )
