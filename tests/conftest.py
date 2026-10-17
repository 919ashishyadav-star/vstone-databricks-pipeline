import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """A small local Spark session for unit tests."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("vstone-unit-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
