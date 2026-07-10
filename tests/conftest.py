import pytest
from pyspark.sql import SparkSession

from orderflow.spark.session import build_local_spark


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark_session = build_local_spark("orderflow-tests")

    yield spark_session

    spark_session.stop()