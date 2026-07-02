# PySpark fixture for whole test session

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark_session = (
        SparkSession.builder
        .appName("orderflow-tests")
        # 2 cores
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # guardrails for cost/performance saving in tests
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    
    yield spark_session 
    
    spark_session.stop()