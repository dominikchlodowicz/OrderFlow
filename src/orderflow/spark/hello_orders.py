from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, sum as spark_sum


def create_orders_df(spark: SparkSession) -> DataFrame:
    orders = [
        (1, "customer_001", "2026-06-01", 120.50),
        (2, "customer_002", "2026-06-01", 75.20),
        (3, "customer_003", "2026-06-02", 310.00),
    ]

    columns = ["order_id", "customer_id", "order_date", "order_amount"]

    return spark.createDataFrame(orders, columns)


def create_daily_orders_df(orders_df: DataFrame) -> DataFrame:
    return (
        orders_df
        .groupBy("order_date")
        .agg(spark_sum(col("order_amount")).alias("daily_order_amount"))
        .orderBy("order_date")
    )


def run_hello_orders(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    orders_df = create_orders_df(spark)
    daily_orders_df = create_daily_orders_df(orders_df)

    return orders_df, daily_orders_df