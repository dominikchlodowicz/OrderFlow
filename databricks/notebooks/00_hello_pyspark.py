# Databricks notebook source
print("Hello from Azure Databricks + PySpark")
print(f"Spark version: {spark.version}")

# COMMAND ----------

orders = [
    (1, "customer_001", "2026-06-01", 120.50),
    (2, "customer_002", "2026-06-01", 75.20),
    (3, "customer_003", "2026-06-02", 310.00),
]

columns = ["order_id", "customer_id", "order_date", "order_amount"]

orders_df = spark.createDataFrame(orders, columns)

display(orders_df)

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum

daily_orders_df = (
    orders_df
    .groupBy("order_date")
    .agg(spark_sum(col("order_amount")).alias("daily_order_amount"))
    .orderBy("order_date")
)

display(daily_orders_df)