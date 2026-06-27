# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

raw_orders = [
    (1, "customer_001", "2026-06-01", 120.50),
    (2, "customer_002", "2026-06-01", 75.20),
    (3, "customer_003", "2026-06-02", 310.00),
]

columns = ["order_id", "customer_id", "order_date", "order_amount"]

# COMMAND ----------

bronze_orders_df = (
    spark.createDataFrame(raw_orders, columns)
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_system", lit("manual_hello_world"))
    .withColumn("_source_layer", lit("bronze"))
)

display(bronze_orders_df)

# COMMAND ----------

bronze_orders_df.printSchema()

# COMMAND ----------

bronze_orders_df.createOrReplaceTempView("bronze_orders_hello")

spark.sql("""
SELECT
    order_date,
    COUNT(*) AS order_count,
    SUM(order_amount) AS total_order_amount
FROM bronze_orders_hello
GROUP BY order_date
ORDER BY order_date
""").display()