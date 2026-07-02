# Databricks notebook source
# This notebook is a Databricks wrapper.
# Real Spark logic lives in:
# src/orderflow/spark/hello_orders.py

from _bootstrap import add_project_src_to_pythonpath

add_project_src_to_pythonpath(dbutils)

# COMMAND ----------

from orderflow.spark.hello_orders import run_hello_orders

# COMMAND ----------

print("Hello from Azure Databricks + PySpark")
print(f"Spark version: {spark.version}")

# COMMAND ----------

orders_df, daily_orders_df = run_hello_orders(spark)

# COMMAND ----------

display(orders_df)

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

display(daily_orders_df)