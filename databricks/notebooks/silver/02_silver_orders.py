# Databricks notebook source

# COMMAND ----------

from typing import Any

dbutils: Any
spark: Any

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

bootstrap_databricks_notebook(dbutils=dbutils, spark=spark)

# COMMAND ----------

from orderflow.config.constants import ORDERS_BRONZE_TABLE, ORDERS_SILVER_TABLE
from orderflow.silver.orders import run_orders_silver_tables

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver orders transformation",
    runner=run_orders_silver_tables,
    spark=spark,
    input_table=ORDERS_BRONZE_TABLE,
    output_table=ORDERS_SILVER_TABLE,
)
