# Databricks notebook source

# COMMAND ----------

from typing import Any

dbutils: Any
spark: Any

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

bootstrap_databricks_notebook(
    dbutils=dbutils,
    spark=spark,
)

# COMMAND ----------

from orderflow.bronze.order_items import run_order_items_bronze_table
from orderflow.config.constants import ORDER_ITEMS_BRONZE_INPUT_PATH, ORDER_ITEMS_BRONZE_TABLE

# COMMAND ----------

run_databricks_volume_to_table_step(
    step_name="Bronze order items ingestion",
    runner=run_order_items_bronze_table,
    spark=spark,
    input_path=ORDER_ITEMS_BRONZE_INPUT_PATH,
    output_table=ORDER_ITEMS_BRONZE_TABLE,
)
