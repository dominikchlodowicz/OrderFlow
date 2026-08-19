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

from orderflow.bronze.orders import run_orders_bronze_table
from orderflow.config.constants import ORDERS_BRONZE_INPUT_PATH, ORDERS_BRONZE_TABLE

# COMMAND ----------

run_databricks_volume_to_table_step(
    step_name="Bronze orders ingestion",
    runner=run_orders_bronze_table,
    spark=spark,
    input_path=ORDERS_BRONZE_INPUT_PATH,
    output_table=ORDERS_BRONZE_TABLE,
)
