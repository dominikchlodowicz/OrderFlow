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

from orderflow.bronze.payments import run_payments_bronze_table
from orderflow.config.constants import PAYMENTS_BRONZE_INPUT_PATH, PAYMENTS_BRONZE_TABLE

# COMMAND ----------

run_databricks_volume_to_table_step(
    step_name="Bronze payments ingestion",
    runner=run_payments_bronze_table,
    spark=spark,
    input_path=PAYMENTS_BRONZE_INPUT_PATH,
    output_table=PAYMENTS_BRONZE_TABLE,
)
