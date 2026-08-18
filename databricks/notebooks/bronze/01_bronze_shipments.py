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

from orderflow.bronze.shipments import run_shipments_bronze_table
from orderflow.config.constants import SHIPMENTS_BRONZE_INPUT_PATH, SHIPMENTS_BRONZE_TABLE

# COMMAND ----------

run_databricks_volume_to_table_step(
    step_name="Bronze shipments ingestion",
    runner=run_shipments_bronze_table,
    spark=spark,
    input_path=SHIPMENTS_BRONZE_INPUT_PATH,
    output_table=SHIPMENTS_BRONZE_TABLE,
)
