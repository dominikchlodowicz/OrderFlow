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

from orderflow.bronze.refunds import run_refunds_bronze_table
from orderflow.config.constants import REFUNDS_BRONZE_INPUT_PATH, REFUNDS_BRONZE_TABLE

# COMMAND ----------

run_databricks_volume_to_table_step(
    step_name="Bronze refunds ingestion",
    runner=run_refunds_bronze_table,
    spark=spark,
    input_path=REFUNDS_BRONZE_INPUT_PATH,
    output_table=REFUNDS_BRONZE_TABLE,
)
