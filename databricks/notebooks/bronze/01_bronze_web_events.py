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

from orderflow.bronze.web_events import run_web_events_bronze_table
from orderflow.config.constants import WEB_EVENTS_BRONZE_INPUT_PATH, WEB_EVENTS_BRONZE_TABLE

# COMMAND ----------

run_databricks_volume_to_table_step(
    step_name="Bronze web events ingestion",
    runner=run_web_events_bronze_table,
    spark=spark,
    input_path=WEB_EVENTS_BRONZE_INPUT_PATH,
    output_table=WEB_EVENTS_BRONZE_TABLE,
)
