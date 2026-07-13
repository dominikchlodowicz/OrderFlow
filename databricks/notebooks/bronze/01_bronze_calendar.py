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

from orderflow.bronze.calendar import run_calendar_bronze
from orderflow.config.constants import (
    CALENDAR_BRONZE_INPUT_PATH,
    CALENDAR_BRONZE_OUTPUT_PATH,
)

# COMMAND ----------

run_databricks_pipeline_step(
    step_name="Bronze calendar ingestion",
    runner=run_calendar_bronze,
    spark=spark,
    input_path=CALENDAR_BRONZE_INPUT_PATH,
    output_path=CALENDAR_BRONZE_OUTPUT_PATH,
)
