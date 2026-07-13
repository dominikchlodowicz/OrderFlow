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

from orderflow.silver.calendar import run_calendar_silver
from orderflow.config.constants import (
    CALENDAR_SILVER_INPUT_PATH,
    CALENDAR_SILVER_OUTPUT_PATH,
)

# COMMAND ----------

run_databricks_pipeline_step(
    step_name="Silver calendar transformation",
    runner=run_calendar_silver,
    spark=spark,
    input_path=CALENDAR_SILVER_INPUT_PATH,
    output_path=CALENDAR_SILVER_OUTPUT_PATH,
)