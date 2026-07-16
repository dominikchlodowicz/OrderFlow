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

from orderflow.config.constants import (
    DIM_CALENDAR_INPUT_PATH,
    DIM_CALENDAR_OUTPUT_PATH,
)
from orderflow.gold.dim_calendar import run_dim_calendar

# COMMAND ----------

run_databricks_pipeline_step(
    step_name="Gold dim_calendar transformation",
    runner=run_dim_calendar,
    spark=spark,
    input_path=DIM_CALENDAR_INPUT_PATH,
    output_path=DIM_CALENDAR_OUTPUT_PATH,
)