# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.bronze.calendar import run_calendar_bronze_table
from orderflow.config.constants import (
    CALENDAR_BRONZE_INPUT_PATH,
    CALENDAR_BRONZE_TABLE,
)
from orderflow.databricks.runtime import (
    run_databricks_volume_to_table_step,
)

spark: Any

# COMMAND ----------

run_databricks_volume_to_table_step(
    step_name="Bronze calendar ingestion",
    runner=run_calendar_bronze_table,
    spark=spark,  # noqa: F821
    input_path=CALENDAR_BRONZE_INPUT_PATH,
    output_table=CALENDAR_BRONZE_TABLE,
)
