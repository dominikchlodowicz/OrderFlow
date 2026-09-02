# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.config.constants import (
    CALENDAR_BRONZE_TABLE,
    CALENDAR_SILVER_TABLE,
)
from orderflow.databricks.runtime import (
    run_databricks_table_to_table_step,
)
from orderflow.silver.calendar import run_calendar_silver_tables

spark: Any

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver calendar transformation",
    runner=run_calendar_silver_tables,
    spark=spark,  # noqa: F821
    input_table=CALENDAR_BRONZE_TABLE,
    output_table=CALENDAR_SILVER_TABLE,
)
