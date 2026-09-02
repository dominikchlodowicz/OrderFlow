# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.config.constants import (
    CALENDAR_GOLD_TABLE,
    CALENDAR_SILVER_TABLE,
)
from orderflow.databricks.runtime import (
    run_databricks_table_to_table_step,
)
from orderflow.gold.dim_calendar import run_dim_calendar_tables

spark: Any

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Gold dim_calendar transformation",
    runner=run_dim_calendar_tables,
    spark=spark,  # noqa: F821
    input_table=CALENDAR_SILVER_TABLE,
    output_table=CALENDAR_GOLD_TABLE,
)
