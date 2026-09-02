# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.config.constants import WEB_EVENTS_BRONZE_TABLE, WEB_EVENTS_SILVER_TABLE
from orderflow.databricks.runtime import (
    run_databricks_table_to_table_step,
)
from orderflow.silver.web_events import run_web_events_silver_tables

spark: Any

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver web events transformation",
    runner=run_web_events_silver_tables,
    spark=spark,  # noqa: F821
    input_table=WEB_EVENTS_BRONZE_TABLE,
    output_table=WEB_EVENTS_SILVER_TABLE,
)
