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
    CALENDAR_BRONZE_TABLE,
    CALENDAR_SILVER_TABLE,
)
from orderflow.silver.calendar import run_calendar_silver_tables

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver calendar transformation",
    runner=run_calendar_silver_tables,
    spark=spark,
    input_table=CALENDAR_BRONZE_TABLE,
    output_table=CALENDAR_SILVER_TABLE,
)
