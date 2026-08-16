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
    CALENDAR_GOLD_TABLE,
    CALENDAR_SILVER_TABLE,
)
from orderflow.gold.dim_calendar import run_dim_calendar_tables

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Gold dim_calendar transformation",
    runner=run_dim_calendar_tables,
    spark=spark,
    input_table=CALENDAR_SILVER_TABLE,
    output_table=CALENDAR_GOLD_TABLE,
)
