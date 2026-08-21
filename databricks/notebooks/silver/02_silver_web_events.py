# Databricks notebook source

# COMMAND ----------

from typing import Any

dbutils: Any
spark: Any

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

bootstrap_databricks_notebook(dbutils=dbutils, spark=spark)

# COMMAND ----------

from orderflow.config.constants import WEB_EVENTS_BRONZE_TABLE, WEB_EVENTS_SILVER_TABLE
from orderflow.silver.web_events import run_web_events_silver_tables

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver web events transformation",
    runner=run_web_events_silver_tables,
    spark=spark,
    input_table=WEB_EVENTS_BRONZE_TABLE,
    output_table=WEB_EVENTS_SILVER_TABLE,
)
