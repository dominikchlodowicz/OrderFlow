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

from orderflow.config.constants import (
    CALENDAR_GOLD_TABLE,
    CAMPAIGNS_GOLD_TABLE,
    CUSTOMERS_GOLD_TABLE,
    PRODUCTS_GOLD_TABLE,
    WEB_EVENTS_GOLD_TABLE,
    WEB_EVENTS_SILVER_TABLE,
)
from orderflow.gold.fct_web_events import run_fct_web_events_tables

# COMMAND ----------

run_databricks_multi_table_to_table_step(
    step_name="Gold fct_web_events transformation",
    runner=run_fct_web_events_tables,
    spark=spark,
    input_tables={
        "web_events_input_table": WEB_EVENTS_SILVER_TABLE,
        "customers_input_table": CUSTOMERS_GOLD_TABLE,
        "products_input_table": PRODUCTS_GOLD_TABLE,
        "campaigns_input_table": CAMPAIGNS_GOLD_TABLE,
        "calendar_input_table": CALENDAR_GOLD_TABLE,
    },
    output_table=WEB_EVENTS_GOLD_TABLE,
)
