# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.config.constants import CALENDAR_SILVER_TABLE
from orderflow.databricks.runtime import verify_catalog_table

spark: Any

# COMMAND ----------

verify_catalog_table(
    spark=spark,  # noqa: F821
    table_name=CALENDAR_SILVER_TABLE,
    order_by="date_day",
)
