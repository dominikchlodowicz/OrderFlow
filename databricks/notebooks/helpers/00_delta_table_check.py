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

from orderflow.config.constants import CALENDAR_SILVER_TABLE

verify_catalog_table(
    spark=spark,
    table_name=CALENDAR_SILVER_TABLE,
    order_by="date_day",
)
