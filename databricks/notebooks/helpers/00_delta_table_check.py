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

path = build_adls2_path("/silver/delta/calendar")

verify_delta_table(
    spark=spark,
    path=path,
    order_by="date_day",
)