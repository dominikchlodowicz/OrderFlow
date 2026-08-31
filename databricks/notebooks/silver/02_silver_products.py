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

from orderflow.config.constants import PRODUCTS_BRONZE_TABLE, PRODUCTS_SILVER_TABLE
from orderflow.silver.products import run_products_silver_tables

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver products transformation",
    runner=run_products_silver_tables,
    spark=spark,
    input_table=PRODUCTS_BRONZE_TABLE,
    output_table=PRODUCTS_SILVER_TABLE,
)
