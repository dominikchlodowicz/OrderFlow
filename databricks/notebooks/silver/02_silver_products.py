# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.config.constants import PRODUCTS_BRONZE_TABLE, PRODUCTS_SILVER_TABLE
from orderflow.databricks.runtime import (
    run_databricks_table_to_table_step,
)
from orderflow.silver.products import run_products_silver_tables

spark: Any

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver products transformation",
    runner=run_products_silver_tables,
    spark=spark,  # noqa: F821
    input_table=PRODUCTS_BRONZE_TABLE,
    output_table=PRODUCTS_SILVER_TABLE,
)
