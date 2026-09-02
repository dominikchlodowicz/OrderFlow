# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.config.constants import PRODUCTS_GOLD_TABLE, PRODUCTS_SILVER_TABLE
from orderflow.databricks.runtime import (
    run_databricks_table_to_table_step,
)
from orderflow.gold.dim_products import run_dim_products_tables

spark: Any

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Gold dim_products transformation",
    runner=run_dim_products_tables,
    spark=spark,  # noqa: F821
    input_table=PRODUCTS_SILVER_TABLE,
    output_table=PRODUCTS_GOLD_TABLE,
)
