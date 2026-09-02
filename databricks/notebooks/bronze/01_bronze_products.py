# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.bronze.products import run_products_bronze_table
from orderflow.config.constants import PRODUCTS_BRONZE_INPUT_PATH, PRODUCTS_BRONZE_TABLE
from orderflow.databricks.runtime import (
    run_databricks_volume_to_table_step,
)

spark: Any

# COMMAND ----------

run_databricks_volume_to_table_step(
    step_name="Bronze products ingestion",
    runner=run_products_bronze_table,
    spark=spark,  # noqa: F821
    input_path=PRODUCTS_BRONZE_INPUT_PATH,
    output_table=PRODUCTS_BRONZE_TABLE,
)
