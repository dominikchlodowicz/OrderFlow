# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.bronze.order_items import run_order_items_bronze_table
from orderflow.config.constants import ORDER_ITEMS_BRONZE_INPUT_PATH, ORDER_ITEMS_BRONZE_TABLE
from orderflow.databricks.runtime import (
    run_databricks_volume_to_table_step,
)

spark: Any

# COMMAND ----------

run_databricks_volume_to_table_step(
    step_name="Bronze order items ingestion",
    runner=run_order_items_bronze_table,
    spark=spark,  # noqa: F821
    input_path=ORDER_ITEMS_BRONZE_INPUT_PATH,
    output_table=ORDER_ITEMS_BRONZE_TABLE,
)
