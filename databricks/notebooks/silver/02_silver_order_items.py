# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.config.constants import ORDER_ITEMS_BRONZE_TABLE, ORDER_ITEMS_SILVER_TABLE
from orderflow.databricks.runtime import (
    run_databricks_table_to_table_step,
)
from orderflow.silver.order_items import run_order_items_silver_tables

spark: Any

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver order items transformation",
    runner=run_order_items_silver_tables,
    spark=spark,  # noqa: F821
    input_table=ORDER_ITEMS_BRONZE_TABLE,
    output_table=ORDER_ITEMS_SILVER_TABLE,
)
