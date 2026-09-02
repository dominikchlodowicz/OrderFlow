# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.config.constants import ORDERS_BRONZE_TABLE, ORDERS_SILVER_TABLE
from orderflow.databricks.runtime import (
    run_databricks_table_to_table_step,
)
from orderflow.silver.orders import run_orders_silver_tables

spark: Any

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver orders transformation",
    runner=run_orders_silver_tables,
    spark=spark,  # noqa: F821
    input_table=ORDERS_BRONZE_TABLE,
    output_table=ORDERS_SILVER_TABLE,
)
