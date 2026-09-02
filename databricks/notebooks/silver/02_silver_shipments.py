# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.config.constants import SHIPMENTS_BRONZE_TABLE, SHIPMENTS_SILVER_TABLE
from orderflow.databricks.runtime import (
    run_databricks_table_to_table_step,
)
from orderflow.silver.shipments import run_shipments_silver_tables

spark: Any

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver shipments transformation",
    runner=run_shipments_silver_tables,
    spark=spark,  # noqa: F821
    input_table=SHIPMENTS_BRONZE_TABLE,
    output_table=SHIPMENTS_SILVER_TABLE,
)
