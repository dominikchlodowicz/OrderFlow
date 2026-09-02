# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.bronze.shipments import run_shipments_bronze_table
from orderflow.config.constants import SHIPMENTS_BRONZE_INPUT_PATH, SHIPMENTS_BRONZE_TABLE
from orderflow.databricks.runtime import (
    run_databricks_volume_to_table_step,
)

spark: Any

# COMMAND ----------

run_databricks_volume_to_table_step(
    step_name="Bronze shipments ingestion",
    runner=run_shipments_bronze_table,
    spark=spark,  # noqa: F821
    input_path=SHIPMENTS_BRONZE_INPUT_PATH,
    output_table=SHIPMENTS_BRONZE_TABLE,
)
