# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.bronze.payments import run_payments_bronze_table
from orderflow.config.constants import PAYMENTS_BRONZE_INPUT_PATH, PAYMENTS_BRONZE_TABLE
from orderflow.databricks.runtime import (
    run_databricks_volume_to_table_step,
)

spark: Any

# COMMAND ----------

run_databricks_volume_to_table_step(
    step_name="Bronze payments ingestion",
    runner=run_payments_bronze_table,
    spark=spark,  # noqa: F821
    input_path=PAYMENTS_BRONZE_INPUT_PATH,
    output_table=PAYMENTS_BRONZE_TABLE,
)
