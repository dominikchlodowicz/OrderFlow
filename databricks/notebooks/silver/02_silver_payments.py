# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.config.constants import PAYMENTS_BRONZE_TABLE, PAYMENTS_SILVER_TABLE
from orderflow.databricks.runtime import (
    run_databricks_table_to_table_step,
)
from orderflow.silver.payments import run_payments_silver_tables

spark: Any

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver payments transformation",
    runner=run_payments_silver_tables,
    spark=spark,  # noqa: F821
    input_table=PAYMENTS_BRONZE_TABLE,
    output_table=PAYMENTS_SILVER_TABLE,
)
