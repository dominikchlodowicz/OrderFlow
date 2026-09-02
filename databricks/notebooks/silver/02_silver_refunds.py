# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.config.constants import REFUNDS_BRONZE_TABLE, REFUNDS_SILVER_TABLE
from orderflow.databricks.runtime import (
    run_databricks_table_to_table_step,
)
from orderflow.silver.refunds import run_refunds_silver_tables

spark: Any

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver refunds transformation",
    runner=run_refunds_silver_tables,
    spark=spark,  # noqa: F821
    input_table=REFUNDS_BRONZE_TABLE,
    output_table=REFUNDS_SILVER_TABLE,
)
