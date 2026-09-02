# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.bronze.refunds import run_refunds_bronze_table
from orderflow.config.constants import REFUNDS_BRONZE_INPUT_PATH, REFUNDS_BRONZE_TABLE
from orderflow.databricks.runtime import (
    run_databricks_volume_to_table_step,
)

spark: Any

# COMMAND ----------

run_databricks_volume_to_table_step(
    step_name="Bronze refunds ingestion",
    runner=run_refunds_bronze_table,
    spark=spark,  # noqa: F821
    input_path=REFUNDS_BRONZE_INPUT_PATH,
    output_table=REFUNDS_BRONZE_TABLE,
)
