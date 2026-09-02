# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.bronze.customers import run_customers_bronze_table
from orderflow.config.constants import (
    CUSTOMERS_BRONZE_INPUT_PATH,
    CUSTOMERS_BRONZE_TABLE,
)
from orderflow.databricks.runtime import (
    run_databricks_volume_to_table_step,
)

spark: Any

# COMMAND ----------

run_databricks_volume_to_table_step(
    step_name="Bronze customers ingestion",
    runner=run_customers_bronze_table,
    spark=spark,  # noqa: F821
    input_path=CUSTOMERS_BRONZE_INPUT_PATH,
    output_table=CUSTOMERS_BRONZE_TABLE,
)
