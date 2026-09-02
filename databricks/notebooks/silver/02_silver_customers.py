# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.config.constants import (
    CUSTOMERS_BRONZE_TABLE,
    CUSTOMERS_SILVER_TABLE,
)
from orderflow.databricks.runtime import (
    run_databricks_table_to_table_step,
)
from orderflow.silver.customers import run_customers_silver_tables

spark: Any

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver customers transformation",
    runner=run_customers_silver_tables,
    spark=spark,  # noqa: F821
    input_table=CUSTOMERS_BRONZE_TABLE,
    output_table=CUSTOMERS_SILVER_TABLE,
)
