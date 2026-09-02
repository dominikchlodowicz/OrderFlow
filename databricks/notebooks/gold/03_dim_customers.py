# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.config.constants import (
    CUSTOMERS_GOLD_TABLE,
    CUSTOMERS_SILVER_TABLE,
)
from orderflow.databricks.runtime import (
    run_databricks_table_to_table_step,
)
from orderflow.gold.dim_customers import run_dim_customers_tables

spark: Any

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Gold dim_customers transformation",
    runner=run_dim_customers_tables,
    spark=spark,  # noqa: F821
    input_table=CUSTOMERS_SILVER_TABLE,
    output_table=CUSTOMERS_GOLD_TABLE,
)
