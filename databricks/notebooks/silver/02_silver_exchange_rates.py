# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.config.constants import (
    EXCHANGE_RATES_BRONZE_TABLE,
    EXCHANGE_RATES_SILVER_TABLE,
)
from orderflow.databricks.runtime import (
    run_databricks_table_to_table_step,
)
from orderflow.silver.exchange_rates import run_exchange_rates_silver_tables

spark: Any

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver exchange rates transformation",
    runner=run_exchange_rates_silver_tables,
    spark=spark,  # noqa: F821
    input_table=EXCHANGE_RATES_BRONZE_TABLE,
    output_table=EXCHANGE_RATES_SILVER_TABLE,
)
