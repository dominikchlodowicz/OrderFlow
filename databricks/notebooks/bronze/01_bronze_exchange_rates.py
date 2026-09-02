# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.bronze.exchange_rates import run_exchange_rates_bronze_table
from orderflow.config.constants import (
    EXCHANGE_RATES_BRONZE_INPUT_PATH,
    EXCHANGE_RATES_BRONZE_TABLE,
)
from orderflow.databricks.runtime import (
    run_databricks_volume_to_table_step,
)

spark: Any

# COMMAND ----------

run_databricks_volume_to_table_step(
    step_name="Bronze exchange rates ingestion",
    runner=run_exchange_rates_bronze_table,
    spark=spark,  # noqa: F821
    input_path=EXCHANGE_RATES_BRONZE_INPUT_PATH,
    output_table=EXCHANGE_RATES_BRONZE_TABLE,
)
