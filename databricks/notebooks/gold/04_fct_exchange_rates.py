# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.config.constants import (
    CALENDAR_GOLD_TABLE,
    CURRENCY_GOLD_TABLE,
    EXCHANGE_RATES_GOLD_TABLE,
    EXCHANGE_RATES_SILVER_TABLE,
)
from orderflow.databricks.runtime import (
    run_databricks_multi_table_to_table_step,
)
from orderflow.gold.fct_exchange_rates import run_fct_exchange_rates_tables

spark: Any

# COMMAND ----------

run_databricks_multi_table_to_table_step(
    step_name="Gold fct_exchange_rates transformation",
    runner=run_fct_exchange_rates_tables,
    spark=spark,  # noqa: F821
    input_tables={
        "exchange_rates_input_table": EXCHANGE_RATES_SILVER_TABLE,
        "currency_input_table": CURRENCY_GOLD_TABLE,
        "calendar_input_table": CALENDAR_GOLD_TABLE,
    },
    output_table=EXCHANGE_RATES_GOLD_TABLE,
)
