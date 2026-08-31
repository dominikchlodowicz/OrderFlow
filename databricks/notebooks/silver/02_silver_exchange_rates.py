# Databricks notebook source

# COMMAND ----------

from typing import Any

dbutils: Any
spark: Any

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

bootstrap_databricks_notebook(dbutils=dbutils, spark=spark)

# COMMAND ----------

from orderflow.config.constants import (
    EXCHANGE_RATES_BRONZE_TABLE,
    EXCHANGE_RATES_SILVER_TABLE,
)
from orderflow.silver.exchange_rates import run_exchange_rates_silver_tables

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver exchange rates transformation",
    runner=run_exchange_rates_silver_tables,
    spark=spark,
    input_table=EXCHANGE_RATES_BRONZE_TABLE,
    output_table=EXCHANGE_RATES_SILVER_TABLE,
)
