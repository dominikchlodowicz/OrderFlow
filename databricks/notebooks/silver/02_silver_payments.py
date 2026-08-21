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

from orderflow.config.constants import PAYMENTS_BRONZE_TABLE, PAYMENTS_SILVER_TABLE
from orderflow.silver.payments import run_payments_silver_tables

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver payments transformation",
    runner=run_payments_silver_tables,
    spark=spark,
    input_table=PAYMENTS_BRONZE_TABLE,
    output_table=PAYMENTS_SILVER_TABLE,
)
