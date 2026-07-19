# Databricks notebook source

# COMMAND ----------

from typing import Any

dbutils: Any
spark: Any

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

bootstrap_databricks_notebook(
    dbutils=dbutils,
    spark=spark,
)

# COMMAND ----------

from orderflow.silver.customers import run_customers_silver
from orderflow.config.constants import (
    CUSTOMERS_SILVER_INPUT_PATH,
    CUSTOMERS_SILVER_OUTPUT_PATH,
)

# COMMAND ----------

run_databricks_pipeline_step(
    step_name="Silver customers transformation",
    runner=run_customers_silver,
    spark=spark,
    input_path=CUSTOMERS_SILVER_INPUT_PATH,
    output_path=CUSTOMERS_SILVER_OUTPUT_PATH,
)