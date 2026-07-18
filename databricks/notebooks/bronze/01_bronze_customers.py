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

from orderflow.bronze.customers import run_customers_bronze
from orderflow.config.constants import (
    CUSTOMERS_BRONZE_INPUT_PATH,
    CUSTOMERS_BRONZE_OUTPUT_PATH,
)

# COMMAND ----------

run_databricks_pipeline_step(
    step_name="Bronze customers ingestion",
    runner=run_customers_bronze,
    spark=spark,
    input_path=CUSTOMERS_BRONZE_INPUT_PATH,
    output_path=CUSTOMERS_BRONZE_OUTPUT_PATH,
)
