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

from orderflow.bronze.products import run_products_bronze_table
from orderflow.config.constants import PRODUCTS_BRONZE_INPUT_PATH, PRODUCTS_BRONZE_TABLE

# COMMAND ----------

run_databricks_volume_to_table_step(
    step_name="Bronze products ingestion",
    runner=run_products_bronze_table,
    spark=spark,
    input_path=PRODUCTS_BRONZE_INPUT_PATH,
    output_table=PRODUCTS_BRONZE_TABLE,
)
