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

from orderflow.config.constants import PRODUCTS_GOLD_TABLE, PRODUCTS_SILVER_TABLE
from orderflow.gold.dim_products import run_dim_products_tables

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Gold dim_products transformation",
    runner=run_dim_products_tables,
    spark=spark,
    input_table=PRODUCTS_SILVER_TABLE,
    output_table=PRODUCTS_GOLD_TABLE,
)
