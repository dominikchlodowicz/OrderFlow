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

from orderflow.config.constants import (
    CUSTOMERS_BRONZE_TABLE,
    CUSTOMERS_SILVER_TABLE,
)
from orderflow.silver.customers import run_customers_silver_tables

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver customers transformation",
    runner=run_customers_silver_tables,
    spark=spark,
    input_table=CUSTOMERS_BRONZE_TABLE,
    output_table=CUSTOMERS_SILVER_TABLE,
)
