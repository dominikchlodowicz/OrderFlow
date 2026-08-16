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
    CUSTOMERS_GOLD_TABLE,
    CUSTOMERS_SILVER_TABLE,
)
from orderflow.gold.dim_customers import run_dim_customers_tables

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Gold dim_customers transformation",
    runner=run_dim_customers_tables,
    spark=spark,
    input_table=CUSTOMERS_SILVER_TABLE,
    output_table=CUSTOMERS_GOLD_TABLE,
)
