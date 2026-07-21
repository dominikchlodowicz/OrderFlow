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
    DIM_CUSTOMERS_INPUT_PATH,
    DIM_CUSTOMERS_OUTPUT_PATH,
)
from orderflow.gold.dim_customers import run_dim_customers

# COMMAND ----------

run_databricks_pipeline_step(
    step_name="Gold dim_customers transformation",
    runner=run_dim_customers,
    spark=spark,
    input_path=DIM_CUSTOMERS_INPUT_PATH,
    output_path=DIM_CUSTOMERS_OUTPUT_PATH,
)
