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

from orderflow.config.constants import REFUNDS_BRONZE_TABLE, REFUNDS_SILVER_TABLE
from orderflow.silver.refunds import run_refunds_silver_tables

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver refunds transformation",
    runner=run_refunds_silver_tables,
    spark=spark,
    input_table=REFUNDS_BRONZE_TABLE,
    output_table=REFUNDS_SILVER_TABLE,
)
