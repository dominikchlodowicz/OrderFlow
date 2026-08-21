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

from orderflow.config.constants import SHIPMENTS_BRONZE_TABLE, SHIPMENTS_SILVER_TABLE
from orderflow.silver.shipments import run_shipments_silver_tables

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver shipments transformation",
    runner=run_shipments_silver_tables,
    spark=spark,
    input_table=SHIPMENTS_BRONZE_TABLE,
    output_table=SHIPMENTS_SILVER_TABLE,
)
