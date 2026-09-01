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

from orderflow.config.constants import CAMPAIGNS_GOLD_TABLE, MARKETING_CAMPAIGNS_SILVER_TABLE
from orderflow.gold.dim_campaigns import run_dim_campaigns_tables

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Gold dim_campaigns transformation",
    runner=run_dim_campaigns_tables,
    spark=spark,
    input_table=MARKETING_CAMPAIGNS_SILVER_TABLE,
    output_table=CAMPAIGNS_GOLD_TABLE,
)
