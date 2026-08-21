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

from orderflow.config.constants import (
    MARKETING_CAMPAIGNS_BRONZE_TABLE,
    MARKETING_CAMPAIGNS_SILVER_TABLE,
)
from orderflow.silver.marketing_campaigns import run_marketing_campaigns_silver_tables

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver marketing campaigns transformation",
    runner=run_marketing_campaigns_silver_tables,
    spark=spark,
    input_table=MARKETING_CAMPAIGNS_BRONZE_TABLE,
    output_table=MARKETING_CAMPAIGNS_SILVER_TABLE,
)
