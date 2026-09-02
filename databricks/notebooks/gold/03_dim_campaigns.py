# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.config.constants import CAMPAIGNS_GOLD_TABLE, MARKETING_CAMPAIGNS_SILVER_TABLE
from orderflow.databricks.runtime import (
    run_databricks_table_to_table_step,
)
from orderflow.gold.dim_campaigns import run_dim_campaigns_tables

spark: Any

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Gold dim_campaigns transformation",
    runner=run_dim_campaigns_tables,
    spark=spark,  # noqa: F821
    input_table=MARKETING_CAMPAIGNS_SILVER_TABLE,
    output_table=CAMPAIGNS_GOLD_TABLE,
)
