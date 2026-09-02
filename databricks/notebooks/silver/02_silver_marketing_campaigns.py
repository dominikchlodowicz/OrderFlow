# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.config.constants import (
    MARKETING_CAMPAIGNS_BRONZE_TABLE,
    MARKETING_CAMPAIGNS_SILVER_TABLE,
)
from orderflow.databricks.runtime import (
    run_databricks_table_to_table_step,
)
from orderflow.silver.marketing_campaigns import run_marketing_campaigns_silver_tables

spark: Any

# COMMAND ----------

run_databricks_table_to_table_step(
    step_name="Silver marketing campaigns transformation",
    runner=run_marketing_campaigns_silver_tables,
    spark=spark,  # noqa: F821
    input_table=MARKETING_CAMPAIGNS_BRONZE_TABLE,
    output_table=MARKETING_CAMPAIGNS_SILVER_TABLE,
)
