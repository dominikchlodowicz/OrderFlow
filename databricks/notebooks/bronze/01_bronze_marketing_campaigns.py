# Databricks notebook source

# COMMAND ----------

from typing import Any

from orderflow.bronze.marketing_campaigns import run_marketing_campaigns_bronze_table
from orderflow.config.constants import (
    MARKETING_CAMPAIGNS_BRONZE_INPUT_PATH,
    MARKETING_CAMPAIGNS_BRONZE_TABLE,
)
from orderflow.databricks.runtime import (
    run_databricks_volume_to_table_step,
)

spark: Any

# COMMAND ----------

run_databricks_volume_to_table_step(
    step_name="Bronze marketing campaigns ingestion",
    runner=run_marketing_campaigns_bronze_table,
    spark=spark,  # noqa: F821
    input_path=MARKETING_CAMPAIGNS_BRONZE_INPUT_PATH,
    output_table=MARKETING_CAMPAIGNS_BRONZE_TABLE,
)
