# Databricks notebook source

# COMMAND ----------

from typing import Any

dbutils: Any
spark: Any

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

add_project_src_to_pythonpath(dbutils)

# COMMAND ----------

ACCOUNT_NAME = "storderflowdevfrc1"
SECRET_SCOPE = "orderflow"
SECRET_KEY = "storderflowdevfrc1-key"

spark.conf.set(
    f"fs.azure.account.auth.type.{ACCOUNT_NAME}.dfs.core.windows.net",
    "SharedKey",
)

spark.conf.set(
    f"fs.azure.account.key.{ACCOUNT_NAME}.dfs.core.windows.net",
    dbutils.secrets.get(
        scope=SECRET_SCOPE,
        key=SECRET_KEY,
    ).strip(),
)

# COMMAND ----------

from orderflow.silver.calendar import run_calendar_silver
from orderflow.config.constants import (
    CALENDAR_SILVER_INPUT_PATH,
    CALENDAR_SILVER_OUTPUT_PATH,
)

# COMMAND ----------

print("Running Silver calendar ingestion")
print(f"Input path:  {CALENDAR_SILVER_INPUT_PATH}")
print(f"Output path: {CALENDAR_SILVER_OUTPUT_PATH}")

run_calendar_silver(
    spark=spark,
    input_path=CALENDAR_SILVER_INPUT_PATH,
    output_path=CALENDAR_SILVER_OUTPUT_PATH,
)

print("Silver calendar transformation completed successfully.")