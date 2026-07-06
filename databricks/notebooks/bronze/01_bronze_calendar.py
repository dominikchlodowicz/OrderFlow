# Databricks notebook source

# COMMAND ----------
# Widgets = parameters passed by Databricks job or manually in notebook

dbutils.widgets.text("input_path", "")
dbutils.widgets.text("output_path", "")
dbutils.widgets.text("source_system", "adls_raw")

# COMMAND ----------

input_path = dbutils.widgets.get("input_path")
output_path = dbutils.widgets.get("output_path")
source_system = dbutils.widgets.get("source_system")

if not input_path:
    raise ValueError("input_path widget is required.")

if not output_path:
    raise ValueError("output_path widget is required.")

# COMMAND ----------

from orderflow.bronze.calendar import run_calendar_bronze

run_calendar_bronze(
    spark=spark,
    input_path=input_path,
    output_path=output_path,
    source_system=source_system,
)