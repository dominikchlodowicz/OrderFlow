# Databricks notebook source

# COMMAND ----------

import re
from datetime import date
from typing import Any

from orderflow.quality import ORDERFLOW_GOLD_TABLES, build_orderflow_quality_suite

spark: Any
dbutils: Any

# COMMAND ----------

catalog = dbutils.widgets.get("catalog").strip()  # noqa: F821
environment = dbutils.widgets.get("environment").strip()  # noqa: F821
batch_id = dbutils.widgets.get("batch_id").strip()  # noqa: F821

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError(f"Invalid Unity Catalog identifier: {catalog!r}")
if not environment:
    raise ValueError("The environment job parameter must not be blank")
try:
    date.fromisoformat(batch_id)
except ValueError as error:
    raise ValueError("The batch_id job parameter must use YYYY-MM-DD format") from error

print("Running Gold data-quality checks")
print(f"Catalog: {catalog}")
print(f"Environment: {environment}")
print(f"Batch ID: {batch_id}")

# COMMAND ----------

gold_tables = {}
for logical_name in ORDERFLOW_GOLD_TABLES:
    qualified_name = f"{catalog}.gold.{logical_name}"
    if spark.catalog.tableExists(qualified_name):  # noqa: F821
        gold_tables[logical_name] = spark.table(qualified_name)  # noqa: F821
    else:
        print(f"Missing expected table: {qualified_name}")

quality_suite = build_orderflow_quality_suite(gold_tables)
quality_report = quality_suite.run()

# COMMAND ----------

print(quality_report.format_summary())

# Raise only after the full report has been displayed. This fails the notebook,
# its Lakeflow task, the complete job run, and the waiting Airflow operator.
quality_report.raise_for_failures()

print("Gold data-quality checks completed successfully.")
