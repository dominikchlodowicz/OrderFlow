# Databricks notebook source
# This notebook is a Databricks wrapper.
# Real Spark logic lives in:
# src/orderflow/spark/hello_orders.py

import sys

# COMMAND ----------

def add_project_src_to_pythonpath() -> None:
    """
    Adds the repo's src/ directory to Python path inside Databricks.

    Needed because this project uses src/ layout:

        src/orderflow/...

    Databricks can run notebooks from the repo, but Python may not automatically
    know where src/ is.
    """
    notebook_path = (
        dbutils.notebook.entry_point
        .getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )

    if "/databricks/" not in notebook_path:
        raise RuntimeError(
            f"Could not infer repo root from notebook path: {notebook_path}"
        )

    repo_workspace_path = notebook_path.split("/databricks/")[0]

    if repo_workspace_path.startswith("/Workspace/"):
        repo_filesystem_path = repo_workspace_path
    else:
        repo_filesystem_path = f"/Workspace{repo_workspace_path}"

    src_path = f"{repo_filesystem_path}/src"

    if src_path not in sys.path:
        sys.path.insert(0, src_path)

    print(f"Notebook path: {notebook_path}")
    print(f"Added to Python path: {src_path}")


add_project_src_to_pythonpath()

# COMMAND ----------

from orderflow.spark.hello_orders import run_hello_orders

# COMMAND ----------

print("Hello from Azure Databricks + PySpark")
print(f"Spark version: {spark.version}")

# COMMAND ----------

orders_df, daily_orders_df = run_hello_orders(spark)

# COMMAND ----------

display(orders_df)

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

display(daily_orders_df)