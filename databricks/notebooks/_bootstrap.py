# Databricks notebook source

# COMMAND ----------

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any


def add_project_src_to_pythonpath(dbutils: Any) -> str:
    """
    Adds the repo's src/ directory to Python path inside a Databricks notebook.

    This is needed for development/workflow runs where the project package
    is not installed as a wheel yet.
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
    package_path = Path(src_path) / "orderflow"

    if not package_path.exists():
        raise RuntimeError(
            f"Could not find orderflow package at expected path: {package_path}"
        )

    if src_path not in sys.path:
        sys.path.insert(0, src_path)

    print(f"Notebook path: {notebook_path}")
    print(f"Added to Python path: {src_path}")

    return src_path


def configure_adls_shared_key_access(
    *,
    spark: Any,
    dbutils: Any,
    account_name: str | None = None,
    secret_scope: str | None = None,
    secret_key: str | None = None,
) -> None:
    """
    Configures Spark to access ADLS Gen2 using a storage account key stored
    in Databricks Secrets.

    Imports project constants inside the function because src/ must be added
    to sys.path first.
    """
    from orderflow.config.constants import (
        SECRET_SCOPE,
        STORAGE_ACCOUNT_KEY_SECRET_NAME,
        STORAGE_ACCOUNT_NAME,
    )

    resolved_account_name = account_name or STORAGE_ACCOUNT_NAME
    resolved_secret_scope = secret_scope or SECRET_SCOPE
    resolved_secret_key = secret_key or STORAGE_ACCOUNT_KEY_SECRET_NAME

    spark.conf.set(
        f"fs.azure.account.auth.type.{resolved_account_name}.dfs.core.windows.net",
        "SharedKey",
    )

    spark.conf.set(
        f"fs.azure.account.key.{resolved_account_name}.dfs.core.windows.net",
        dbutils.secrets.get(
            scope=resolved_secret_scope,
            key=resolved_secret_key,
        ).strip(),
    )

    print(f"Configured ADLS access for storage account: {resolved_account_name}")


def bootstrap_databricks_notebook(
    *,
    dbutils: Any,
    spark: Any,
) -> None:
    """
    Standard setup for project Databricks notebooks.
    """
    add_project_src_to_pythonpath(dbutils)

    configure_adls_shared_key_access(
        spark=spark,
        dbutils=dbutils,
    )


def run_databricks_pipeline_step(
    *,
    step_name: str,
    runner: Callable[..., None],
    spark: Any,
    input_path: str,
    output_path: str,
) -> None:
    """
    Runs a standard one-input / one-output pipeline step.

    Example:
        Bronze calendar:
            raw path -> bronze path

        Silver calendar:
            bronze path -> silver path

        Gold dim_calendar:
            silver path -> gold path
    """
    print(f"Running {step_name}")
    print(f"Input path:  {input_path}")
    print(f"Output path: {output_path}")

    runner(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
    )

    print(f"{step_name} completed successfully.")


def build_adls2_path(
    path_inside_container: str,
    *,
    account_name: str | None = None,
    container_name: str | None = None,
) -> str:
    """
    Builds a full abfss:// ADLS Gen2 path.

    Example:
        build_adls2_path("/silver/delta/calendar")

    Returns:
        abfss://lakehouse@storderflowdevfrc1.dfs.core.windows.net/silver/delta/calendar
    """
    from orderflow.config.constants import (
        LAKEHOUSE_CONTAINER,
        STORAGE_ACCOUNT_NAME,
    )

    resolved_account_name = account_name or STORAGE_ACCOUNT_NAME
    resolved_container_name = container_name or LAKEHOUSE_CONTAINER

    normalized_path = path_inside_container.lstrip("/")

    return (
        f"abfss://{resolved_container_name}"
        f"@{resolved_account_name}.dfs.core.windows.net"
        f"/{normalized_path}"
    )


def verify_delta_table(
    *,
    spark: Any,
    path: str,
    order_by: str | None = None,
) -> None:
    """
    Quick Databricks helper for verifying a Delta table path.
    """
    df = spark.read.format("delta").load(path)

    print(f"Path: {path}")
    print(f"Row count: {df.count()}")

    df.printSchema()

    if order_by is None:
        display(df)
    else:
        display(df.orderBy(order_by))