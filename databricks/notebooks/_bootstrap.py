# Databricks notebook source

# COMMAND ----------

import sys
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