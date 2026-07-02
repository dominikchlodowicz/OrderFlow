import sys
from typing import Any

def add_project_src_to_pythonpath(dbutils: Any) -> None:
    """
    Adds the repo's src/ directory reference to usable Python path inside Databricks Environment.
    
    Args:
        dbutils: Databricks utilities object available in notebooks.
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
