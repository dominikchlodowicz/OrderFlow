"""Repository-aware paths used by local Spark entry points."""

import os
from pathlib import Path


def _configured_path(environment_variable: str, default: Path) -> Path:
    configured_path = os.environ.get(environment_variable)
    if configured_path is None:
        return default.resolve()
    return Path(configured_path).expanduser().resolve()


PROJECT_ROOT = _configured_path(
    "ORDERFLOW_PROJECT_ROOT",
    Path(__file__).resolve().parents[3],
)
DATA_ROOT = _configured_path(
    "ORDERFLOW_DATA_ROOT",
    PROJECT_ROOT / "data",
)


def data_path(*parts: str) -> Path:
    """Return an absolute path below the root-level local data directory."""
    return DATA_ROOT.joinpath(*parts)


__all__ = ["DATA_ROOT", "PROJECT_ROOT", "data_path"]
