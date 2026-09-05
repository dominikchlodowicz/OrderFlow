from collections.abc import Sequence

from pyspark.sql import DataFrame


def find_missing_required_columns(
    df: DataFrame,
    required_columns: Sequence[str],
) -> list[str]:
    """Return required columns that are absent from a DataFrame schema."""
    return [column_name for column_name in required_columns if column_name not in df.columns]


def validate_required_columns(
    df: DataFrame,
    required_columns: Sequence[str],
    *,
    dataset_name: str,
) -> None:
    missing_columns = find_missing_required_columns(df, required_columns)

    if missing_columns:
        raise ValueError(f"{dataset_name} is missing required columns: " f"{missing_columns}")
