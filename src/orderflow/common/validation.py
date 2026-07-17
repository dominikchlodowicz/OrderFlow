from collections.abc import Sequence

from pyspark.sql import DataFrame


def validate_required_columns(
    df: DataFrame,
    required_columns: Sequence[str],
    *,
    dataset_name: str,
) -> None:
    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"{dataset_name} is missing required columns: "
            f"{missing_columns}"
        )