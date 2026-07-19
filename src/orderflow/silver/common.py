"""Shared writing and orchestration helpers for Silver-layer pipelines."""

from collections.abc import Callable
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from orderflow.common.delta import read_delta, write_delta

TransformFunction = Callable[[DataFrame], DataFrame]


def write_silver(
    silver_df: DataFrame,
    output_path: str | Path,
) -> None:
    """Overwrite a Silver Delta table, including its schema.

    Args:
        silver_df: DataFrame to write.
        output_path: Destination Delta path.
    """
    write_delta(
        df=silver_df,
        path=output_path,
        mode="overwrite",
        overwrite_schema=True,
    )


def run_silver_pipeline(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
    transform: TransformFunction,
) -> None:
    """Read Bronze Delta, apply a transform, and overwrite Silver Delta.

    Args:
        spark: Active Spark session.
        input_path: Source Bronze Delta path.
        output_path: Destination Silver Delta path.
        transform: Callable that maps a Bronze DataFrame to a Silver DataFrame.
    """
    bronze_df = read_delta(
        spark=spark,
        path=input_path,
    )

    silver_df = transform(bronze_df)

    write_silver(
        silver_df=silver_df,
        output_path=output_path,
    )
