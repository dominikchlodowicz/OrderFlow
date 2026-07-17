from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def read_delta(
    spark: SparkSession,
    path: str | Path,
) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(str(path))
    )


def write_delta(
    df: DataFrame,
    path: str | Path,
    *,
    mode: str = "overwrite",
    overwrite_schema: bool = True,
    partition_by: Sequence[str] | None = None,
) -> None:
    writer = (
        df.write
        .format("delta")
        .mode(mode)
    )

    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(str(path))