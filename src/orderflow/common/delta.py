from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def _build_delta_writer(
    df: DataFrame,
    *,
    mode: str,
    overwrite_schema: bool,
    partition_by: Sequence[str] | None,
):
    writer = (
        df.write
        .format("delta")
        .mode(mode)
    )

    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    return writer


def read_delta(
    spark: SparkSession,
    path: str | Path,
) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(str(path))
    )


def read_delta_table(
    spark: SparkSession,
    table_name: str,
) -> DataFrame:
    """Read a Delta table registered in the active Spark catalog."""
    return spark.table(table_name)


def write_delta(
    df: DataFrame,
    path: str | Path,
    *,
    mode: str = "overwrite",
    overwrite_schema: bool = True,
    partition_by: Sequence[str] | None = None,
) -> None:
    writer = _build_delta_writer(
        df,
        mode=mode,
        overwrite_schema=overwrite_schema,
        partition_by=partition_by,
    )

    writer.save(str(path))


def write_delta_table(
    df: DataFrame,
    table_name: str,
    *,
    mode: str = "overwrite",
    overwrite_schema: bool = True,
    partition_by: Sequence[str] | None = None,
) -> None:
    """Write to a Delta table registered in the active Spark catalog."""
    writer = _build_delta_writer(
        df,
        mode=mode,
        overwrite_schema=overwrite_schema,
        partition_by=partition_by,
    )

    writer.saveAsTable(table_name)
