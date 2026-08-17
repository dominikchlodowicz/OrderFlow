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
    writer = df.write.format("delta").mode(mode)

    if overwrite_schema:
        writer = writer.option("overwriteSchema", "true")

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    return writer


def read_delta(
    spark: SparkSession,
    path: str | Path,
) -> DataFrame:
    return spark.read.format("delta").load(str(path))


def read_delta_table(
    spark: SparkSession,
    table_name: str,
) -> DataFrame:
    """Read a Delta table registered in the active Spark catalog."""
    return spark.table(table_name)


def align_dataframe_to_table_contract(
    df: DataFrame,
    table_name: str,
) -> DataFrame:
    """Validate and order a DataFrame against an existing catalog table.

    Contract tables are created by the setup SQL. Registered-table writes must
    conform to those schemas instead of replacing them.
    """
    target_schema = read_delta_table(
        spark=df.sparkSession,
        table_name=table_name,
    ).schema
    target_columns = [field.name for field in target_schema]

    missing_columns = [name for name in target_columns if name not in df.columns]
    extra_columns = [name for name in df.columns if name not in target_columns]

    if missing_columns or extra_columns:
        raise ValueError(
            f"DataFrame does not match table contract '{table_name}': "
            f"missing columns={missing_columns}, extra columns={extra_columns}."
        )

    source_fields = {field.name: field for field in df.schema}
    type_mismatches = [
        (
            target_field.name,
            source_fields[target_field.name].dataType.simpleString(),
            target_field.dataType.simpleString(),
        )
        for target_field in target_schema
        if source_fields[target_field.name].dataType != target_field.dataType
    ]

    if type_mismatches:
        mismatch_details = ", ".join(
            f"{name}: DataFrame={source_type}, table={target_type}"
            for name, source_type, target_type in type_mismatches
        )
        raise ValueError(
            f"DataFrame does not match table contract '{table_name}' types: " f"{mismatch_details}."
        )

    return df.select(*target_columns)


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
) -> None:
    """Write data into a pre-created catalog table without replacing its contract."""
    aligned_df = align_dataframe_to_table_contract(
        df,
        table_name,
    )

    aligned_df.write.mode(mode).insertInto(table_name)
