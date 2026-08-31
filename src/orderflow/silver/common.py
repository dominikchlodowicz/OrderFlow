"""Shared transformation, validation, and orchestration helpers for Silver."""

from collections.abc import Callable, Sequence
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from orderflow.common.delta import (
    read_delta,
    read_delta_table,
    write_delta,
    write_delta_table,
)

TransformFunction = Callable[[DataFrame], DataFrame]

SILVER_LINEAGE_INPUT_COLUMNS = [
    "_source_file_name",
    "_source_file_path",
    "_source_load_date",
    "_ingestion_run_id",
    "_ingested_at",
    "_raw_record_hash",
]

SILVER_LINEAGE_OUTPUT_COLUMNS = [
    "_source_file_name",
    "_source_file_path",
    "_ingestion_run_id",
    "_bronze_ingested_at",
    "_raw_record_hash",
    "_silver_processed_at",
]


def normalize_blank_to_null(column_name: str) -> Column:
    """Trim a raw string and standardize blank values to null."""
    normalized = F.trim(F.col(column_name))
    return F.when(normalized == "", F.lit(None)).otherwise(normalized)


def normalize_lower(column_name: str) -> Column:
    return F.lower(normalize_blank_to_null(column_name))


def normalize_upper(column_name: str) -> Column:
    return F.upper(normalize_blank_to_null(column_name))


def try_cast_column(column_name: str, target_type: str) -> Column:
    """Cast malformed raw strings to null so contract validation can reject them."""
    return F.expr(f"try_cast(`{column_name}` as {target_type})")


def try_cast_blank_as_default(
    column_name: str,
    target_type: str,
    default_value: object,
) -> Column:
    """Default only null/blank raw values; malformed non-blank values remain null."""
    raw_value = normalize_blank_to_null(column_name)
    return F.when(
        raw_value.isNull(),
        F.lit(default_value).cast(target_type),
    ).otherwise(try_cast_column(column_name, target_type))


def silver_lineage_columns() -> list[Column]:
    """Project Bronze lineage into the exact shared Silver contract order."""
    return [
        F.col("_source_file_name"),
        F.col("_source_file_path"),
        F.col("_ingestion_run_id"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.col("_raw_record_hash"),
        F.current_timestamp().alias("_silver_processed_at"),
    ]


def source_load_date_for_deduplication() -> Column:
    return F.col("_source_load_date").alias("_dedupe_source_load_date")


def deduplicate_latest(
    df: DataFrame,
    *,
    key_columns: Sequence[str],
    order_columns: Sequence[str],
) -> DataFrame:
    """Keep one deterministic latest record for a Silver business key."""
    latest_record = Window.partitionBy(*key_columns).orderBy(
        *[F.col(column_name).desc_nulls_last() for column_name in order_columns],
        F.col("_dedupe_source_load_date").desc_nulls_last(),
        F.col("_bronze_ingested_at").desc_nulls_last(),
        F.col("_raw_record_hash").desc_nulls_last(),
    )

    return (
        df.withColumn("_row_number", F.row_number().over(latest_record))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number", "_dedupe_source_load_date")
    )


def validate_required_values(
    df: DataFrame,
    *,
    required_columns: Sequence[str],
    dataset_name: str,
) -> None:
    invalid_rows = df.filter(
        F.greatest(
            *[F.col(column_name).isNull().cast("int") for column_name in required_columns]
        )
        == 1
    ).count()

    if invalid_rows > 0:
        raise ValueError(
            f"{dataset_name} Silver validation failed: "
            f"{invalid_rows} rows have null required fields."
        )


def validate_unique_key(
    df: DataFrame,
    *,
    key_columns: Sequence[str],
    dataset_name: str,
) -> None:
    duplicate_keys = df.groupBy(*key_columns).count().filter(F.col("count") > 1).count()

    if duplicate_keys > 0:
        key_name = ", ".join(key_columns)
        raise ValueError(
            f"{dataset_name} Silver validation failed: "
            f"{duplicate_keys} duplicate {key_name} values found."
        )


def validate_rule(
    df: DataFrame,
    *,
    invalid_when: Column,
    dataset_name: str,
    rule_description: str,
) -> None:
    invalid_rows = df.filter(invalid_when).count()

    if invalid_rows > 0:
        raise ValueError(
            f"{dataset_name} Silver validation failed: "
            f"{invalid_rows} rows {rule_description}."
        )


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


def write_silver_table(
    silver_df: DataFrame,
    output_table: str,
) -> None:
    """Overwrite a Silver table registered in the active Spark catalog."""
    write_delta_table(
        df=silver_df,
        table_name=output_table,
        mode="overwrite",
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


def run_silver_table_pipeline(
    spark: SparkSession,
    input_table: str,
    output_table: str,
    transform: TransformFunction,
) -> None:
    """Transform one registered Bronze table into a registered Silver table."""
    bronze_df = read_delta_table(
        spark=spark,
        table_name=input_table,
    )

    silver_df = transform(bronze_df)

    write_silver_table(
        silver_df=silver_df,
        output_table=output_table,
    )
