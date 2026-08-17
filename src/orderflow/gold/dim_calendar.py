from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from orderflow.common.delta import (
    read_delta,
    read_delta_table,
    write_delta,
    write_delta_table,
)
from orderflow.common.validation import validate_required_columns

DIM_CALENDAR_REQUIRED_COLUMNS = [
    "date_day",
    "year",
    "quarter",
    "month",
    "day_of_month",
    "day_of_week",
    "day_name",
    "week_of_year",
    "is_weekend",
    "is_polish_public_holiday",
    "holiday_name",
]


def transform_dim_calendar(
    silver_calendar_df: DataFrame,
) -> DataFrame:
    """
    Converts the cleaned Silver calendar entity into an analytical
    Gold calendar dimension.

    Gold intentionally excludes Silver lineage such as:
        _source_file_name
        _bronze_ingested_at
        _silver_processed_at
    """
    validate_required_columns(
        silver_calendar_df,
        DIM_CALENDAR_REQUIRED_COLUMNS,
        dataset_name="Silver calendar",
    )

    dim_calendar_df = silver_calendar_df.select(
        F.date_format(
            F.col("date_day"),
            "yyyyMMdd",
        )
        .cast("int")
        .alias("date_key"),
        F.col("date_day"),
        F.col("year"),
        F.col("quarter"),
        F.col("month"),
        F.date_format(
            F.col("date_day"),
            "MMMM",
        ).alias("month_name"),
        F.col("day_of_month"),
        F.col("day_of_week"),
        F.col("day_name"),
        F.col("week_of_year"),
        F.col("is_weekend"),
        F.col("is_polish_public_holiday"),
        F.col("holiday_name"),
    )

    validate_dim_calendar(dim_calendar_df)

    return dim_calendar_df.withColumn(
        "_gold_processed_at",
        F.current_timestamp(),
    )


def validate_dim_calendar(
    df: DataFrame,
) -> None:
    null_key_count = df.filter(F.col("date_key").isNull() | F.col("date_day").isNull()).count()

    if null_key_count > 0:
        raise ValueError(
            "dim_calendar validation failed: " f"{null_key_count} rows have null keys."
        )

    duplicate_key_count = df.groupBy("date_key").count().filter(F.col("count") > 1).count()

    if duplicate_key_count > 0:
        raise ValueError(
            "dim_calendar validation failed: "
            f"{duplicate_key_count} duplicate date_key values found."
        )


def write_dim_calendar(
    dim_calendar_df: DataFrame,
    output_path: str | Path,
) -> None:
    write_delta(
        df=dim_calendar_df,
        path=output_path,
        mode="overwrite",
        overwrite_schema=True,
    )


def write_dim_calendar_table(
    dim_calendar_df: DataFrame,
    output_table: str,
) -> None:
    write_delta_table(
        df=dim_calendar_df,
        table_name=output_table,
        mode="overwrite",
    )


def run_dim_calendar(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    silver_calendar_df = read_delta(
        spark=spark,
        path=input_path,
    )

    dim_calendar_df = transform_dim_calendar(silver_calendar_df)

    write_dim_calendar(
        dim_calendar_df=dim_calendar_df,
        output_path=output_path,
    )


def run_dim_calendar_tables(
    spark: SparkSession,
    input_table: str,
    output_table: str,
) -> None:
    silver_calendar_df = read_delta_table(
        spark=spark,
        table_name=input_table,
    )

    dim_calendar_df = transform_dim_calendar(silver_calendar_df)

    write_dim_calendar_table(
        dim_calendar_df=dim_calendar_df,
        output_table=output_table,
    )
