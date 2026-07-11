from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


CALENDAR_REQUIRED_COLUMNS = [
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
    "load_date",
    "loaded_at",
]


def validate_required_columns(df: DataFrame, required_columns: list[str]) -> None:
    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required calendar columns: {missing_columns}"
        )


def normalize_blank_to_null(column_name: str) -> F.Column:
    return F.when(
        F.trim(F.col(column_name)) == "",
        F.lit(None),
    ).otherwise(F.trim(F.col(column_name)))


def try_cast_column(
    column_name: str,
    target_type: str,
) -> F.Column:
    return F.expr(f"try_cast(`{column_name}` as {target_type})")


def transform_calendar_silver(bronze_df: DataFrame) -> DataFrame:
    validate_required_columns(
        bronze_df,
        CALENDAR_REQUIRED_COLUMNS,
    )

    silver_df = (
        bronze_df
        .select(
            try_cast_column("date_day", "date").alias("date_day"),
            try_cast_column("year", "int").alias("year"),
            try_cast_column("quarter", "int").alias("quarter"),
            try_cast_column("month", "int").alias("month"),
            try_cast_column("day_of_month", "int").alias("day_of_month"),
            try_cast_column("day_of_week", "int").alias("day_of_week"),
            normalize_blank_to_null("day_name").alias("day_name"),
            try_cast_column("week_of_year", "int").alias("week_of_year"),
            try_cast_column("is_weekend", "boolean").alias("is_weekend"),
            try_cast_column(
                "is_polish_public_holiday",
                "boolean",
            ).alias("is_polish_public_holiday"),
            normalize_blank_to_null("holiday_name").alias("holiday_name"),
            try_cast_column("load_date", "date").alias("source_load_date"),
            try_cast_column("loaded_at", "timestamp").alias("source_loaded_at"),
        )
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    latest_row_per_date = Window.partitionBy("date_day").orderBy(
        F.col("source_loaded_at").desc_nulls_last(),
        F.col("source_load_date").desc_nulls_last(),
    )

    silver_df = (
        silver_df
        .withColumn("_row_number", F.row_number().over(latest_row_per_date))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    validate_calendar_silver(silver_df)

    return silver_df


def validate_calendar_silver(df: DataFrame) -> None:
    invalid_required_rows = df.filter(
        F.col("date_day").isNull()
        | F.col("year").isNull()
        | F.col("quarter").isNull()
        | F.col("month").isNull()
        | F.col("day_of_month").isNull()
        | F.col("day_of_week").isNull()
        | F.col("day_name").isNull()
        | F.col("week_of_year").isNull()
        | F.col("is_weekend").isNull()
        | F.col("is_polish_public_holiday").isNull()
        | F.col("source_load_date").isNull()
        | F.col("source_loaded_at").isNull()
    ).count()

    if invalid_required_rows > 0:
        raise ValueError(
            f"Calendar Silver validation failed: "
            f"{invalid_required_rows} rows have null required fields."
        )

    invalid_domain_rows = df.filter(
        ~F.col("quarter").between(1, 4)
        | ~F.col("month").between(1, 12)
        | ~F.col("day_of_month").between(1, 31)
        | ~F.col("day_of_week").between(1, 7)
        | ~F.col("week_of_year").between(1, 53)
    ).count()

    if invalid_domain_rows > 0:
        raise ValueError(
            f"Calendar Silver validation failed: "
            f"{invalid_domain_rows} rows have invalid calendar values."
        )

    duplicate_date_rows = (
        df.groupBy("date_day")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_date_rows > 0:
        raise ValueError(
            f"Calendar Silver validation failed: "
            f"{duplicate_date_rows} duplicate date_day values found."
        )


def write_calendar_silver(
    silver_df: DataFrame,
    output_path: str | Path,
) -> None:
    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(output_path))
    )


def run_calendar_silver(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    bronze_df = spark.read.format("delta").load(str(input_path))

    silver_df = transform_calendar_silver(bronze_df)

    write_calendar_silver(
        silver_df=silver_df,
        output_path=output_path,
    )