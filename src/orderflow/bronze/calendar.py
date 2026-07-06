from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from orderflow.bronze.common import add_standard_bronze_metadata

CALENDAR_COLUMNS = [
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
    "load_date",
    "loaded_at",
]

CALENDAR_SCHEMA = StructType(
    [StructField(column, StringType(), True) for column in CALENDAR_COLUMNS]
)

def read_calendar_raw(spark: SparkSession, input_path: str | Path) -> DataFrame:
    return (
        spark.read
        .format("csv")
        .schema(CALENDAR_SCHEMA)
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .load(str(input_path))
    )

def run_calendar_bronze(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    raw_df = read_calendar_raw(spark, input_path)
    bronze_df = add_standard_bronze_metadata(
        raw_df,
        source_system="local_files",
        source_entity="calendar",
        ingestion_run_id="manual-local-run",
        raw_columns=CALENDAR_COLUMNS,
    )

    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("_source_load_date")
        .save(str(output_path))
    )