from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from orderflow.bronze.common import (
    add_standard_bronze_metadata,
    select_bronze_contract_columns,
    validate_bronze_dataframe,
    write_delta_idempotent_by_load_date,
)
from orderflow.common.delta import write_delta_table
from orderflow.common.validation import validate_required_columns
from orderflow.config.constants import ADLS_SOURCE_SYSTEM

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
    [
        StructField(
            column_name,
            StringType(),
            nullable=True,
        )
        for column_name in CALENDAR_COLUMNS
    ]
)


def read_calendar_raw(
    spark: SparkSession,
    input_path: str | Path,
) -> DataFrame:
    raw_df = (
        spark.read.format("csv")
        .schema(CALENDAR_SCHEMA)
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .load(str(input_path))
    )

    validate_required_columns(
        raw_df,
        CALENDAR_COLUMNS,
        dataset_name="Raw calendar",
    )

    return raw_df


def build_calendar_bronze(
    spark: SparkSession,
    input_path: str | Path,
    *,
    source_system: str = "local_files",
    ingestion_run_id: str | None = None,
) -> DataFrame:
    raw_df = read_calendar_raw(
        spark=spark,
        input_path=input_path,
    )

    bronze_df = add_standard_bronze_metadata(
        raw_df,
        source_system=source_system,
        source_entity="calendar",
        ingestion_run_id=ingestion_run_id or uuid4().hex,
        raw_columns=CALENDAR_COLUMNS,
    )

    contract_df = select_bronze_contract_columns(
        bronze_df,
        raw_columns=CALENDAR_COLUMNS,
    )
    validate_bronze_dataframe(contract_df, source_entity="calendar")

    return contract_df


def run_calendar_bronze(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    bronze_df = build_calendar_bronze(
        spark=spark,
        input_path=input_path,
    )

    write_delta_idempotent_by_load_date(
        spark=spark,
        df=bronze_df,
        output_path=output_path,
        source_entity="calendar",
    )


def run_calendar_bronze_table(
    spark: SparkSession,
    input_path: str | Path,
    output_table: str,
) -> None:
    bronze_df = build_calendar_bronze(
        spark=spark,
        input_path=input_path,
        source_system=ADLS_SOURCE_SYSTEM,
    )

    write_delta_table(
        df=bronze_df,
        table_name=output_table,
        mode="overwrite",
    )
