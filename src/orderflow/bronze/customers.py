from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from orderflow.bronze.common import (
    add_standard_bronze_metadata,
    select_bronze_contract_columns,
    validate_bronze_dataframe,
)
from orderflow.common.delta import write_delta, write_delta_table
from orderflow.common.validation import validate_required_columns
from orderflow.config.constants import ADLS_SOURCE_SYSTEM

CUSTOMERS_COLUMNS = [
    "customer_id",
    "email",
    "first_name",
    "last_name",
    "country_code",
    "city",
    "created_at",
    "updated_at",
    "customer_status",
    "marketing_consent",
    "load_date",
    "loaded_at",
    "source_event_at",
]

CUSTOMERS_SCHEMA = StructType(
    [
        StructField(
            column_name,
            StringType(),
            nullable=True,
        )
        for column_name in CUSTOMERS_COLUMNS
    ]
)


def read_customers_raw(
    spark: SparkSession,
    input_path: str | Path,
) -> DataFrame:
    raw_df = (
        spark.read.format("csv")
        .schema(CUSTOMERS_SCHEMA)
        .option("header", "true")
        .option("recursiveFileLookup", "true")
        .load(str(input_path))
    )

    validate_required_columns(
        raw_df,
        CUSTOMERS_COLUMNS,
        dataset_name="Raw customers",
    )

    return raw_df


def build_customers_bronze(
    spark: SparkSession,
    input_path: str | Path,
    *,
    source_system: str = "local_files",
    ingestion_run_id: str | None = None,
) -> DataFrame:
    raw_df = read_customers_raw(spark=spark, input_path=input_path)

    bronze_df = add_standard_bronze_metadata(
        raw_df,
        source_system=source_system,
        source_entity="customers",
        ingestion_run_id=ingestion_run_id or uuid4().hex,
        raw_columns=CUSTOMERS_COLUMNS,
    )

    contract_df = select_bronze_contract_columns(
        bronze_df,
        raw_columns=CUSTOMERS_COLUMNS,
    )
    validate_bronze_dataframe(contract_df, source_entity="customers")

    return contract_df


def run_customers_bronze(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    bronze_df = build_customers_bronze(
        spark=spark,
        input_path=input_path,
    )

    write_delta(
        df=bronze_df,
        path=output_path,
        mode="overwrite",
        overwrite_schema=True,
        partition_by=["_source_load_date"],
    )


def run_customers_bronze_table(
    spark: SparkSession,
    input_path: str | Path,
    output_table: str,
) -> None:
    bronze_df = build_customers_bronze(
        spark=spark,
        input_path=input_path,
        source_system=ADLS_SOURCE_SYSTEM,
    )

    write_delta_table(
        df=bronze_df,
        table_name=output_table,
        mode="overwrite",
    )
