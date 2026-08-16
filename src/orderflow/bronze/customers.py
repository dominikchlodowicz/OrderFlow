from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from orderflow.bronze.common import add_standard_bronze_metadata
from orderflow.common.delta import write_delta, write_delta_table
from orderflow.common.validation import validate_required_columns

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
    "source_event_at"
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
        spark.read
        .format("csv")
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
) -> DataFrame:
    raw_df = read_customers_raw(
        spark=spark,
        input_path=input_path
    )

    bronze_df = add_standard_bronze_metadata(
        raw_df,
        source_system="local_files",
        source_entity="customers",
        ingestion_run_id="manual-local-run",
        raw_columns=CUSTOMERS_COLUMNS,
    )

    return bronze_df


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
    )

    write_delta_table(
        df=bronze_df,
        table_name=output_table,
        mode="overwrite",
        overwrite_schema=True,
        partition_by=["_source_load_date"],
    )
