from pathlib import Path

from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql import DataFrame, SparkSession

from orderflow.common.validation import validate_required_columns
from orderflow.common.delta import write_delta
from orderflow.bronze.common import add_standard_bronze_metadata


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


def run_customers_bronze(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
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

    write_delta(
        df=bronze_df,
        path=output_path,
        mode="overwrite",
        overwrite_schema=True,
        partition_by=["_source_load_date"],
    )