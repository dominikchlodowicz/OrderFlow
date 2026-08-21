from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from orderflow.common.validation import validate_required_columns
from orderflow.silver.common import (
    SILVER_LINEAGE_INPUT_COLUMNS,
    deduplicate_latest,
    normalize_blank_to_null,
    normalize_upper,
    run_silver_pipeline,
    run_silver_table_pipeline,
    silver_lineage_columns,
    source_load_date_for_deduplication,
    try_cast_column,
    validate_required_values,
    validate_rule,
    validate_unique_key,
)

DECIMAL_TYPE = "decimal(18,2)"

EXCHANGE_RATES_REQUIRED_COLUMNS = [
    "rate_date",
    "currency",
    "rate_to_pln",
    "source",
    "load_date",
    "loaded_at",
    *SILVER_LINEAGE_INPUT_COLUMNS,
]


def transform_exchange_rates_silver(bronze_df: DataFrame) -> DataFrame:
    validate_required_columns(
        bronze_df,
        EXCHANGE_RATES_REQUIRED_COLUMNS,
        dataset_name="Bronze exchange_rates",
    )

    silver_df = bronze_df.select(
        try_cast_column("rate_date", "date").alias("rate_date"),
        normalize_upper("currency").alias("currency"),
        try_cast_column("rate_to_pln", DECIMAL_TYPE).alias("rate_to_pln"),
        normalize_blank_to_null("source").alias("source"),
        try_cast_column("load_date", "date").alias("load_date"),
        try_cast_column("loaded_at", "timestamp").alias("loaded_at"),
        *silver_lineage_columns(),
        source_load_date_for_deduplication(),
    )

    silver_df = deduplicate_latest(
        silver_df,
        key_columns=["rate_date", "currency", "source"],
        order_columns=["loaded_at", "load_date"],
    )

    validate_exchange_rates_silver(silver_df)
    return silver_df


def validate_exchange_rates_silver(df: DataFrame) -> None:
    dataset_name = "Exchange rates"
    validate_required_values(
        df,
        required_columns=[
            "currency",
            "rate_to_pln",
            "load_date",
            "loaded_at",
            "_source_file_name",
            "_source_file_path",
            "_ingestion_run_id",
            "_bronze_ingested_at",
            "_raw_record_hash",
            "_silver_processed_at",
        ],
        dataset_name=dataset_name,
    )
    validate_rule(
        df,
        invalid_when=F.col("rate_to_pln") < 0,
        dataset_name=dataset_name,
        rule_description="have negative exchange rates",
    )
    validate_unique_key(
        df,
        key_columns=["rate_date", "currency", "source"],
        dataset_name=dataset_name,
    )


def run_exchange_rates_silver(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    run_silver_pipeline(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
        transform=transform_exchange_rates_silver,
    )


def run_exchange_rates_silver_tables(
    spark: SparkSession,
    input_table: str,
    output_table: str,
) -> None:
    run_silver_table_pipeline(
        spark=spark,
        input_table=input_table,
        output_table=output_table,
        transform=transform_exchange_rates_silver,
    )
