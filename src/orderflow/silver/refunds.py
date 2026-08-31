from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from orderflow.common.validation import validate_required_columns
from orderflow.silver.common import (
    SILVER_LINEAGE_INPUT_COLUMNS,
    deduplicate_latest,
    normalize_lower,
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

REFUNDS_REQUIRED_COLUMNS = [
    "refund_id",
    "order_id",
    "payment_id",
    "refund_reason",
    "refund_amount",
    "currency",
    "created_at",
    "processed_at",
    "refund_status",
    "load_date",
    "loaded_at",
    "source_event_at",
    *SILVER_LINEAGE_INPUT_COLUMNS,
]


def transform_refunds_silver(bronze_df: DataFrame) -> DataFrame:
    validate_required_columns(
        bronze_df,
        REFUNDS_REQUIRED_COLUMNS,
        dataset_name="Bronze refunds",
    )

    silver_df = bronze_df.select(
        normalize_lower("refund_id").alias("refund_id"),
        normalize_lower("order_id").alias("order_id"),
        normalize_lower("payment_id").alias("payment_id"),
        normalize_lower("refund_reason").alias("refund_reason"),
        normalize_lower("refund_status").alias("refund_status"),
        try_cast_column("refund_amount", DECIMAL_TYPE).alias("refund_amount"),
        normalize_upper("currency").alias("currency"),
        try_cast_column("created_at", "timestamp").alias("created_at"),
        try_cast_column("processed_at", "timestamp").alias("processed_at"),
        try_cast_column("load_date", "date").alias("load_date"),
        try_cast_column("loaded_at", "timestamp").alias("loaded_at"),
        try_cast_column("source_event_at", "timestamp").alias("source_event_at"),
        *silver_lineage_columns(),
        source_load_date_for_deduplication(),
    )

    silver_df = deduplicate_latest(
        silver_df,
        key_columns=["refund_id"],
        order_columns=["processed_at", "source_event_at", "loaded_at"],
    )

    validate_refunds_silver(silver_df)
    return silver_df


def validate_refunds_silver(df: DataFrame) -> None:
    dataset_name = "Refunds"
    validate_required_values(
        df,
        required_columns=[
            "refund_id",
            "order_id",
            "payment_id",
            "refund_status",
            "refund_amount",
            "currency",
            "created_at",
            "processed_at",
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
        invalid_when=(F.col("refund_amount") < 0)
        | (F.col("processed_at") < F.col("created_at")),
        dataset_name=dataset_name,
        rule_description="have invalid refund amount or chronology",
    )
    validate_unique_key(
        df,
        key_columns=["refund_id"],
        dataset_name=dataset_name,
    )


def run_refunds_silver(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    run_silver_pipeline(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
        transform=transform_refunds_silver,
    )


def run_refunds_silver_tables(
    spark: SparkSession,
    input_table: str,
    output_table: str,
) -> None:
    run_silver_table_pipeline(
        spark=spark,
        input_table=input_table,
        output_table=output_table,
        transform=transform_refunds_silver,
    )
