from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from orderflow.common.validation import validate_required_columns
from orderflow.silver.common import (
    SILVER_LINEAGE_INPUT_COLUMNS,
    deduplicate_latest,
    normalize_blank_to_null,
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

DEVICE_TYPES = ["tablet", "mobile", "desktop"]
PRODUCT_EVENT_TYPES = ["product_view", "add_to_cart"]

WEB_EVENTS_REQUIRED_COLUMNS = [
    "event_id",
    "session_id",
    "customer_id",
    "anonymous_id",
    "event_type",
    "event_timestamp",
    "product_id",
    "campaign_id",
    "device_type",
    "country_code",
    "page_url",
    "load_date",
    "loaded_at",
    "source_event_at",
    *SILVER_LINEAGE_INPUT_COLUMNS,
]


def transform_web_events_silver(bronze_df: DataFrame) -> DataFrame:
    validate_required_columns(
        bronze_df,
        WEB_EVENTS_REQUIRED_COLUMNS,
        dataset_name="Bronze web_events",
    )

    silver_df = bronze_df.select(
        normalize_lower("event_id").alias("event_id"),
        normalize_lower("session_id").alias("session_id"),
        normalize_lower("customer_id").alias("customer_id"),
        normalize_lower("anonymous_id").alias("anonymous_id"),
        normalize_lower("event_type").alias("event_type"),
        try_cast_column("event_timestamp", "timestamp").alias("event_timestamp"),
        normalize_lower("product_id").alias("product_id"),
        normalize_lower("campaign_id").alias("campaign_id"),
        normalize_lower("device_type").alias("device_type"),
        normalize_upper("country_code").alias("country_code"),
        normalize_blank_to_null("page_url").alias("page_url"),
        try_cast_column("loaded_at", "timestamp").alias("loaded_at"),
        try_cast_column("source_event_at", "timestamp").alias("source_event_at"),
        *silver_lineage_columns(),
        source_load_date_for_deduplication(),
    )

    silver_df = deduplicate_latest(
        silver_df,
        key_columns=["event_id"],
        order_columns=["source_event_at", "loaded_at", "event_timestamp"],
    )

    validate_web_events_silver(silver_df)
    return silver_df


def validate_web_events_silver(df: DataFrame) -> None:
    dataset_name = "Web events"
    validate_required_values(
        df,
        required_columns=[
            "event_id",
            "session_id",
            "anonymous_id",
            "event_type",
            "device_type",
            "country_code",
            "page_url",
            "loaded_at",
            "source_event_at",
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
        invalid_when=~F.col("device_type").isin(DEVICE_TYPES),
        dataset_name=dataset_name,
        rule_description="have invalid device type values",
    )
    validate_rule(
        df,
        invalid_when=F.col("event_type").isin(PRODUCT_EVENT_TYPES)
        & F.col("product_id").isNull(),
        dataset_name=dataset_name,
        rule_description="have product events without product_id",
    )
    validate_unique_key(
        df,
        key_columns=["event_id"],
        dataset_name=dataset_name,
    )


def run_web_events_silver(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    run_silver_pipeline(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
        transform=transform_web_events_silver,
    )


def run_web_events_silver_tables(
    spark: SparkSession,
    input_table: str,
    output_table: str,
) -> None:
    run_silver_table_pipeline(
        spark=spark,
        input_table=input_table,
        output_table=output_table,
        transform=transform_web_events_silver,
    )
