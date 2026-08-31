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

DECIMAL_TYPE = "decimal(18,2)"

MARKETING_CAMPAIGNS_REQUIRED_COLUMNS = [
    "campaign_id",
    "campaign_name",
    "source_channel",
    "start_date",
    "end_date",
    "budget_amount",
    "currency",
    "created_at",
    "updated_at",
    "is_active",
    "load_date",
    "loaded_at",
    "source_event_at",
    *SILVER_LINEAGE_INPUT_COLUMNS,
]


def transform_marketing_campaigns_silver(bronze_df: DataFrame) -> DataFrame:
    validate_required_columns(
        bronze_df,
        MARKETING_CAMPAIGNS_REQUIRED_COLUMNS,
        dataset_name="Bronze marketing_campaigns",
    )

    silver_df = bronze_df.select(
        normalize_lower("campaign_id").alias("campaign_id"),
        normalize_blank_to_null("campaign_name").alias("campaign_name"),
        normalize_lower("source_channel").alias("source_channel"),
        try_cast_column("start_date", "date").alias("start_date"),
        try_cast_column("end_date", "date").alias("end_date"),
        try_cast_column("budget_amount", DECIMAL_TYPE).alias("budget_amount"),
        normalize_upper("currency").alias("currency"),
        try_cast_column("is_active", "boolean").alias("is_active"),
        try_cast_column("created_at", "timestamp").alias("created_at"),
        try_cast_column("updated_at", "timestamp").alias("updated_at"),
        try_cast_column("load_date", "date").alias("load_date"),
        try_cast_column("loaded_at", "timestamp").alias("loaded_at"),
        try_cast_column("source_event_at", "timestamp").alias("source_event_at"),
        *silver_lineage_columns(),
        source_load_date_for_deduplication(),
    )

    silver_df = deduplicate_latest(
        silver_df,
        key_columns=["campaign_id"],
        order_columns=["updated_at", "source_event_at", "loaded_at"],
    )

    validate_marketing_campaigns_silver(silver_df)
    return silver_df


def validate_marketing_campaigns_silver(df: DataFrame) -> None:
    dataset_name = "Marketing campaigns"
    validate_required_values(
        df,
        required_columns=[
            "campaign_id",
            "campaign_name",
            "start_date",
            "budget_amount",
            "currency",
            "is_active",
            "created_at",
            "updated_at",
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
        invalid_when=F.col("budget_amount") <= 0,
        dataset_name=dataset_name,
        rule_description="have non-positive campaign budgets",
    )
    validate_rule(
        df,
        invalid_when=(F.col("end_date").isNotNull() & (F.col("end_date") < F.col("start_date")))
        | (F.col("updated_at") < F.col("created_at")),
        dataset_name=dataset_name,
        rule_description="have invalid campaign chronology",
    )
    validate_unique_key(
        df,
        key_columns=["campaign_id"],
        dataset_name=dataset_name,
    )


def run_marketing_campaigns_silver(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    run_silver_pipeline(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
        transform=transform_marketing_campaigns_silver,
    )


def run_marketing_campaigns_silver_tables(
    spark: SparkSession,
    input_table: str,
    output_table: str,
) -> None:
    run_silver_table_pipeline(
        spark=spark,
        input_table=input_table,
        output_table=output_table,
        transform=transform_marketing_campaigns_silver,
    )
