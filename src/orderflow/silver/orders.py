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
    try_cast_blank_as_default,
    try_cast_column,
    validate_required_values,
    validate_rule,
    validate_unique_key,
)

DECIMAL_TYPE = "decimal(18,2)"
ORDER_STATUSES = ["created", "paid", "shipped", "cancelled", "returned"]

ORDERS_REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_created_at",
    "order_updated_at",
    "country_code",
    "currency",
    "gross_amount",
    "discount_amount",
    "net_amount",
    "source_channel",
    "campaign_id",
    "load_date",
    "loaded_at",
    "source_event_at",
    *SILVER_LINEAGE_INPUT_COLUMNS,
]


def transform_orders_silver(bronze_df: DataFrame) -> DataFrame:
    validate_required_columns(
        bronze_df,
        ORDERS_REQUIRED_COLUMNS,
        dataset_name="Bronze orders",
    )

    silver_df = bronze_df.select(
        normalize_lower("order_id").alias("order_id"),
        normalize_lower("customer_id").alias("customer_id"),
        normalize_lower("order_status").alias("order_status"),
        try_cast_column("order_created_at", "timestamp").alias("order_created_at"),
        try_cast_column("order_updated_at", "timestamp").alias("order_updated_at"),
        normalize_upper("country_code").alias("country_code"),
        normalize_upper("currency").alias("currency"),
        try_cast_column("gross_amount", DECIMAL_TYPE).alias("gross_amount"),
        try_cast_blank_as_default(
            "discount_amount",
            DECIMAL_TYPE,
            "0.00",
        ).alias("discount_amount"),
        try_cast_column("net_amount", DECIMAL_TYPE).alias("net_amount"),
        normalize_lower("source_channel").alias("source_channel"),
        normalize_lower("campaign_id").alias("campaign_id"),
        try_cast_column("source_event_at", "timestamp").alias("source_event_at"),
        *silver_lineage_columns(),
        try_cast_column("loaded_at", "timestamp").alias("_dedupe_loaded_at"),
        source_load_date_for_deduplication(),
    )

    silver_df = deduplicate_latest(
        silver_df,
        key_columns=["order_id"],
        order_columns=["order_updated_at", "source_event_at", "_dedupe_loaded_at"],
    ).drop("_dedupe_loaded_at")

    validate_orders_silver(silver_df)
    return silver_df


def validate_orders_silver(df: DataFrame) -> None:
    dataset_name = "Orders"
    validate_required_values(
        df,
        required_columns=[
            "order_id",
            "order_status",
            "order_created_at",
            "country_code",
            "currency",
            "gross_amount",
            "discount_amount",
            "net_amount",
            "source_channel",
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
        invalid_when=~F.col("order_status").isin(ORDER_STATUSES),
        dataset_name=dataset_name,
        rule_description="have invalid order status values",
    )
    validate_rule(
        df,
        invalid_when=(F.col("gross_amount") < 0)
        | (F.col("discount_amount") < 0)
        | (F.col("discount_amount") > F.col("gross_amount"))
        | (
            F.col("net_amount")
            != (F.col("gross_amount") - F.col("discount_amount")).cast(DECIMAL_TYPE)
        ),
        dataset_name=dataset_name,
        rule_description="have inconsistent order amounts",
    )
    validate_rule(
        df,
        invalid_when=F.col("order_updated_at").isNotNull()
        & (F.col("order_updated_at") < F.col("order_created_at")),
        dataset_name=dataset_name,
        rule_description="have order_updated_at before order_created_at",
    )
    validate_unique_key(
        df,
        key_columns=["order_id"],
        dataset_name=dataset_name,
    )


def run_orders_silver(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    run_silver_pipeline(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
        transform=transform_orders_silver,
    )


def run_orders_silver_tables(
    spark: SparkSession,
    input_table: str,
    output_table: str,
) -> None:
    run_silver_table_pipeline(
        spark=spark,
        input_table=input_table,
        output_table=output_table,
        transform=transform_orders_silver,
    )
