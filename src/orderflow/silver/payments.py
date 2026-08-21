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
PAYMENT_METHODS = [
    "card",
    "paypal",
    "blik",
    "bank_transfer",
    "on delivery",
    "online installments",
]
PAYMENT_STATUSES = ["captured", "authorized", "failed"]
FAILURE_REASONS = ["timeout", "insufficient_funds", "card_declined"]

PAYMENTS_REQUIRED_COLUMNS = [
    "payment_id",
    "order_id",
    "payment_attempt_number",
    "payment_method",
    "payment_status",
    "amount",
    "currency",
    "created_at",
    "processed_at",
    "failure_reason",
    "load_date",
    "loaded_at",
    "source_event_at",
    *SILVER_LINEAGE_INPUT_COLUMNS,
]


def transform_payments_silver(bronze_df: DataFrame) -> DataFrame:
    validate_required_columns(
        bronze_df,
        PAYMENTS_REQUIRED_COLUMNS,
        dataset_name="Bronze payments",
    )

    payment_status = normalize_lower("payment_status")
    failure_reason = F.when(
        payment_status == "failed",
        normalize_lower("failure_reason"),
    ).otherwise(F.lit(None).cast("string"))

    silver_df = bronze_df.select(
        normalize_lower("payment_id").alias("payment_id"),
        normalize_lower("order_id").alias("order_id"),
        try_cast_column("payment_attempt_number", "int").alias("payment_attempt_number"),
        normalize_lower("payment_method").alias("payment_method"),
        payment_status.alias("payment_status"),
        failure_reason.alias("failure_reason"),
        try_cast_column("amount", DECIMAL_TYPE).alias("amount"),
        normalize_upper("currency").alias("currency"),
        try_cast_column("created_at", "timestamp").alias("created_at"),
        try_cast_column("processed_at", "timestamp").alias("processed_at"),
        try_cast_column("load_date", "date").alias("load_date"),
        try_cast_column("loaded_at", "timestamp").alias("loaded_at"),
        *silver_lineage_columns(),
        try_cast_column("source_event_at", "timestamp").alias("_dedupe_source_event_at"),
        source_load_date_for_deduplication(),
    )

    silver_df = deduplicate_latest(
        silver_df,
        key_columns=["payment_id"],
        order_columns=["processed_at", "_dedupe_source_event_at", "loaded_at"],
    ).drop("_dedupe_source_event_at")

    validate_payments_silver(silver_df)
    return silver_df


def validate_payments_silver(df: DataFrame) -> None:
    dataset_name = "Payments"
    validate_required_values(
        df,
        required_columns=[
            "payment_id",
            "order_id",
            "payment_attempt_number",
            "payment_method",
            "payment_status",
            "amount",
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
        invalid_when=(~F.col("payment_method").isin(PAYMENT_METHODS))
        | (~F.col("payment_status").isin(PAYMENT_STATUSES)),
        dataset_name=dataset_name,
        rule_description="have invalid payment method or status values",
    )
    validate_rule(
        df,
        invalid_when=(F.col("payment_status") == "failed")
        & (
            F.col("failure_reason").isNull()
            | ~F.col("failure_reason").isin(FAILURE_REASONS)
        ),
        dataset_name=dataset_name,
        rule_description="have an invalid failure reason",
    )
    validate_rule(
        df,
        invalid_when=(F.col("payment_attempt_number") <= 0)
        | (F.col("amount") < 0)
        | (F.col("processed_at") < F.col("created_at")),
        dataset_name=dataset_name,
        rule_description="have invalid payment values",
    )
    validate_unique_key(
        df,
        key_columns=["payment_id"],
        dataset_name=dataset_name,
    )


def run_payments_silver(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    run_silver_pipeline(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
        transform=transform_payments_silver,
    )


def run_payments_silver_tables(
    spark: SparkSession,
    input_table: str,
    output_table: str,
) -> None:
    run_silver_table_pipeline(
        spark=spark,
        input_table=input_table,
        output_table=output_table,
        transform=transform_payments_silver,
    )
