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
SHIPMENT_STATUSES = ["lost", "shipped", "delivered", "returned"]

SHIPMENTS_REQUIRED_COLUMNS = [
    "shipment_id",
    "order_id",
    "carrier",
    "shipment_status",
    "shipped_at",
    "estimated_delivery_at",
    "delivered_at",
    "delivery_country",
    "delivery_city",
    "shipping_cost",
    "load_date",
    "loaded_at",
    "source_event_at",
    *SILVER_LINEAGE_INPUT_COLUMNS,
]


def transform_shipments_silver(bronze_df: DataFrame) -> DataFrame:
    validate_required_columns(
        bronze_df,
        SHIPMENTS_REQUIRED_COLUMNS,
        dataset_name="Bronze shipments",
    )

    silver_df = bronze_df.select(
        normalize_lower("shipment_id").alias("shipment_id"),
        normalize_lower("order_id").alias("order_id"),
        normalize_blank_to_null("carrier").alias("carrier"),
        normalize_lower("shipment_status").alias("shipment_status"),
        try_cast_column("shipped_at", "timestamp").alias("shipped_at"),
        try_cast_column("estimated_delivery_at", "date").alias("estimated_delivery_at"),
        try_cast_column("delivered_at", "timestamp").alias("delivered_at"),
        normalize_upper("delivery_country").alias("delivery_country"),
        normalize_blank_to_null("delivery_city").alias("delivery_city"),
        try_cast_column("shipping_cost", DECIMAL_TYPE).alias("shipping_cost"),
        try_cast_column("load_date", "date").alias("load_date"),
        try_cast_column("loaded_at", "timestamp").alias("loaded_at"),
        try_cast_column("source_event_at", "timestamp").alias("source_event_at"),
        *silver_lineage_columns(),
        source_load_date_for_deduplication(),
    )

    silver_df = deduplicate_latest(
        silver_df,
        key_columns=["shipment_id"],
        order_columns=["source_event_at", "delivered_at", "shipped_at", "loaded_at"],
    )

    validate_shipments_silver(silver_df)
    return silver_df


def validate_shipments_silver(df: DataFrame) -> None:
    dataset_name = "Shipments"
    validate_required_values(
        df,
        required_columns=[
            "shipment_id",
            "order_id",
            "carrier",
            "shipment_status",
            "shipped_at",
            "delivery_country",
            "delivery_city",
            "shipping_cost",
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
        invalid_when=(~F.col("shipment_status").isin(SHIPMENT_STATUSES))
        | (F.col("shipping_cost") < 0),
        dataset_name=dataset_name,
        rule_description="have invalid shipment status or cost values",
    )
    validate_rule(
        df,
        invalid_when=F.col("estimated_delivery_at").isNotNull()
        & (F.col("estimated_delivery_at") < F.to_date(F.col("shipped_at"))),
        dataset_name=dataset_name,
        rule_description="have estimated delivery before shipment",
    )
    validate_rule(
        df,
        invalid_when=((F.col("shipment_status") == "delivered") & F.col("delivered_at").isNull())
        | (
            F.col("delivered_at").isNotNull()
            & (F.col("delivered_at") < F.col("shipped_at"))
        ),
        dataset_name=dataset_name,
        rule_description="have invalid delivered_at values",
    )
    validate_unique_key(
        df,
        key_columns=["shipment_id"],
        dataset_name=dataset_name,
    )


def run_shipments_silver(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    run_silver_pipeline(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
        transform=transform_shipments_silver,
    )


def run_shipments_silver_tables(
    spark: SparkSession,
    input_table: str,
    output_table: str,
) -> None:
    run_silver_table_pipeline(
        spark=spark,
        input_table=input_table,
        output_table=output_table,
        transform=transform_shipments_silver,
    )
