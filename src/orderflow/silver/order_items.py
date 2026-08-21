from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from orderflow.common.validation import validate_required_columns
from orderflow.silver.common import (
    SILVER_LINEAGE_INPUT_COLUMNS,
    deduplicate_latest,
    normalize_lower,
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

ORDER_ITEMS_REQUIRED_COLUMNS = [
    "order_item_id",
    "order_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount_amount",
    "line_total",
    "created_at",
    "load_date",
    "loaded_at",
    "source_event_at",
    *SILVER_LINEAGE_INPUT_COLUMNS,
]


def transform_order_items_silver(bronze_df: DataFrame) -> DataFrame:
    validate_required_columns(
        bronze_df,
        ORDER_ITEMS_REQUIRED_COLUMNS,
        dataset_name="Bronze order_items",
    )

    quantity = try_cast_column("quantity", "int")
    unit_price = try_cast_column("unit_price", DECIMAL_TYPE)
    discount_amount = try_cast_blank_as_default(
        "discount_amount",
        DECIMAL_TYPE,
        "0.00",
    )
    gross_amount = (quantity.cast(DECIMAL_TYPE) * unit_price).cast(DECIMAL_TYPE)
    line_total = (gross_amount - discount_amount).cast(DECIMAL_TYPE)

    silver_df = bronze_df.select(
        normalize_lower("order_item_id").alias("order_item_id"),
        normalize_lower("order_id").alias("order_id"),
        normalize_lower("product_id").alias("product_id"),
        quantity.alias("quantity"),
        unit_price.alias("unit_price"),
        discount_amount.alias("discount_amount"),
        gross_amount.alias("gross_amount"),
        line_total.alias("line_total"),
        try_cast_column("created_at", "timestamp").alias("created_at"),
        try_cast_column("source_event_at", "timestamp").alias("source_event_at"),
        *silver_lineage_columns(),
        try_cast_column("loaded_at", "timestamp").alias("_dedupe_loaded_at"),
        source_load_date_for_deduplication(),
    )

    silver_df = deduplicate_latest(
        silver_df,
        key_columns=["order_item_id"],
        order_columns=["source_event_at", "created_at", "_dedupe_loaded_at"],
    ).drop("_dedupe_loaded_at")

    validate_order_items_silver(silver_df)
    return silver_df


def validate_order_items_silver(df: DataFrame) -> None:
    dataset_name = "Order items"
    validate_required_values(
        df,
        required_columns=[
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "discount_amount",
            "gross_amount",
            "line_total",
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
        invalid_when=F.col("quantity") <= 0,
        dataset_name=dataset_name,
        rule_description="have quantity less than or equal to zero",
    )
    validate_rule(
        df,
        invalid_when=(F.col("unit_price") < 0)
        | (F.col("discount_amount") < 0)
        | (F.col("discount_amount") > F.col("gross_amount")),
        dataset_name=dataset_name,
        rule_description="have invalid monetary values",
    )
    validate_unique_key(
        df,
        key_columns=["order_item_id"],
        dataset_name=dataset_name,
    )


def run_order_items_silver(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    run_silver_pipeline(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
        transform=transform_order_items_silver,
    )


def run_order_items_silver_tables(
    spark: SparkSession,
    input_table: str,
    output_table: str,
) -> None:
    run_silver_table_pipeline(
        spark=spark,
        input_table=input_table,
        output_table=output_table,
        transform=transform_order_items_silver,
    )
