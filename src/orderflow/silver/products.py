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

PRODUCTS_REQUIRED_COLUMNS = [
    "product_id",
    "sku",
    "product_name",
    "category",
    "brand",
    "unit_price",
    "currency",
    "is_active",
    "created_at",
    "updated_at",
    "load_date",
    "loaded_at",
    "source_event_at",
    *SILVER_LINEAGE_INPUT_COLUMNS,
]


def transform_products_silver(bronze_df: DataFrame) -> DataFrame:
    validate_required_columns(
        bronze_df,
        PRODUCTS_REQUIRED_COLUMNS,
        dataset_name="Bronze products",
    )

    silver_df = bronze_df.select(
        normalize_lower("product_id").alias("product_id"),
        normalize_upper("sku").alias("sku"),
        normalize_blank_to_null("product_name").alias("product_name"),
        normalize_lower("category").alias("category"),
        normalize_blank_to_null("brand").alias("brand"),
        try_cast_column("unit_price", DECIMAL_TYPE).alias("unit_price"),
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
        key_columns=["product_id"],
        order_columns=["updated_at", "source_event_at", "loaded_at"],
    )

    validate_products_silver(silver_df)
    return silver_df


def validate_products_silver(df: DataFrame) -> None:
    dataset_name = "Products"
    validate_required_values(
        df,
        required_columns=[
            "product_id",
            "sku",
            "product_name",
            "unit_price",
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
        invalid_when=F.col("unit_price") < 0,
        dataset_name=dataset_name,
        rule_description="have negative unit prices",
    )
    validate_rule(
        df,
        invalid_when=F.col("updated_at") < F.col("created_at"),
        dataset_name=dataset_name,
        rule_description="have updated_at before created_at",
    )
    validate_unique_key(
        df,
        key_columns=["product_id"],
        dataset_name=dataset_name,
    )


def run_products_silver(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    run_silver_pipeline(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
        transform=transform_products_silver,
    )


def run_products_silver_tables(
    spark: SparkSession,
    input_table: str,
    output_table: str,
) -> None:
    run_silver_table_pipeline(
        spark=spark,
        input_table=input_table,
        output_table=output_table,
        transform=transform_products_silver,
    )
