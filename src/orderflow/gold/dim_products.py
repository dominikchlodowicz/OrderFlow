from datetime import datetime
from decimal import Decimal
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from orderflow.common.delta import read_delta, read_delta_table
from orderflow.common.validation import validate_required_columns
from orderflow.gold.common import (
    NOT_APPLICABLE_PRODUCT_ID,
    NOT_APPLICABLE_PRODUCT_KEY,
    UNKNOWN_KEY,
    UNKNOWN_MEMBER_ID,
    surrogate_key,
    validate_required_values,
    validate_rule,
    validate_unique_key,
    with_gold_processed_at,
    write_gold,
    write_gold_table,
)

DIM_PRODUCTS_REQUIRED_COLUMNS = [
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
]

SPECIAL_PRODUCT_KEYS = [UNKNOWN_KEY, NOT_APPLICABLE_PRODUCT_KEY]

SPECIAL_PRODUCT_SCHEMA = T.StructType(
    [
        T.StructField("product_key", T.LongType(), nullable=False),
        T.StructField("product_id", T.StringType(), nullable=False),
        T.StructField("sku", T.StringType(), nullable=False),
        T.StructField("product_name", T.StringType(), nullable=False),
        T.StructField("category", T.StringType(), nullable=True),
        T.StructField("brand", T.StringType(), nullable=True),
        T.StructField("unit_price", T.DecimalType(18, 2), nullable=False),
        T.StructField("currency_code", T.StringType(), nullable=False),
        T.StructField("is_active", T.BooleanType(), nullable=False),
        T.StructField("created_at", T.TimestampType(), nullable=False),
        T.StructField("updated_at", T.TimestampType(), nullable=True),
    ]
)

SPECIAL_PRODUCTS = [
    (
        UNKNOWN_KEY,
        UNKNOWN_MEMBER_ID,
        UNKNOWN_MEMBER_ID.upper(),
        "Unknown product",
        None,
        None,
        Decimal("0.00"),
        "XXX",
        False,
        datetime(1970, 1, 1),
        None,
    ),
    (
        NOT_APPLICABLE_PRODUCT_KEY,
        NOT_APPLICABLE_PRODUCT_ID,
        NOT_APPLICABLE_PRODUCT_ID.upper(),
        "Not applicable",
        None,
        None,
        Decimal("0.00"),
        "XXX",
        False,
        datetime(1970, 1, 1),
        None,
    ),
]


def transform_dim_products(silver_products_df: DataFrame) -> DataFrame:
    """Convert the current Silver product snapshot into a Gold dimension."""
    validate_required_columns(
        silver_products_df,
        DIM_PRODUCTS_REQUIRED_COLUMNS,
        dataset_name="Silver products",
    )

    business_products_df = silver_products_df.select(
        surrogate_key("product_id").alias("product_key"),
        F.col("product_id"),
        F.col("sku"),
        F.col("product_name"),
        F.col("category"),
        F.col("brand"),
        F.col("unit_price").cast("decimal(18,2)").alias("unit_price"),
        F.col("currency").alias("currency_code"),
        F.col("is_active").cast("boolean").alias("is_active"),
        F.col("created_at").cast("timestamp").alias("created_at"),
        F.col("updated_at").cast("timestamp").alias("updated_at"),
    )
    special_products_df = silver_products_df.sparkSession.createDataFrame(
        SPECIAL_PRODUCTS,
        schema=SPECIAL_PRODUCT_SCHEMA,
    )
    dim_products_df = business_products_df.unionByName(special_products_df)

    validate_dim_products(dim_products_df)
    return with_gold_processed_at(dim_products_df)


def validate_dim_products(df: DataFrame) -> None:
    dataset_name = "dim_products"
    validate_required_values(
        df,
        required_columns=[
            "product_key",
            "product_id",
            "sku",
            "product_name",
            "unit_price",
            "currency_code",
            "is_active",
            "created_at",
        ],
        dataset_name=dataset_name,
    )
    regular_product = ~F.col("product_key").isin(SPECIAL_PRODUCT_KEYS)
    validate_rule(
        df,
        invalid_when=regular_product & (F.col("unit_price") < 0),
        dataset_name=dataset_name,
        rule_description="have negative unit prices",
    )
    validate_rule(
        df,
        invalid_when=F.col("updated_at").isNotNull() & (F.col("updated_at") < F.col("created_at")),
        dataset_name=dataset_name,
        rule_description="have updated_at before created_at",
    )
    validate_rule(
        df,
        invalid_when=~F.col("currency_code").rlike(r"^[A-Z]{3}$"),
        dataset_name=dataset_name,
        rule_description="have invalid currency codes",
    )
    validate_unique_key(
        df,
        key_columns=["product_id"],
        dataset_name=dataset_name,
    )
    validate_unique_key(
        df,
        key_columns=["sku"],
        dataset_name=dataset_name,
    )
    validate_unique_key(
        df,
        key_columns=["product_key"],
        dataset_name=dataset_name,
    )


def write_dim_products(dim_products_df: DataFrame, output_path: str | Path) -> None:
    write_gold(dim_products_df, output_path)


def write_dim_products_table(dim_products_df: DataFrame, output_table: str) -> None:
    write_gold_table(dim_products_df, output_table)


def run_dim_products(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    silver_products_df = read_delta(spark=spark, path=input_path)
    write_dim_products(
        transform_dim_products(silver_products_df),
        output_path,
    )


def run_dim_products_tables(
    spark: SparkSession,
    input_table: str,
    output_table: str,
) -> None:
    silver_products_df = read_delta_table(spark=spark, table_name=input_table)
    write_dim_products_table(
        transform_dim_products(silver_products_df),
        output_table,
    )
