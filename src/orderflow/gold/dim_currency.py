from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from orderflow.common.delta import read_delta, read_delta_table
from orderflow.common.validation import validate_required_columns
from orderflow.gold.common import (
    surrogate_key,
    validate_required_values,
    validate_rule,
    validate_unique_key,
    with_gold_processed_at,
    write_gold,
    write_gold_table,
)

CURRENCY_INPUTS = [
    "Silver products",
    "Silver marketing_campaigns",
    "Silver orders",
    "Silver payments",
    "Silver refunds",
    "Silver exchange_rates",
]


def transform_dim_currency(
    silver_products_df: DataFrame,
    silver_marketing_campaigns_df: DataFrame,
    silver_orders_df: DataFrame,
    silver_payments_df: DataFrame,
    silver_refunds_df: DataFrame,
    silver_exchange_rates_df: DataFrame,
) -> DataFrame:
    """Build one conformed currency member from every currency-bearing source."""
    input_frames = [
        silver_products_df,
        silver_marketing_campaigns_df,
        silver_orders_df,
        silver_payments_df,
        silver_refunds_df,
        silver_exchange_rates_df,
    ]
    for input_df, dataset_name in zip(input_frames, CURRENCY_INPUTS, strict=True):
        validate_required_columns(
            input_df,
            ["currency"],
            dataset_name=dataset_name,
        )

    currency_codes_df = input_frames[0].select(F.col("currency").alias("currency_code"))
    for input_df in input_frames[1:]:
        currency_codes_df = currency_codes_df.unionByName(
            input_df.select(F.col("currency").alias("currency_code"))
        )

    currency_codes_df = currency_codes_df.select(
        F.upper(F.trim(F.col("currency_code").cast("string"))).alias("currency_code")
    ).distinct()
    dim_currency_df = currency_codes_df.select(
        surrogate_key("currency_code").alias("currency_key"),
        F.col("currency_code"),
        (F.col("currency_code") == F.lit("PLN")).alias("is_reporting_currency"),
    )

    validate_dim_currency(dim_currency_df)
    return with_gold_processed_at(dim_currency_df)


def validate_dim_currency(df: DataFrame) -> None:
    dataset_name = "dim_currency"
    validate_required_values(
        df,
        required_columns=[
            "currency_key",
            "currency_code",
            "is_reporting_currency",
        ],
        dataset_name=dataset_name,
    )
    validate_rule(
        df,
        invalid_when=~F.col("currency_code").rlike(r"^[A-Z]{3}$"),
        dataset_name=dataset_name,
        rule_description="have invalid currency codes",
    )
    validate_rule(
        df,
        invalid_when=(F.col("currency_code") == "PLN") != F.col("is_reporting_currency"),
        dataset_name=dataset_name,
        rule_description="have an invalid reporting-currency flag",
    )
    validate_unique_key(
        df,
        key_columns=["currency_code"],
        dataset_name=dataset_name,
    )
    validate_unique_key(
        df,
        key_columns=["currency_key"],
        dataset_name=dataset_name,
    )


def write_dim_currency(dim_currency_df: DataFrame, output_path: str | Path) -> None:
    write_gold(dim_currency_df, output_path)


def write_dim_currency_table(dim_currency_df: DataFrame, output_table: str) -> None:
    write_gold_table(dim_currency_df, output_table)


def run_dim_currency(
    spark: SparkSession,
    products_input_path: str | Path,
    marketing_campaigns_input_path: str | Path,
    orders_input_path: str | Path,
    payments_input_path: str | Path,
    refunds_input_path: str | Path,
    exchange_rates_input_path: str | Path,
    output_path: str | Path,
) -> None:
    dim_currency_df = transform_dim_currency(
        read_delta(spark=spark, path=products_input_path),
        read_delta(spark=spark, path=marketing_campaigns_input_path),
        read_delta(spark=spark, path=orders_input_path),
        read_delta(spark=spark, path=payments_input_path),
        read_delta(spark=spark, path=refunds_input_path),
        read_delta(spark=spark, path=exchange_rates_input_path),
    )
    write_dim_currency(dim_currency_df, output_path)


def run_dim_currency_tables(
    spark: SparkSession,
    products_input_table: str,
    marketing_campaigns_input_table: str,
    orders_input_table: str,
    payments_input_table: str,
    refunds_input_table: str,
    exchange_rates_input_table: str,
    output_table: str,
) -> None:
    dim_currency_df = transform_dim_currency(
        read_delta_table(spark=spark, table_name=products_input_table),
        read_delta_table(spark=spark, table_name=marketing_campaigns_input_table),
        read_delta_table(spark=spark, table_name=orders_input_table),
        read_delta_table(spark=spark, table_name=payments_input_table),
        read_delta_table(spark=spark, table_name=refunds_input_table),
        read_delta_table(spark=spark, table_name=exchange_rates_input_table),
    )
    write_dim_currency_table(dim_currency_df, output_table)
