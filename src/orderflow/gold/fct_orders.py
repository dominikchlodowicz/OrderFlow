"""Gold current-state order fact transformation and orchestration."""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from orderflow.common.delta import read_delta, read_delta_table
from orderflow.common.validation import validate_required_columns
from orderflow.gold.common import (
    GUEST_CUSTOMER_KEY,
    NO_CAMPAIGN_KEY,
    UNKNOWN_KEY,
    surrogate_key,
    validate_required_values,
    validate_rule,
    validate_unique_key,
    with_gold_processed_at,
    write_gold,
    write_gold_table,
)

ORDERS_REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "campaign_id",
    "currency",
    "order_created_at",
    "order_updated_at",
    "order_status",
    "country_code",
    "source_channel",
    "gross_amount",
    "discount_amount",
    "net_amount",
]


def _validate_orders_lookups(
    *,
    dim_customers_df: DataFrame,
    dim_campaigns_df: DataFrame,
    dim_currency_df: DataFrame,
    dim_calendar_df: DataFrame,
) -> None:
    lookups = [
        (dim_customers_df, ["customer_id", "customer_key"], "dim_customers"),
        (dim_campaigns_df, ["campaign_id", "campaign_key"], "dim_campaigns"),
        (dim_currency_df, ["currency_code", "currency_key"], "dim_currency"),
        (dim_calendar_df, ["date_day", "date_key"], "dim_calendar"),
    ]
    for lookup_df, required_columns, dataset_name in lookups:
        validate_required_columns(
            lookup_df,
            required_columns,
            dataset_name=dataset_name,
        )
        validate_required_values(
            lookup_df,
            required_columns=required_columns,
            dataset_name=dataset_name,
        )
        validate_unique_key(
            lookup_df,
            key_columns=[required_columns[0]],
            dataset_name=dataset_name,
        )
        validate_unique_key(
            lookup_df,
            key_columns=[required_columns[1]],
            dataset_name=dataset_name,
        )


def transform_fct_orders(
    *,
    silver_orders_df: DataFrame,
    dim_customers_df: DataFrame,
    dim_campaigns_df: DataFrame,
    dim_currency_df: DataFrame,
    dim_calendar_df: DataFrame,
) -> DataFrame:
    """Enrich accepted Silver orders with dimensional keys."""
    validate_required_columns(
        silver_orders_df,
        ORDERS_REQUIRED_COLUMNS,
        dataset_name="Silver orders",
    )
    _validate_orders_lookups(
        dim_customers_df=dim_customers_df,
        dim_campaigns_df=dim_campaigns_df,
        dim_currency_df=dim_currency_df,
        dim_calendar_df=dim_calendar_df,
    )

    customer_lookup = dim_customers_df.select(
        F.col("customer_id").alias("_customer_lookup_id"),
        F.col("customer_key").alias("_resolved_customer_key"),
    )
    campaign_lookup = dim_campaigns_df.select(
        F.col("campaign_id").alias("_campaign_lookup_id"),
        F.col("campaign_key").alias("_resolved_campaign_key"),
    )
    currency_lookup = dim_currency_df.select(
        F.col("currency_code").alias("_currency_lookup_code"),
        F.col("currency_key").alias("_resolved_currency_key"),
    )
    calendar_lookup = dim_calendar_df.select(
        F.col("date_day").alias("_calendar_date_day"),
        F.col("date_key").alias("_resolved_order_date_key"),
    )

    enriched_df = (
        silver_orders_df.join(
            customer_lookup,
            F.col("customer_id") == F.col("_customer_lookup_id"),
            "left",
        )
        .join(
            campaign_lookup,
            F.col("campaign_id") == F.col("_campaign_lookup_id"),
            "left",
        )
        .join(
            currency_lookup,
            F.col("currency") == F.col("_currency_lookup_code"),
            "left",
        )
        .join(
            calendar_lookup,
            F.to_date(F.col("order_created_at")) == F.col("_calendar_date_day"),
            "left",
        )
    )

    validate_rule(
        enriched_df,
        invalid_when=F.col("_resolved_currency_key").isNull(),
        dataset_name="fct_orders",
        rule_description="have unresolved currency references",
    )
    validate_rule(
        enriched_df,
        invalid_when=F.col("_resolved_order_date_key").isNull(),
        dataset_name="fct_orders",
        rule_description="have unresolved order calendar dates",
    )

    customer_key = (
        F.when(
            F.col("customer_id").isNull(),
            F.lit(GUEST_CUSTOMER_KEY).cast("bigint"),
        )
        .when(
            F.col("_resolved_customer_key").isNull(),
            F.lit(UNKNOWN_KEY).cast("bigint"),
        )
        .otherwise(F.col("_resolved_customer_key"))
    )
    campaign_key = (
        F.when(
            F.col("campaign_id").isNull(),
            F.lit(NO_CAMPAIGN_KEY).cast("bigint"),
        )
        .when(
            F.col("_resolved_campaign_key").isNull(),
            F.lit(UNKNOWN_KEY).cast("bigint"),
        )
        .otherwise(F.col("_resolved_campaign_key"))
    )

    fct_orders_df = enriched_df.select(
        surrogate_key("order_id").alias("order_key"),
        F.col("order_id"),
        customer_key.alias("customer_key"),
        campaign_key.alias("campaign_key"),
        F.col("_resolved_currency_key").alias("currency_key"),
        F.col("_resolved_order_date_key").alias("order_date_key"),
        F.col("order_created_at"),
        F.col("order_updated_at"),
        F.col("order_status"),
        F.col("country_code").alias("order_country_code"),
        F.col("source_channel"),
        F.col("gross_amount"),
        F.col("discount_amount"),
        F.col("net_amount"),
    )

    fct_orders_df = with_gold_processed_at(fct_orders_df)
    validate_fct_orders(fct_orders_df)
    return fct_orders_df


def validate_fct_orders(df: DataFrame) -> None:
    dataset_name = "fct_orders"
    validate_required_values(
        df,
        required_columns=[
            "order_key",
            "order_id",
            "customer_key",
            "campaign_key",
            "currency_key",
            "order_date_key",
            "order_created_at",
            "order_status",
            "order_country_code",
            "source_channel",
            "gross_amount",
            "discount_amount",
            "net_amount",
            "_gold_processed_at",
        ],
        dataset_name=dataset_name,
    )
    validate_rule(
        df,
        invalid_when=(F.col("gross_amount") < 0)
        | (F.col("discount_amount") < 0)
        | (F.col("discount_amount") > F.col("gross_amount"))
        | (
            F.col("net_amount")
            != (F.col("gross_amount") - F.col("discount_amount")).cast("decimal(18,2)")
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
    validate_unique_key(
        df,
        key_columns=["order_key"],
        dataset_name=dataset_name,
    )


def write_fct_orders(fct_orders_df: DataFrame, output_path: str | Path) -> None:
    write_gold(fct_orders_df, output_path)


def write_fct_orders_table(fct_orders_df: DataFrame, output_table: str) -> None:
    write_gold_table(fct_orders_df, output_table)


def run_fct_orders(
    spark: SparkSession,
    *,
    orders_input_path: str | Path,
    customers_input_path: str | Path,
    campaigns_input_path: str | Path,
    currency_input_path: str | Path,
    calendar_input_path: str | Path,
    output_path: str | Path,
) -> None:
    fct_orders_df = transform_fct_orders(
        silver_orders_df=read_delta(spark, orders_input_path),
        dim_customers_df=read_delta(spark, customers_input_path),
        dim_campaigns_df=read_delta(spark, campaigns_input_path),
        dim_currency_df=read_delta(spark, currency_input_path),
        dim_calendar_df=read_delta(spark, calendar_input_path),
    )
    write_fct_orders(fct_orders_df, output_path)


def run_fct_orders_tables(
    spark: SparkSession,
    *,
    orders_input_table: str,
    customers_input_table: str,
    campaigns_input_table: str,
    currency_input_table: str,
    calendar_input_table: str,
    output_table: str,
) -> None:
    fct_orders_df = transform_fct_orders(
        silver_orders_df=read_delta_table(spark, orders_input_table),
        dim_customers_df=read_delta_table(spark, customers_input_table),
        dim_campaigns_df=read_delta_table(spark, campaigns_input_table),
        dim_currency_df=read_delta_table(spark, currency_input_table),
        dim_calendar_df=read_delta_table(spark, calendar_input_table),
    )
    write_fct_orders_table(fct_orders_df, output_table)
