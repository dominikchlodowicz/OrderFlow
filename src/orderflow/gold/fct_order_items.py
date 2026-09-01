"""Gold order-item fact transformation and orchestration."""

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

ORDER_ITEMS_REQUIRED_COLUMNS = [
    "order_item_id",
    "order_id",
    "product_id",
    "created_at",
    "quantity",
    "unit_price",
    "discount_amount",
    "gross_amount",
    "line_total",
]

ORDER_HEADER_REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "campaign_id",
    "currency",
    "country_code",
    "order_created_at",
]


def _validate_order_item_inputs(
    *,
    silver_orders_df: DataFrame,
    dim_customers_df: DataFrame,
    dim_products_df: DataFrame,
    dim_campaigns_df: DataFrame,
    dim_currency_df: DataFrame,
    dim_calendar_df: DataFrame,
) -> None:
    validate_required_columns(
        silver_orders_df,
        ORDER_HEADER_REQUIRED_COLUMNS,
        dataset_name="Silver orders",
    )
    validate_required_values(
        silver_orders_df,
        required_columns=["order_id", "currency", "country_code", "order_created_at"],
        dataset_name="Silver orders",
    )
    validate_unique_key(
        silver_orders_df,
        key_columns=["order_id"],
        dataset_name="Silver orders",
    )

    lookups = [
        (dim_customers_df, ["customer_id", "customer_key"], "dim_customers"),
        (dim_products_df, ["product_id", "product_key"], "dim_products"),
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


def transform_fct_order_items(
    *,
    silver_order_items_df: DataFrame,
    silver_orders_df: DataFrame,
    dim_customers_df: DataFrame,
    dim_products_df: DataFrame,
    dim_campaigns_df: DataFrame,
    dim_currency_df: DataFrame,
    dim_calendar_df: DataFrame,
) -> DataFrame:
    """Enrich order items with their accepted order-header context."""
    validate_required_columns(
        silver_order_items_df,
        ORDER_ITEMS_REQUIRED_COLUMNS,
        dataset_name="Silver order items",
    )
    _validate_order_item_inputs(
        silver_orders_df=silver_orders_df,
        dim_customers_df=dim_customers_df,
        dim_products_df=dim_products_df,
        dim_campaigns_df=dim_campaigns_df,
        dim_currency_df=dim_currency_df,
        dim_calendar_df=dim_calendar_df,
    )

    order_lookup = silver_orders_df.select(
        F.col("order_id").alias("_header_order_id"),
        F.col("customer_id").alias("_header_customer_id"),
        F.col("campaign_id").alias("_header_campaign_id"),
        F.col("currency").alias("_header_currency"),
        F.col("country_code").alias("_header_country_code"),
        F.col("order_created_at").alias("_header_order_created_at"),
    )
    customer_lookup = dim_customers_df.select(
        F.col("customer_id").alias("_customer_lookup_id"),
        F.col("customer_key").alias("_resolved_customer_key"),
    )
    product_lookup = dim_products_df.select(
        F.col("product_id").alias("_product_lookup_id"),
        F.col("product_key").alias("_resolved_product_key"),
    )
    campaign_lookup = dim_campaigns_df.select(
        F.col("campaign_id").alias("_campaign_lookup_id"),
        F.col("campaign_key").alias("_resolved_campaign_key"),
    )
    currency_lookup = dim_currency_df.select(
        F.col("currency_code").alias("_currency_lookup_code"),
        F.col("currency_key").alias("_resolved_currency_key"),
    )
    order_calendar_lookup = dim_calendar_df.select(
        F.col("date_day").alias("_order_calendar_date_day"),
        F.col("date_key").alias("_resolved_order_date_key"),
    )
    item_calendar_lookup = dim_calendar_df.select(
        F.col("date_day").alias("_item_calendar_date_day"),
        F.col("date_key").alias("_resolved_item_created_date_key"),
    )

    enriched_df = (
        silver_order_items_df.join(
            order_lookup,
            F.col("order_id") == F.col("_header_order_id"),
            "left",
        )
        .join(
            customer_lookup,
            F.col("_header_customer_id") == F.col("_customer_lookup_id"),
            "left",
        )
        .join(
            product_lookup,
            F.col("product_id") == F.col("_product_lookup_id"),
            "left",
        )
        .join(
            campaign_lookup,
            F.col("_header_campaign_id") == F.col("_campaign_lookup_id"),
            "left",
        )
        .join(
            currency_lookup,
            F.col("_header_currency") == F.col("_currency_lookup_code"),
            "left",
        )
        .join(
            order_calendar_lookup,
            F.to_date(F.col("_header_order_created_at")) == F.col("_order_calendar_date_day"),
            "left",
        )
        .join(
            item_calendar_lookup,
            F.to_date(F.col("created_at")) == F.col("_item_calendar_date_day"),
            "left",
        )
    )

    relationship_rules = [
        (F.col("_header_order_id").isNull(), "have unresolved order references"),
        (F.col("_resolved_product_key").isNull(), "have unresolved product references"),
        (F.col("_resolved_currency_key").isNull(), "have unresolved currency references"),
        (F.col("_resolved_order_date_key").isNull(), "have unresolved order calendar dates"),
        (
            F.col("_resolved_item_created_date_key").isNull(),
            "have unresolved order-item calendar dates",
        ),
    ]
    for invalid_when, rule_description in relationship_rules:
        validate_rule(
            enriched_df,
            invalid_when=invalid_when,
            dataset_name="fct_order_items",
            rule_description=rule_description,
        )

    customer_key = (
        F.when(
            F.col("_header_customer_id").isNull(),
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
            F.col("_header_campaign_id").isNull(),
            F.lit(NO_CAMPAIGN_KEY).cast("bigint"),
        )
        .when(
            F.col("_resolved_campaign_key").isNull(),
            F.lit(UNKNOWN_KEY).cast("bigint"),
        )
        .otherwise(F.col("_resolved_campaign_key"))
    )

    fct_order_items_df = enriched_df.select(
        surrogate_key("order_item_id").alias("order_item_key"),
        F.col("order_item_id"),
        F.col("order_id"),
        customer_key.alias("customer_key"),
        F.col("_resolved_product_key").alias("product_key"),
        campaign_key.alias("campaign_key"),
        F.col("_resolved_currency_key").alias("currency_key"),
        F.col("_resolved_order_date_key").alias("order_date_key"),
        F.col("_resolved_item_created_date_key").alias("order_item_created_date_key"),
        F.col("_header_country_code").alias("order_country_code"),
        F.col("created_at").alias("order_item_created_at"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("discount_amount"),
        F.col("gross_amount"),
        F.col("line_total"),
    )

    fct_order_items_df = with_gold_processed_at(fct_order_items_df)
    validate_fct_order_items(fct_order_items_df)
    return fct_order_items_df


def validate_fct_order_items(df: DataFrame) -> None:
    dataset_name = "fct_order_items"
    validate_required_values(
        df,
        required_columns=[
            "order_item_key",
            "order_item_id",
            "order_id",
            "customer_key",
            "product_key",
            "campaign_key",
            "currency_key",
            "order_date_key",
            "order_item_created_date_key",
            "order_country_code",
            "order_item_created_at",
            "quantity",
            "unit_price",
            "discount_amount",
            "gross_amount",
            "line_total",
            "_gold_processed_at",
        ],
        dataset_name=dataset_name,
    )
    expected_gross_amount = (F.col("quantity") * F.col("unit_price")).cast("decimal(18,2)")
    expected_line_total = (F.col("gross_amount") - F.col("discount_amount")).cast("decimal(18,2)")
    validate_rule(
        df,
        invalid_when=(F.col("quantity") <= 0)
        | (F.col("unit_price") < 0)
        | (F.col("discount_amount") < 0)
        | (F.col("discount_amount") > F.col("gross_amount"))
        | (F.col("gross_amount") != expected_gross_amount)
        | (F.col("line_total") != expected_line_total),
        dataset_name=dataset_name,
        rule_description="have inconsistent order-item measures",
    )
    validate_unique_key(
        df,
        key_columns=["order_item_id"],
        dataset_name=dataset_name,
    )
    validate_unique_key(
        df,
        key_columns=["order_item_key"],
        dataset_name=dataset_name,
    )


def write_fct_order_items(
    fct_order_items_df: DataFrame,
    output_path: str | Path,
) -> None:
    write_gold(fct_order_items_df, output_path)


def write_fct_order_items_table(
    fct_order_items_df: DataFrame,
    output_table: str,
) -> None:
    write_gold_table(fct_order_items_df, output_table)


def run_fct_order_items(
    spark: SparkSession,
    *,
    order_items_input_path: str | Path,
    orders_input_path: str | Path,
    customers_input_path: str | Path,
    products_input_path: str | Path,
    campaigns_input_path: str | Path,
    currency_input_path: str | Path,
    calendar_input_path: str | Path,
    output_path: str | Path,
) -> None:
    fct_order_items_df = transform_fct_order_items(
        silver_order_items_df=read_delta(spark, order_items_input_path),
        silver_orders_df=read_delta(spark, orders_input_path),
        dim_customers_df=read_delta(spark, customers_input_path),
        dim_products_df=read_delta(spark, products_input_path),
        dim_campaigns_df=read_delta(spark, campaigns_input_path),
        dim_currency_df=read_delta(spark, currency_input_path),
        dim_calendar_df=read_delta(spark, calendar_input_path),
    )
    write_fct_order_items(fct_order_items_df, output_path)


def run_fct_order_items_tables(
    spark: SparkSession,
    *,
    order_items_input_table: str,
    orders_input_table: str,
    customers_input_table: str,
    products_input_table: str,
    campaigns_input_table: str,
    currency_input_table: str,
    calendar_input_table: str,
    output_table: str,
) -> None:
    fct_order_items_df = transform_fct_order_items(
        silver_order_items_df=read_delta_table(spark, order_items_input_table),
        silver_orders_df=read_delta_table(spark, orders_input_table),
        dim_customers_df=read_delta_table(spark, customers_input_table),
        dim_products_df=read_delta_table(spark, products_input_table),
        dim_campaigns_df=read_delta_table(spark, campaigns_input_table),
        dim_currency_df=read_delta_table(spark, currency_input_table),
        dim_calendar_df=read_delta_table(spark, calendar_input_table),
    )
    write_fct_order_items_table(fct_order_items_df, output_table)
