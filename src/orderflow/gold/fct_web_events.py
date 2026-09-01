from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from orderflow.common.delta import read_delta, read_delta_table
from orderflow.common.validation import validate_required_columns
from orderflow.gold.common import (
    ANONYMOUS_CUSTOMER_KEY,
    NO_CAMPAIGN_KEY,
    NOT_APPLICABLE_PRODUCT_KEY,
    UNKNOWN_KEY,
    surrogate_key,
    validate_required_values,
    validate_rule,
    validate_unique_key,
    with_gold_processed_at,
    write_gold,
    write_gold_table,
)

DEVICE_TYPES = ["tablet", "mobile", "desktop"]
PRODUCT_EVENT_TYPES = ["product_view", "add_to_cart"]

SILVER_WEB_EVENTS_REQUIRED_COLUMNS = [
    "event_id",
    "session_id",
    "customer_id",
    "anonymous_id",
    "event_type",
    "event_timestamp",
    "product_id",
    "campaign_id",
    "device_type",
    "country_code",
    "page_url",
]
DIM_CUSTOMERS_REQUIRED_COLUMNS = ["customer_key", "customer_id"]
DIM_PRODUCTS_REQUIRED_COLUMNS = ["product_key", "product_id"]
DIM_CAMPAIGNS_REQUIRED_COLUMNS = ["campaign_key", "campaign_id"]
DIM_CALENDAR_REQUIRED_COLUMNS = ["date_key", "date_day"]

FCT_WEB_EVENTS_REQUIRED_COLUMNS = [
    "event_key",
    "event_id",
    "session_id",
    "anonymous_id",
    "customer_key",
    "product_key",
    "campaign_key",
    "event_date_key",
    "event_type",
    "event_timestamp",
    "device_type",
    "country_code",
    "page_url",
    "_gold_processed_at",
]


def transform_fct_web_events(
    *,
    silver_web_events_df: DataFrame,
    dim_customers_df: DataFrame,
    dim_products_df: DataFrame,
    dim_campaigns_df: DataFrame,
    dim_calendar_df: DataFrame,
) -> DataFrame:
    """Build the immutable web-event fact with documented fallback members."""
    validate_required_columns(
        silver_web_events_df,
        SILVER_WEB_EVENTS_REQUIRED_COLUMNS,
        dataset_name="Silver web_events",
    )
    validate_required_columns(
        dim_customers_df,
        DIM_CUSTOMERS_REQUIRED_COLUMNS,
        dataset_name="Gold dim_customers",
    )
    validate_required_columns(
        dim_products_df,
        DIM_PRODUCTS_REQUIRED_COLUMNS,
        dataset_name="Gold dim_products",
    )
    validate_required_columns(
        dim_campaigns_df,
        DIM_CAMPAIGNS_REQUIRED_COLUMNS,
        dataset_name="Gold dim_campaigns",
    )
    validate_required_columns(
        dim_calendar_df,
        DIM_CALENDAR_REQUIRED_COLUMNS,
        dataset_name="Gold dim_calendar",
    )

    events = silver_web_events_df.select(
        *SILVER_WEB_EVENTS_REQUIRED_COLUMNS,
        F.to_date("event_timestamp").alias("_event_date"),
    )
    customers = dim_customers_df.select(
        F.col("customer_id").alias("_dimension_customer_id"),
        F.col("customer_key").alias("_resolved_customer_key"),
    )
    products = dim_products_df.select(
        F.col("product_id").alias("_dimension_product_id"),
        F.col("product_key").alias("_resolved_product_key"),
    )
    campaigns = dim_campaigns_df.select(
        F.col("campaign_id").alias("_dimension_campaign_id"),
        F.col("campaign_key").alias("_resolved_campaign_key"),
    )
    calendar = dim_calendar_df.select("date_day", "date_key")

    joined = (
        events.alias("event")
        .join(
            customers.alias("customer"),
            F.col("event.customer_id") == F.col("customer._dimension_customer_id"),
            "left",
        )
        .join(
            products.alias("product"),
            F.col("event.product_id") == F.col("product._dimension_product_id"),
            "left",
        )
        .join(
            campaigns.alias("campaign"),
            F.col("event.campaign_id") == F.col("campaign._dimension_campaign_id"),
            "left",
        )
        .join(
            calendar.alias("event_calendar"),
            F.col("event._event_date") == F.col("event_calendar.date_day"),
            "left",
        )
    )

    resolved = joined.select(
        "event.*",
        F.col("customer._resolved_customer_key").alias("_resolved_customer_key"),
        F.col("product._resolved_product_key").alias("_resolved_product_key"),
        F.col("campaign._resolved_campaign_key").alias("_resolved_campaign_key"),
        F.col("event_calendar.date_key").alias("_resolved_event_date_key"),
    )

    _validate_web_event_lookups(resolved)

    customer_key = (
        F.when(F.col("customer_id").isNull(), F.lit(ANONYMOUS_CUSTOMER_KEY))
        .when(F.col("_resolved_customer_key").isNull(), F.lit(UNKNOWN_KEY))
        .otherwise(F.col("_resolved_customer_key"))
        .cast("bigint")
    )
    product_key = (
        F.when(
            ~F.col("event_type").isin(PRODUCT_EVENT_TYPES),
            F.lit(NOT_APPLICABLE_PRODUCT_KEY),
        )
        .when(F.col("_resolved_product_key").isNull(), F.lit(UNKNOWN_KEY))
        .otherwise(F.col("_resolved_product_key"))
        .cast("bigint")
    )
    campaign_key = (
        F.when(F.col("campaign_id").isNull(), F.lit(NO_CAMPAIGN_KEY))
        .when(F.col("_resolved_campaign_key").isNull(), F.lit(UNKNOWN_KEY))
        .otherwise(F.col("_resolved_campaign_key"))
        .cast("bigint")
    )

    fact_df = resolved.select(
        surrogate_key("event_id").alias("event_key"),
        F.col("event_id"),
        F.col("session_id"),
        F.col("anonymous_id"),
        customer_key.alias("customer_key"),
        product_key.alias("product_key"),
        campaign_key.alias("campaign_key"),
        F.col("_resolved_event_date_key").alias("event_date_key"),
        F.col("event_type"),
        F.col("event_timestamp"),
        F.col("device_type"),
        F.col("country_code"),
        F.col("page_url"),
    )
    fact_df = with_gold_processed_at(fact_df)
    validate_fct_web_events(fact_df)
    return fact_df


def _validate_web_event_lookups(df: DataFrame) -> None:
    validate_rule(
        df,
        invalid_when=F.col("_event_date").isNull() | F.col("_resolved_event_date_key").isNull(),
        dataset_name="Web events",
        rule_description="have event dates missing from dim_calendar",
    )


def validate_fct_web_events(df: DataFrame) -> None:
    validate_required_values(
        df,
        required_columns=FCT_WEB_EVENTS_REQUIRED_COLUMNS,
        dataset_name="Web events",
    )
    validate_rule(
        df,
        invalid_when=~F.col("device_type").isin(DEVICE_TYPES),
        dataset_name="Web events",
        rule_description="have invalid device type values",
    )
    validate_unique_key(
        df,
        key_columns=["event_id"],
        dataset_name="Web events",
    )
    validate_unique_key(
        df,
        key_columns=["event_key"],
        dataset_name="Web events",
    )


def write_fct_web_events(fct_web_events_df: DataFrame, output_path: str | Path) -> None:
    write_gold(fct_web_events_df, output_path)


def write_fct_web_events_table(fct_web_events_df: DataFrame, output_table: str) -> None:
    write_gold_table(fct_web_events_df, output_table)


def run_fct_web_events(
    spark: SparkSession,
    *,
    web_events_input_path: str | Path,
    customers_input_path: str | Path,
    products_input_path: str | Path,
    campaigns_input_path: str | Path,
    calendar_input_path: str | Path,
    output_path: str | Path,
) -> None:
    fact_df = transform_fct_web_events(
        silver_web_events_df=read_delta(spark, web_events_input_path),
        dim_customers_df=read_delta(spark, customers_input_path),
        dim_products_df=read_delta(spark, products_input_path),
        dim_campaigns_df=read_delta(spark, campaigns_input_path),
        dim_calendar_df=read_delta(spark, calendar_input_path),
    )
    write_fct_web_events(fact_df, output_path)


def run_fct_web_events_tables(
    spark: SparkSession,
    *,
    web_events_input_table: str,
    customers_input_table: str,
    products_input_table: str,
    campaigns_input_table: str,
    calendar_input_table: str,
    output_table: str,
) -> None:
    fact_df = transform_fct_web_events(
        silver_web_events_df=read_delta_table(spark, web_events_input_table),
        dim_customers_df=read_delta_table(spark, customers_input_table),
        dim_products_df=read_delta_table(spark, products_input_table),
        dim_campaigns_df=read_delta_table(spark, campaigns_input_table),
        dim_calendar_df=read_delta_table(spark, calendar_input_table),
    )
    write_fct_web_events_table(fact_df, output_table)
