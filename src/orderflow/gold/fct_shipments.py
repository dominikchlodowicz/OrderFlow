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

SHIPMENT_STATUSES = ["lost", "shipped", "delivered", "returned"]

SILVER_SHIPMENTS_REQUIRED_COLUMNS = [
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
]
SILVER_ORDERS_REQUIRED_COLUMNS = ["order_id", "customer_id", "campaign_id"]
DIM_CUSTOMERS_REQUIRED_COLUMNS = ["customer_key", "customer_id"]
DIM_CAMPAIGNS_REQUIRED_COLUMNS = ["campaign_key", "campaign_id"]
DIM_CALENDAR_REQUIRED_COLUMNS = ["date_key", "date_day"]

FCT_SHIPMENTS_REQUIRED_COLUMNS = [
    "shipment_key",
    "shipment_id",
    "order_id",
    "customer_key",
    "campaign_key",
    "shipped_date_key",
    "carrier",
    "shipment_status",
    "shipped_at",
    "delivery_country_code",
    "delivery_city",
    "shipping_cost",
    "_gold_processed_at",
]


def transform_fct_shipments(
    *,
    silver_shipments_df: DataFrame,
    silver_orders_df: DataFrame,
    dim_customers_df: DataFrame,
    dim_campaigns_df: DataFrame,
    dim_calendar_df: DataFrame,
) -> DataFrame:
    """Build the current shipment fact and enrich it from accepted orders."""
    validate_required_columns(
        silver_shipments_df,
        SILVER_SHIPMENTS_REQUIRED_COLUMNS,
        dataset_name="Silver shipments",
    )
    validate_required_columns(
        silver_orders_df,
        SILVER_ORDERS_REQUIRED_COLUMNS,
        dataset_name="Silver orders",
    )
    validate_required_columns(
        dim_customers_df,
        DIM_CUSTOMERS_REQUIRED_COLUMNS,
        dataset_name="Gold dim_customers",
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

    shipments = silver_shipments_df.select(
        *SILVER_SHIPMENTS_REQUIRED_COLUMNS,
        F.to_date("shipped_at").alias("_shipped_date"),
        F.col("estimated_delivery_at").alias("_estimated_delivery_date"),
        F.to_date("delivered_at").alias("_delivered_date"),
    )
    orders = silver_orders_df.select(
        F.col("order_id").alias("_dimension_order_id"),
        F.col("customer_id").alias("_order_customer_id"),
        F.col("campaign_id").alias("_order_campaign_id"),
    )
    customers = dim_customers_df.select(
        F.col("customer_id").alias("_dimension_customer_id"),
        F.col("customer_key").alias("_resolved_customer_key"),
    )
    campaigns = dim_campaigns_df.select(
        F.col("campaign_id").alias("_dimension_campaign_id"),
        F.col("campaign_key").alias("_resolved_campaign_key"),
    )
    calendar = dim_calendar_df.select("date_day", "date_key")

    joined = (
        shipments.alias("shipment")
        .join(
            orders.alias("order"),
            F.col("shipment.order_id") == F.col("order._dimension_order_id"),
            "left",
        )
        .join(
            customers.alias("customer"),
            F.col("order._order_customer_id") == F.col("customer._dimension_customer_id"),
            "left",
        )
        .join(
            campaigns.alias("campaign"),
            F.col("order._order_campaign_id") == F.col("campaign._dimension_campaign_id"),
            "left",
        )
        .join(
            calendar.alias("shipped_calendar"),
            F.col("shipment._shipped_date") == F.col("shipped_calendar.date_day"),
            "left",
        )
        .join(
            calendar.alias("estimated_calendar"),
            F.col("shipment._estimated_delivery_date") == F.col("estimated_calendar.date_day"),
            "left",
        )
        .join(
            calendar.alias("delivered_calendar"),
            F.col("shipment._delivered_date") == F.col("delivered_calendar.date_day"),
            "left",
        )
    )

    resolved = joined.select(
        "shipment.*",
        F.col("order._dimension_order_id").alias("_matched_order_id"),
        F.col("order._order_customer_id").alias("_order_customer_id"),
        F.col("order._order_campaign_id").alias("_order_campaign_id"),
        F.col("customer._resolved_customer_key").alias("_resolved_customer_key"),
        F.col("campaign._resolved_campaign_key").alias("_resolved_campaign_key"),
        F.col("shipped_calendar.date_key").alias("_resolved_shipped_date_key"),
        F.col("estimated_calendar.date_key").alias("_resolved_estimated_delivery_date_key"),
        F.col("delivered_calendar.date_key").alias("_resolved_delivered_date_key"),
    )

    _validate_shipment_lookups(resolved)

    customer_key = (
        F.when(F.col("_order_customer_id").isNull(), F.lit(GUEST_CUSTOMER_KEY))
        .when(F.col("_resolved_customer_key").isNull(), F.lit(UNKNOWN_KEY))
        .otherwise(F.col("_resolved_customer_key"))
        .cast("bigint")
    )
    campaign_key = (
        F.when(F.col("_order_campaign_id").isNull(), F.lit(NO_CAMPAIGN_KEY))
        .when(F.col("_resolved_campaign_key").isNull(), F.lit(UNKNOWN_KEY))
        .otherwise(F.col("_resolved_campaign_key"))
        .cast("bigint")
    )

    fact_df = resolved.select(
        surrogate_key("shipment_id").alias("shipment_key"),
        F.col("shipment_id"),
        F.col("order_id"),
        customer_key.alias("customer_key"),
        campaign_key.alias("campaign_key"),
        F.col("_resolved_shipped_date_key").alias("shipped_date_key"),
        F.col("_resolved_estimated_delivery_date_key").alias("estimated_delivery_date_key"),
        F.col("_resolved_delivered_date_key").alias("delivered_date_key"),
        F.col("carrier"),
        F.col("shipment_status"),
        F.col("shipped_at"),
        F.col("delivered_at"),
        F.col("delivery_country").alias("delivery_country_code"),
        F.col("delivery_city"),
        F.col("shipping_cost"),
    )
    fact_df = with_gold_processed_at(fact_df)
    validate_fct_shipments(fact_df)
    return fact_df


def _validate_shipment_lookups(df: DataFrame) -> None:
    validate_rule(
        df,
        invalid_when=F.col("_matched_order_id").isNull(),
        dataset_name="Shipments",
        rule_description="reference missing accepted orders",
    )
    validate_rule(
        df,
        invalid_when=F.col("_shipped_date").isNull() | F.col("_resolved_shipped_date_key").isNull(),
        dataset_name="Shipments",
        rule_description="have shipped dates missing from dim_calendar",
    )
    validate_rule(
        df,
        invalid_when=F.col("_estimated_delivery_date").isNotNull()
        & F.col("_resolved_estimated_delivery_date_key").isNull(),
        dataset_name="Shipments",
        rule_description="have estimated delivery dates missing from dim_calendar",
    )
    validate_rule(
        df,
        invalid_when=F.col("_delivered_date").isNotNull()
        & F.col("_resolved_delivered_date_key").isNull(),
        dataset_name="Shipments",
        rule_description="have delivered dates missing from dim_calendar",
    )


def validate_fct_shipments(df: DataFrame) -> None:
    validate_required_values(
        df,
        required_columns=FCT_SHIPMENTS_REQUIRED_COLUMNS,
        dataset_name="Shipments",
    )
    validate_rule(
        df,
        invalid_when=(~F.col("shipment_status").isin(SHIPMENT_STATUSES))
        | (F.col("shipping_cost") < 0),
        dataset_name="Shipments",
        rule_description="have invalid shipment status or cost values",
    )
    validate_rule(
        df,
        invalid_when=((F.col("shipment_status") == "delivered") & F.col("delivered_at").isNull())
        | (F.col("delivered_at").isNotNull() & (F.col("delivered_at") < F.col("shipped_at"))),
        dataset_name="Shipments",
        rule_description="have invalid delivered_at values",
    )
    validate_unique_key(
        df,
        key_columns=["shipment_id"],
        dataset_name="Shipments",
    )
    validate_unique_key(
        df,
        key_columns=["shipment_key"],
        dataset_name="Shipments",
    )


def write_fct_shipments(fct_shipments_df: DataFrame, output_path: str | Path) -> None:
    write_gold(fct_shipments_df, output_path)


def write_fct_shipments_table(fct_shipments_df: DataFrame, output_table: str) -> None:
    write_gold_table(fct_shipments_df, output_table)


def run_fct_shipments(
    spark: SparkSession,
    *,
    shipments_input_path: str | Path,
    orders_input_path: str | Path,
    customers_input_path: str | Path,
    campaigns_input_path: str | Path,
    calendar_input_path: str | Path,
    output_path: str | Path,
) -> None:
    fact_df = transform_fct_shipments(
        silver_shipments_df=read_delta(spark, shipments_input_path),
        silver_orders_df=read_delta(spark, orders_input_path),
        dim_customers_df=read_delta(spark, customers_input_path),
        dim_campaigns_df=read_delta(spark, campaigns_input_path),
        dim_calendar_df=read_delta(spark, calendar_input_path),
    )
    write_fct_shipments(fact_df, output_path)


def run_fct_shipments_tables(
    spark: SparkSession,
    *,
    shipments_input_table: str,
    orders_input_table: str,
    customers_input_table: str,
    campaigns_input_table: str,
    calendar_input_table: str,
    output_table: str,
) -> None:
    fact_df = transform_fct_shipments(
        silver_shipments_df=read_delta_table(spark, shipments_input_table),
        silver_orders_df=read_delta_table(spark, orders_input_table),
        dim_customers_df=read_delta_table(spark, customers_input_table),
        dim_campaigns_df=read_delta_table(spark, campaigns_input_table),
        dim_calendar_df=read_delta_table(spark, calendar_input_table),
    )
    write_fct_shipments_table(fact_df, output_table)
