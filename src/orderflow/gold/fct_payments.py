"""Gold current-state payment fact transformation and orchestration."""

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

PAYMENT_METHODS = [
    "card",
    "paypal",
    "blik",
    "bank_transfer",
    "cash_on_delivery",
    "online_installments",
]
PAYMENT_STATUSES = ["authorized", "captured", "failed", "refunded"]
FAILURE_REASONS = ["timeout", "insufficient_funds", "card_declined"]

PAYMENTS_REQUIRED_COLUMNS = [
    "payment_id",
    "order_id",
    "currency",
    "created_at",
    "processed_at",
    "payment_attempt_number",
    "payment_method",
    "payment_status",
    "failure_reason",
    "amount",
]

ORDER_CONTEXT_REQUIRED_COLUMNS = ["order_id", "customer_id", "campaign_id"]


def _validate_payment_inputs(
    *,
    silver_orders_df: DataFrame,
    dim_customers_df: DataFrame,
    dim_campaigns_df: DataFrame,
    dim_currency_df: DataFrame,
    dim_calendar_df: DataFrame,
) -> None:
    validate_required_columns(
        silver_orders_df,
        ORDER_CONTEXT_REQUIRED_COLUMNS,
        dataset_name="Silver orders",
    )
    validate_required_values(
        silver_orders_df,
        required_columns=["order_id"],
        dataset_name="Silver orders",
    )
    validate_unique_key(
        silver_orders_df,
        key_columns=["order_id"],
        dataset_name="Silver orders",
    )

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


def transform_fct_payments(
    *,
    silver_payments_df: DataFrame,
    silver_orders_df: DataFrame,
    dim_customers_df: DataFrame,
    dim_campaigns_df: DataFrame,
    dim_currency_df: DataFrame,
    dim_calendar_df: DataFrame,
) -> DataFrame:
    """Enrich accepted Silver payments with order and dimension context."""
    validate_required_columns(
        silver_payments_df,
        PAYMENTS_REQUIRED_COLUMNS,
        dataset_name="Silver payments",
    )
    _validate_payment_inputs(
        silver_orders_df=silver_orders_df,
        dim_customers_df=dim_customers_df,
        dim_campaigns_df=dim_campaigns_df,
        dim_currency_df=dim_currency_df,
        dim_calendar_df=dim_calendar_df,
    )

    order_lookup = silver_orders_df.select(
        F.col("order_id").alias("_header_order_id"),
        F.col("customer_id").alias("_header_customer_id"),
        F.col("campaign_id").alias("_header_campaign_id"),
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
    created_calendar_lookup = dim_calendar_df.select(
        F.col("date_day").alias("_created_calendar_date_day"),
        F.col("date_key").alias("_resolved_created_date_key"),
    )
    processed_calendar_lookup = dim_calendar_df.select(
        F.col("date_day").alias("_processed_calendar_date_day"),
        F.col("date_key").alias("_resolved_processed_date_key"),
    )

    enriched_df = (
        silver_payments_df.join(
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
            campaign_lookup,
            F.col("_header_campaign_id") == F.col("_campaign_lookup_id"),
            "left",
        )
        .join(
            currency_lookup,
            F.col("currency") == F.col("_currency_lookup_code"),
            "left",
        )
        .join(
            created_calendar_lookup,
            F.to_date(F.col("created_at")) == F.col("_created_calendar_date_day"),
            "left",
        )
        .join(
            processed_calendar_lookup,
            F.to_date(F.col("processed_at")) == F.col("_processed_calendar_date_day"),
            "left",
        )
    )

    relationship_rules = [
        (F.col("_header_order_id").isNull(), "have unresolved order references"),
        (F.col("_resolved_currency_key").isNull(), "have unresolved currency references"),
        (F.col("_resolved_created_date_key").isNull(), "have unresolved created dates"),
        (F.col("_resolved_processed_date_key").isNull(), "have unresolved processed dates"),
    ]
    for invalid_when, rule_description in relationship_rules:
        validate_rule(
            enriched_df,
            invalid_when=invalid_when,
            dataset_name="fct_payments",
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
    payment_method = (
        F.when(F.col("payment_method") == "on delivery", F.lit("cash_on_delivery"))
        .when(
            F.col("payment_method") == "online installments",
            F.lit("online_installments"),
        )
        .otherwise(F.col("payment_method"))
    )

    fct_payments_df = enriched_df.select(
        surrogate_key("payment_id").alias("payment_key"),
        F.col("payment_id"),
        F.col("order_id"),
        customer_key.alias("customer_key"),
        campaign_key.alias("campaign_key"),
        F.col("_resolved_currency_key").alias("currency_key"),
        F.col("_resolved_created_date_key").alias("payment_created_date_key"),
        F.col("_resolved_processed_date_key").alias("payment_processed_date_key"),
        F.col("payment_attempt_number"),
        payment_method.alias("payment_method"),
        F.col("payment_status"),
        F.col("failure_reason"),
        F.col("amount"),
        F.col("created_at"),
        F.col("processed_at"),
    )

    fct_payments_df = with_gold_processed_at(fct_payments_df)
    validate_fct_payments(fct_payments_df)
    return fct_payments_df


def validate_fct_payments(df: DataFrame) -> None:
    dataset_name = "fct_payments"
    validate_required_values(
        df,
        required_columns=[
            "payment_key",
            "payment_id",
            "order_id",
            "customer_key",
            "campaign_key",
            "currency_key",
            "payment_created_date_key",
            "payment_processed_date_key",
            "payment_attempt_number",
            "payment_method",
            "payment_status",
            "amount",
            "created_at",
            "processed_at",
            "_gold_processed_at",
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
        invalid_when=((F.col("payment_status") == "failed") & F.col("failure_reason").isNull())
        | ((F.col("payment_status") == "failed") & ~F.col("failure_reason").isin(FAILURE_REASONS))
        | ((F.col("payment_status") != "failed") & F.col("failure_reason").isNotNull()),
        dataset_name=dataset_name,
        rule_description="have failure reasons inconsistent with payment status",
    )
    validate_rule(
        df,
        invalid_when=(F.col("payment_attempt_number") <= 0)
        | (F.col("amount") < 0)
        | (F.col("processed_at") < F.col("created_at")),
        dataset_name=dataset_name,
        rule_description="have invalid payment measures or chronology",
    )
    validate_unique_key(
        df,
        key_columns=["payment_id"],
        dataset_name=dataset_name,
    )
    validate_unique_key(
        df,
        key_columns=["payment_key"],
        dataset_name=dataset_name,
    )


def write_fct_payments(fct_payments_df: DataFrame, output_path: str | Path) -> None:
    write_gold(fct_payments_df, output_path)


def write_fct_payments_table(fct_payments_df: DataFrame, output_table: str) -> None:
    write_gold_table(fct_payments_df, output_table)


def run_fct_payments(
    spark: SparkSession,
    *,
    payments_input_path: str | Path,
    orders_input_path: str | Path,
    customers_input_path: str | Path,
    campaigns_input_path: str | Path,
    currency_input_path: str | Path,
    calendar_input_path: str | Path,
    output_path: str | Path,
) -> None:
    fct_payments_df = transform_fct_payments(
        silver_payments_df=read_delta(spark, payments_input_path),
        silver_orders_df=read_delta(spark, orders_input_path),
        dim_customers_df=read_delta(spark, customers_input_path),
        dim_campaigns_df=read_delta(spark, campaigns_input_path),
        dim_currency_df=read_delta(spark, currency_input_path),
        dim_calendar_df=read_delta(spark, calendar_input_path),
    )
    write_fct_payments(fct_payments_df, output_path)


def run_fct_payments_tables(
    spark: SparkSession,
    *,
    payments_input_table: str,
    orders_input_table: str,
    customers_input_table: str,
    campaigns_input_table: str,
    currency_input_table: str,
    calendar_input_table: str,
    output_table: str,
) -> None:
    fct_payments_df = transform_fct_payments(
        silver_payments_df=read_delta_table(spark, payments_input_table),
        silver_orders_df=read_delta_table(spark, orders_input_table),
        dim_customers_df=read_delta_table(spark, customers_input_table),
        dim_campaigns_df=read_delta_table(spark, campaigns_input_table),
        dim_currency_df=read_delta_table(spark, currency_input_table),
        dim_calendar_df=read_delta_table(spark, calendar_input_table),
    )
    write_fct_payments_table(fct_payments_df, output_table)
