"""Production data-quality contract for the OrderFlow Gold model."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from functools import reduce
from operator import or_

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from orderflow.gold.common import (
    ANONYMOUS_CUSTOMER_ID,
    ANONYMOUS_CUSTOMER_KEY,
    GUEST_CUSTOMER_ID,
    GUEST_CUSTOMER_KEY,
    NO_CAMPAIGN_ID,
    NO_CAMPAIGN_KEY,
    NOT_APPLICABLE_PRODUCT_ID,
    NOT_APPLICABLE_PRODUCT_KEY,
    UNKNOWN_KEY,
    UNKNOWN_MEMBER_ID,
)
from orderflow.gold.fct_payments import FAILURE_REASONS, PAYMENT_METHODS, PAYMENT_STATUSES
from orderflow.gold.fct_shipments import SHIPMENT_STATUSES
from orderflow.gold.fct_web_events import DEVICE_TYPES, PRODUCT_EVENT_TYPES
from orderflow.quality.checks import CheckResult, DataQualitySuite
from orderflow.silver.orders import ORDER_STATUSES

ORDERFLOW_GOLD_TABLES = (
    "dim_calendar",
    "dim_customers",
    "dim_products",
    "dim_campaigns",
    "dim_currency",
    "fct_orders",
    "fct_order_items",
    "fct_payments",
    "fct_refunds",
    "fct_shipments",
    "fct_web_events",
    "fct_exchange_rates",
)

REFUND_STATUSES = ("processed", "rejected")
EVENT_TYPES = (
    "page_view",
    "product_view",
    "add_to_cart",
    "checkout_started",
    "purchase_completed",
)
ORDER_SOURCE_CHANNELS = ("direct", "email", "organic", "paid_search", "social")
CAMPAIGN_SOURCE_CHANNELS = ("email", "paid_search", "social")

_CUSTOMER_SPECIAL_KEYS = (UNKNOWN_KEY, GUEST_CUSTOMER_KEY, ANONYMOUS_CUSTOMER_KEY)
_CAMPAIGN_SPECIAL_KEYS = (UNKNOWN_KEY, NO_CAMPAIGN_KEY)
_PRODUCT_SPECIAL_KEYS = (UNKNOWN_KEY, NOT_APPLICABLE_PRODUCT_KEY)

_GOLD_SCHEMAS: dict[str, dict[str, str]] = {
    "dim_calendar": {
        "date_key": "int",
        "date_day": "date",
        "year": "int",
        "quarter": "int",
        "month": "int",
        "month_name": "string",
        "day_of_month": "int",
        "day_of_week": "int",
        "day_name": "string",
        "week_of_year": "int",
        "is_weekend": "boolean",
        "is_polish_public_holiday": "boolean",
        "holiday_name": "string",
        "_gold_processed_at": "timestamp",
    },
    "dim_customers": {
        "customer_key": "bigint",
        "customer_id": "string",
        "email": "string",
        "email_domain": "string",
        "first_name": "string",
        "last_name": "string",
        "full_name": "string",
        "country_code": "string",
        "city": "string",
        "customer_status": "string",
        "is_active_customer": "boolean",
        "marketing_consent": "boolean",
        "registered_at": "timestamp",
        "registration_date": "date",
        "_gold_processed_at": "timestamp",
    },
    "dim_products": {
        "product_key": "bigint",
        "product_id": "string",
        "sku": "string",
        "product_name": "string",
        "category": "string",
        "brand": "string",
        "unit_price": "decimal(18,2)",
        "currency_code": "string",
        "is_active": "boolean",
        "created_at": "timestamp",
        "updated_at": "timestamp",
        "_gold_processed_at": "timestamp",
    },
    "dim_campaigns": {
        "campaign_key": "bigint",
        "campaign_id": "string",
        "campaign_name": "string",
        "source_channel": "string",
        "start_date": "date",
        "end_date": "date",
        "budget_amount": "decimal(18,2)",
        "budget_currency_code": "string",
        "is_active": "boolean",
        "created_at": "timestamp",
        "updated_at": "timestamp",
        "_gold_processed_at": "timestamp",
    },
    "dim_currency": {
        "currency_key": "bigint",
        "currency_code": "string",
        "is_reporting_currency": "boolean",
        "_gold_processed_at": "timestamp",
    },
    "fct_orders": {
        "order_key": "bigint",
        "order_id": "string",
        "customer_key": "bigint",
        "campaign_key": "bigint",
        "currency_key": "bigint",
        "order_date_key": "int",
        "order_created_at": "timestamp",
        "order_updated_at": "timestamp",
        "order_status": "string",
        "order_country_code": "string",
        "source_channel": "string",
        "gross_amount": "decimal(18,2)",
        "discount_amount": "decimal(18,2)",
        "net_amount": "decimal(18,2)",
        "_gold_processed_at": "timestamp",
    },
    "fct_order_items": {
        "order_item_key": "bigint",
        "order_item_id": "string",
        "order_id": "string",
        "customer_key": "bigint",
        "product_key": "bigint",
        "campaign_key": "bigint",
        "currency_key": "bigint",
        "order_date_key": "int",
        "order_item_created_date_key": "int",
        "order_country_code": "string",
        "order_item_created_at": "timestamp",
        "quantity": "int",
        "unit_price": "decimal(18,2)",
        "discount_amount": "decimal(18,2)",
        "gross_amount": "decimal(18,2)",
        "line_total": "decimal(18,2)",
        "_gold_processed_at": "timestamp",
    },
    "fct_payments": {
        "payment_key": "bigint",
        "payment_id": "string",
        "order_id": "string",
        "customer_key": "bigint",
        "campaign_key": "bigint",
        "currency_key": "bigint",
        "payment_created_date_key": "int",
        "payment_processed_date_key": "int",
        "payment_attempt_number": "int",
        "payment_method": "string",
        "payment_status": "string",
        "failure_reason": "string",
        "amount": "decimal(18,2)",
        "created_at": "timestamp",
        "processed_at": "timestamp",
        "_gold_processed_at": "timestamp",
    },
    "fct_refunds": {
        "refund_key": "bigint",
        "refund_id": "string",
        "order_id": "string",
        "payment_id": "string",
        "customer_key": "bigint",
        "campaign_key": "bigint",
        "currency_key": "bigint",
        "refund_created_date_key": "int",
        "refund_processed_date_key": "int",
        "refund_reason": "string",
        "refund_status": "string",
        "refund_amount": "decimal(18,2)",
        "created_at": "timestamp",
        "processed_at": "timestamp",
        "_gold_processed_at": "timestamp",
    },
    "fct_shipments": {
        "shipment_key": "bigint",
        "shipment_id": "string",
        "order_id": "string",
        "customer_key": "bigint",
        "campaign_key": "bigint",
        "shipped_date_key": "int",
        "estimated_delivery_date_key": "int",
        "delivered_date_key": "int",
        "carrier": "string",
        "shipment_status": "string",
        "shipped_at": "timestamp",
        "delivered_at": "timestamp",
        "delivery_country_code": "string",
        "delivery_city": "string",
        "shipping_cost": "decimal(18,2)",
        "_gold_processed_at": "timestamp",
    },
    "fct_web_events": {
        "event_key": "bigint",
        "event_id": "string",
        "session_id": "string",
        "anonymous_id": "string",
        "customer_key": "bigint",
        "product_key": "bigint",
        "campaign_key": "bigint",
        "event_date_key": "int",
        "event_type": "string",
        "event_timestamp": "timestamp",
        "device_type": "string",
        "country_code": "string",
        "page_url": "string",
        "_gold_processed_at": "timestamp",
    },
    "fct_exchange_rates": {
        "exchange_rate_key": "bigint",
        "rate_date_key": "int",
        "currency_key": "bigint",
        "rate_to_pln": "decimal(18,6)",
        "source": "string",
        "_gold_processed_at": "timestamp",
    },
}

_NON_NULL_COLUMNS: dict[str, tuple[str, ...]] = {
    table_name: tuple(
        column
        for column in schema
        if column
        not in {
            "holiday_name",
            "email",
            "email_domain",
            "first_name",
            "last_name",
            "full_name",
            "city",
            "customer_status",
            "registered_at",
            "registration_date",
            "category",
            "brand",
            "updated_at",
            "source_channel",
            "end_date",
            "order_updated_at",
            "failure_reason",
            "refund_reason",
            "estimated_delivery_date_key",
            "delivered_date_key",
            "delivered_at",
        }
    )
    for table_name, schema in _GOLD_SCHEMAS.items()
}
_NON_NULL_COLUMNS["fct_orders"] = tuple(
    column for column in _GOLD_SCHEMAS["fct_orders"] if column != "order_updated_at"
)


def build_orderflow_quality_suite(
    tables: Mapping[str, DataFrame],
    *,
    table_names: Sequence[str] | None = None,
) -> DataQualitySuite:
    """Build checks for all Gold tables, or a selected subset for focused tests."""
    selected = tuple(table_names or ORDERFLOW_GOLD_TABLES)
    unknown = sorted(set(selected) - set(ORDERFLOW_GOLD_TABLES))
    if unknown:
        raise ValueError(f"Unknown OrderFlow Gold quality tables: {unknown}")

    suite = DataQualitySuite(tables)
    for table_name in selected:
        if table_name not in tables:
            suite.record_result(
                CheckResult(
                    table_name=table_name,
                    check_name="table_exists",
                    status="FAIL",
                    violation_count=1,
                    details="expected Unity Catalog Gold table is missing",
                )
            )
            continue
        suite.record_result(
            CheckResult(
                table_name=table_name,
                check_name="table_exists",
                status="PASS",
                violation_count=0,
                details="Unity Catalog Gold table is available",
            )
        )
        _add_standard_checks(suite, table_name)
        _TABLE_BUILDERS[table_name](suite)

    _add_reference_checks(suite, tables, selected)
    _add_cross_table_checks(suite, tables, selected)
    return suite


def _add_standard_checks(suite: DataQualitySuite, table_name: str) -> None:
    schema = _GOLD_SCHEMAS[table_name]
    suite.check_required_columns(table_name, tuple(schema))
    suite.check_schema(table_name, schema)
    suite.check_non_empty(table_name)
    suite.check_non_null(table_name, _NON_NULL_COLUMNS[table_name])


def _add_dim_calendar_checks(suite: DataQualitySuite) -> None:
    table = "dim_calendar"
    suite.check_unique(table, ["date_key"])
    suite.check_unique(table, ["date_day"])
    suite.check_numeric_range(table, "quarter", minimum=1, maximum=4)
    suite.check_numeric_range(table, "month", minimum=1, maximum=12)
    suite.check_numeric_range(table, "day_of_month", minimum=1, maximum=31)
    suite.check_numeric_range(table, "day_of_week", minimum=1, maximum=7)
    suite.check_numeric_range(table, "week_of_year", minimum=1, maximum=53)
    suite.check_condition(
        table,
        check_name="calendar_attributes_match_date",
        required_columns=["date_key", "date_day", "year", "month", "day_of_month"],
        invalid_when=(F.col("date_key") != F.date_format("date_day", "yyyyMMdd").cast("int"))
        | (F.col("year") != F.year("date_day"))
        | (F.col("month") != F.month("date_day"))
        | (F.col("day_of_month") != F.dayofmonth("date_day")),
        expectation="date_key and core date attributes agree with date_day",
    )


def _add_dim_customers_checks(suite: DataQualitySuite) -> None:
    table = "dim_customers"
    regular = ~F.col("customer_key").isin(*_CUSTOMER_SPECIAL_KEYS)
    suite.check_unique(table, ["customer_key"])
    suite.check_unique(table, ["customer_id"])
    suite.check_values_present(
        table,
        "customer_id",
        [UNKNOWN_MEMBER_ID, GUEST_CUSTOMER_ID, ANONYMOUS_CUSTOMER_ID],
    )
    suite.check_accepted_values(
        table,
        "customer_status",
        ["active", "inactive"],
        where=regular,
    )
    suite.check_condition(
        table,
        check_name="regular_customer_descriptions_non_null",
        required_columns=[
            "customer_key",
            "email",
            "first_name",
            "last_name",
            "full_name",
            "customer_status",
            "registered_at",
            "registration_date",
        ],
        invalid_when=regular
        & _any_null(
            "email",
            "first_name",
            "last_name",
            "full_name",
            "customer_status",
            "registered_at",
            "registration_date",
        ),
        expectation="regular customers retain mandatory descriptive attributes",
    )
    suite.check_condition(
        table,
        check_name="valid_country_code",
        required_columns=["country_code"],
        invalid_when=~F.col("country_code").rlike(r"^[A-Z]{2}$"),
        expectation="country_code is an uppercase two-letter code",
    )
    suite.check_condition(
        table,
        check_name="active_flag_matches_status",
        required_columns=["customer_status", "is_active_customer"],
        invalid_when=F.coalesce(F.col("customer_status") == "active", F.lit(False))
        != F.col("is_active_customer"),
        expectation="is_active_customer agrees with customer_status",
    )
    suite.check_condition(
        table,
        check_name="special_customer_key_mapping",
        required_columns=["customer_id", "customer_key"],
        invalid_when=(
            (F.col("customer_id") == UNKNOWN_MEMBER_ID) & (F.col("customer_key") != UNKNOWN_KEY)
        )
        | (
            (F.col("customer_id") == GUEST_CUSTOMER_ID)
            & (F.col("customer_key") != GUEST_CUSTOMER_KEY)
        )
        | (
            (F.col("customer_id") == ANONYMOUS_CUSTOMER_ID)
            & (F.col("customer_key") != ANONYMOUS_CUSTOMER_KEY)
        ),
        expectation="special customer IDs use their documented negative keys",
    )


def _add_dim_products_checks(suite: DataQualitySuite) -> None:
    table = "dim_products"
    regular = ~F.col("product_key").isin(*_PRODUCT_SPECIAL_KEYS)
    suite.check_unique(table, ["product_key"])
    suite.check_unique(table, ["product_id"])
    suite.check_unique(table, ["sku"])
    suite.check_values_present(
        table,
        "product_id",
        [UNKNOWN_MEMBER_ID, NOT_APPLICABLE_PRODUCT_ID],
    )
    suite.check_numeric_range(table, "unit_price", minimum=0)
    suite.check_date_order(table, "created_at", "updated_at")
    suite.check_condition(
        table,
        check_name="regular_product_currency_code",
        required_columns=["product_key", "currency_code"],
        invalid_when=regular & ~F.col("currency_code").rlike(r"^[A-Z]{3}$"),
        expectation="regular product currency codes are uppercase ISO-style codes",
    )
    suite.check_condition(
        table,
        check_name="special_product_key_mapping",
        required_columns=["product_id", "product_key"],
        invalid_when=(
            (F.col("product_id") == UNKNOWN_MEMBER_ID) & (F.col("product_key") != UNKNOWN_KEY)
        )
        | (
            (F.col("product_id") == NOT_APPLICABLE_PRODUCT_ID)
            & (F.col("product_key") != NOT_APPLICABLE_PRODUCT_KEY)
        ),
        expectation="special product IDs use their documented negative keys",
    )


def _add_dim_campaigns_checks(suite: DataQualitySuite) -> None:
    table = "dim_campaigns"
    regular = ~F.col("campaign_key").isin(*_CAMPAIGN_SPECIAL_KEYS)
    suite.check_unique(table, ["campaign_key"])
    suite.check_unique(table, ["campaign_id"])
    suite.check_values_present(
        table,
        "campaign_id",
        [UNKNOWN_MEMBER_ID, NO_CAMPAIGN_ID],
    )
    suite.check_numeric_range(
        table,
        "budget_amount",
        minimum=0,
        include_minimum=False,
        where=regular,
    )
    suite.check_accepted_values(
        table,
        "source_channel",
        CAMPAIGN_SOURCE_CHANNELS,
        where=regular,
    )
    suite.check_date_order(table, "start_date", "end_date")
    suite.check_date_order(table, "created_at", "updated_at")
    suite.check_condition(
        table,
        check_name="regular_campaign_currency_code",
        required_columns=["campaign_key", "budget_currency_code"],
        invalid_when=regular & ~F.col("budget_currency_code").rlike(r"^[A-Z]{3}$"),
        expectation="regular campaign currency codes are uppercase ISO-style codes",
    )
    suite.check_condition(
        table,
        check_name="special_campaign_key_mapping",
        required_columns=["campaign_id", "campaign_key"],
        invalid_when=(
            (F.col("campaign_id") == UNKNOWN_MEMBER_ID) & (F.col("campaign_key") != UNKNOWN_KEY)
        )
        | ((F.col("campaign_id") == NO_CAMPAIGN_ID) & (F.col("campaign_key") != NO_CAMPAIGN_KEY)),
        expectation="special campaign IDs use their documented negative keys",
    )


def _add_dim_currency_checks(suite: DataQualitySuite) -> None:
    table = "dim_currency"
    suite.check_unique(table, ["currency_key"])
    suite.check_unique(table, ["currency_code"])
    suite.check_condition(
        table,
        check_name="valid_currency_code",
        required_columns=["currency_code"],
        invalid_when=~F.col("currency_code").rlike(r"^[A-Z]{3}$"),
        expectation="currency_code is an uppercase ISO-style code",
    )
    suite.check_condition(
        table,
        check_name="reporting_currency_flag",
        required_columns=["currency_code", "is_reporting_currency"],
        invalid_when=(F.col("currency_code") == "PLN") != F.col("is_reporting_currency"),
        expectation="only PLN is marked as the reporting currency",
    )
    suite.check_values_present(table, "currency_code", ["PLN"])


def _add_fct_orders_checks(suite: DataQualitySuite) -> None:
    table = "fct_orders"
    suite.check_unique(table, ["order_key"])
    suite.check_unique(table, ["order_id"])
    suite.check_accepted_values(table, "order_status", ORDER_STATUSES)
    suite.check_accepted_values(table, "source_channel", ORDER_SOURCE_CHANNELS)
    suite.check_numeric_range(table, "gross_amount", minimum=0)
    suite.check_numeric_range(table, "discount_amount", minimum=0)
    suite.check_numeric_range(table, "net_amount", minimum=0)
    suite.check_date_order(table, "order_created_at", "order_updated_at")
    suite.check_condition(
        table,
        check_name="order_amount_reconciliation",
        required_columns=["gross_amount", "discount_amount", "net_amount"],
        invalid_when=(F.col("discount_amount") > F.col("gross_amount"))
        | (
            F.col("net_amount")
            != (F.col("gross_amount") - F.col("discount_amount")).cast("decimal(18,2)")
        ),
        expectation="net amount equals gross less discount and discount does not exceed gross",
    )
    _add_country_check(suite, table, "order_country_code")
    _add_fallback_domain_checks(suite, table)


def _add_fct_order_items_checks(suite: DataQualitySuite) -> None:
    table = "fct_order_items"
    suite.check_unique(table, ["order_item_key"])
    suite.check_unique(table, ["order_item_id"])
    suite.check_numeric_range(table, "quantity", minimum=0, include_minimum=False)
    for column in ("unit_price", "discount_amount", "gross_amount", "line_total"):
        suite.check_numeric_range(table, column, minimum=0)
    suite.check_condition(
        table,
        check_name="order_item_amount_reconciliation",
        required_columns=[
            "quantity",
            "unit_price",
            "discount_amount",
            "gross_amount",
            "line_total",
        ],
        invalid_when=(
            F.col("gross_amount") != (F.col("quantity") * F.col("unit_price")).cast("decimal(18,2)")
        )
        | (F.col("discount_amount") > F.col("gross_amount"))
        | (
            F.col("line_total")
            != (F.col("gross_amount") - F.col("discount_amount")).cast("decimal(18,2)")
        ),
        expectation="item gross and line totals reconcile to quantity, price, and discount",
    )
    _add_country_check(suite, table, "order_country_code")
    _add_fallback_domain_checks(suite, table)


def _add_fct_payments_checks(suite: DataQualitySuite) -> None:
    table = "fct_payments"
    suite.check_unique(table, ["payment_key"])
    suite.check_unique(table, ["payment_id"])
    suite.check_accepted_values(table, "payment_method", PAYMENT_METHODS)
    suite.check_accepted_values(table, "payment_status", PAYMENT_STATUSES)
    suite.check_numeric_range(
        table,
        "payment_attempt_number",
        minimum=0,
        include_minimum=False,
    )
    suite.check_numeric_range(table, "amount", minimum=0)
    suite.check_date_order(table, "created_at", "processed_at")
    suite.check_condition(
        table,
        check_name="failure_reason_matches_status",
        required_columns=["payment_status", "failure_reason"],
        invalid_when=((F.col("payment_status") == "failed") & F.col("failure_reason").isNull())
        | ((F.col("payment_status") == "failed") & ~F.col("failure_reason").isin(*FAILURE_REASONS))
        | ((F.col("payment_status") != "failed") & F.col("failure_reason").isNotNull()),
        expectation="failed payments have an accepted reason and other payments have none",
    )
    _add_fallback_domain_checks(suite, table)


def _add_fct_refunds_checks(suite: DataQualitySuite) -> None:
    table = "fct_refunds"
    suite.check_unique(table, ["refund_key"])
    suite.check_unique(table, ["refund_id"])
    suite.check_accepted_values(table, "refund_status", REFUND_STATUSES)
    suite.check_numeric_range(table, "refund_amount", minimum=0)
    suite.check_date_order(table, "created_at", "processed_at")
    _add_fallback_domain_checks(suite, table)


def _add_fct_shipments_checks(suite: DataQualitySuite) -> None:
    table = "fct_shipments"
    suite.check_unique(table, ["shipment_key"])
    suite.check_unique(table, ["shipment_id"])
    suite.check_accepted_values(table, "shipment_status", SHIPMENT_STATUSES)
    suite.check_numeric_range(table, "shipping_cost", minimum=0)
    suite.check_date_order(table, "shipped_at", "delivered_at")
    suite.check_date_order(table, "shipped_date_key", "estimated_delivery_date_key")
    suite.check_date_order(table, "shipped_date_key", "delivered_date_key")
    suite.check_condition(
        table,
        check_name="delivered_timestamp_required",
        required_columns=["shipment_status", "delivered_at"],
        invalid_when=(F.col("shipment_status") == "delivered") & F.col("delivered_at").isNull(),
        expectation="delivered shipments have a delivered_at timestamp",
    )
    _add_country_check(suite, table, "delivery_country_code")
    _add_fallback_domain_checks(suite, table)


def _add_fct_web_events_checks(suite: DataQualitySuite) -> None:
    table = "fct_web_events"
    suite.check_unique(table, ["event_key"])
    suite.check_unique(table, ["event_id"])
    suite.check_accepted_values(table, "event_type", EVENT_TYPES)
    suite.check_accepted_values(table, "device_type", DEVICE_TYPES)
    suite.check_condition(
        table,
        check_name="product_event_key_applicable",
        required_columns=["event_type", "product_key"],
        invalid_when=F.col("event_type").isin(*PRODUCT_EVENT_TYPES)
        & (F.col("product_key") == NOT_APPLICABLE_PRODUCT_KEY),
        expectation="product events do not use the not-applicable product member",
    )
    _add_country_check(suite, table, "country_code")
    _add_fallback_domain_checks(suite, table)


def _add_fct_exchange_rates_checks(suite: DataQualitySuite) -> None:
    table = "fct_exchange_rates"
    suite.check_unique(table, ["exchange_rate_key"])
    suite.check_unique(table, ["rate_date_key", "currency_key", "source"])
    suite.check_numeric_range(
        table,
        "rate_to_pln",
        minimum=0,
        include_minimum=False,
    )
    suite.check_condition(
        table,
        check_name="non_blank_source",
        required_columns=["source"],
        invalid_when=F.trim(F.col("source")) == "",
        expectation="exchange-rate source is not blank",
    )
    _add_fallback_domain_checks(suite, table)


def _add_country_check(suite: DataQualitySuite, table: str, column: str) -> None:
    suite.check_condition(
        table,
        check_name=f"valid_{column}",
        required_columns=[column],
        invalid_when=~F.col(column).rlike(r"^[A-Z]{2}$"),
        expectation=f"{column} is an uppercase two-letter code",
    )


def _add_fallback_domain_checks(suite: DataQualitySuite, table: str) -> None:
    columns = set(_GOLD_SCHEMAS[table])
    if "customer_key" in columns:
        allowed = (
            (UNKNOWN_KEY, ANONYMOUS_CUSTOMER_KEY)
            if table == "fct_web_events"
            else (UNKNOWN_KEY, GUEST_CUSTOMER_KEY)
        )
        suite.check_condition(
            table,
            check_name="legal_customer_fallback_key",
            required_columns=["customer_key"],
            invalid_when=(F.col("customer_key") < 0) & ~F.col("customer_key").isin(*allowed),
            expectation=f"negative customer keys are limited to {list(allowed)}",
        )
    if "campaign_key" in columns:
        suite.check_condition(
            table,
            check_name="legal_campaign_fallback_key",
            required_columns=["campaign_key"],
            invalid_when=(F.col("campaign_key") < 0)
            & ~F.col("campaign_key").isin(UNKNOWN_KEY, NO_CAMPAIGN_KEY),
            expectation="negative campaign keys are limited to unknown and no-campaign",
        )
    if "product_key" in columns:
        allowed_products = (
            (UNKNOWN_KEY, NOT_APPLICABLE_PRODUCT_KEY) if table == "fct_web_events" else ()
        )
        invalid_product = F.col("product_key") < 0
        if allowed_products:
            invalid_product = invalid_product & ~F.col("product_key").isin(*allowed_products)
        suite.check_condition(
            table,
            check_name="legal_product_fallback_key",
            required_columns=["product_key"],
            invalid_when=invalid_product,
            expectation=(
                f"negative product keys are limited to {list(allowed_products)}"
                if allowed_products
                else "order items use resolved non-negative product keys"
            ),
        )
    if "currency_key" in columns:
        suite.check_numeric_range(table, "currency_key", minimum=0)


def _add_reference_checks(
    suite: DataQualitySuite,
    tables: Mapping[str, DataFrame],
    selected: Sequence[str],
) -> None:
    reference_contracts = {
        "fct_orders": [
            ("customer_key", "dim_customers", "customer_key"),
            ("campaign_key", "dim_campaigns", "campaign_key"),
            ("currency_key", "dim_currency", "currency_key"),
            ("order_date_key", "dim_calendar", "date_key"),
        ],
        "fct_order_items": [
            ("customer_key", "dim_customers", "customer_key"),
            ("product_key", "dim_products", "product_key"),
            ("campaign_key", "dim_campaigns", "campaign_key"),
            ("currency_key", "dim_currency", "currency_key"),
            ("order_date_key", "dim_calendar", "date_key"),
            ("order_item_created_date_key", "dim_calendar", "date_key"),
            ("order_id", "fct_orders", "order_id"),
        ],
        "fct_payments": [
            ("customer_key", "dim_customers", "customer_key"),
            ("campaign_key", "dim_campaigns", "campaign_key"),
            ("currency_key", "dim_currency", "currency_key"),
            ("payment_created_date_key", "dim_calendar", "date_key"),
            ("payment_processed_date_key", "dim_calendar", "date_key"),
            ("order_id", "fct_orders", "order_id"),
        ],
        "fct_refunds": [
            ("customer_key", "dim_customers", "customer_key"),
            ("campaign_key", "dim_campaigns", "campaign_key"),
            ("currency_key", "dim_currency", "currency_key"),
            ("refund_created_date_key", "dim_calendar", "date_key"),
            ("refund_processed_date_key", "dim_calendar", "date_key"),
            ("order_id", "fct_orders", "order_id"),
            ("payment_id", "fct_payments", "payment_id"),
        ],
        "fct_shipments": [
            ("customer_key", "dim_customers", "customer_key"),
            ("campaign_key", "dim_campaigns", "campaign_key"),
            ("shipped_date_key", "dim_calendar", "date_key"),
            ("estimated_delivery_date_key", "dim_calendar", "date_key"),
            ("delivered_date_key", "dim_calendar", "date_key"),
            ("order_id", "fct_orders", "order_id"),
        ],
        "fct_web_events": [
            ("customer_key", "dim_customers", "customer_key"),
            ("product_key", "dim_products", "product_key"),
            ("campaign_key", "dim_campaigns", "campaign_key"),
            ("event_date_key", "dim_calendar", "date_key"),
        ],
        "fct_exchange_rates": [
            ("currency_key", "dim_currency", "currency_key"),
            ("rate_date_key", "dim_calendar", "date_key"),
        ],
    }
    selected_set = set(selected)
    for source_table, contracts in reference_contracts.items():
        if source_table not in selected_set or source_table not in tables:
            continue
        for source_column, reference_table, reference_column in contracts:
            if reference_table not in tables:
                continue
            suite.check_reference(
                source_table,
                [source_column],
                reference_table,
                [reference_column],
                check_name=f"{source_column}_resolves_to_{reference_table}",
            )


def _add_cross_table_checks(
    suite: DataQualitySuite,
    tables: Mapping[str, DataFrame],
    selected: Sequence[str],
) -> None:
    selected_set = set(selected)
    if "fct_order_items" in selected_set and _has_contracts(
        tables, "fct_order_items", "fct_orders"
    ):
        _add_order_item_order_checks(suite, tables)
    if "fct_payments" in selected_set and _has_contracts(tables, "fct_payments", "fct_orders"):
        _add_payment_order_checks(suite, tables)
    if "fct_refunds" in selected_set and _has_contracts(
        tables, "fct_refunds", "fct_payments", "fct_orders"
    ):
        _add_refund_checks(suite, tables)
    if "fct_shipments" in selected_set and _has_contracts(tables, "fct_shipments", "fct_orders"):
        _add_shipment_order_checks(suite, tables)
    if "fct_orders" in selected_set and _has_contracts(
        tables, "fct_orders", "fct_order_items", "dim_currency"
    ):
        _add_pln_order_reconciliation(suite, tables)


def _add_order_item_order_checks(suite: DataQualitySuite, tables: Mapping[str, DataFrame]) -> None:
    name = "reconciliation.order_items_to_orders"
    joined = (
        tables["fct_order_items"]
        .alias("item")
        .join(
            tables["fct_orders"].select(
                F.col("order_id").alias("_order_id"),
                F.col("currency_key").alias("_order_currency_key"),
                F.col("order_created_at").alias("_order_created_at"),
            ),
            F.col("item.order_id") == F.col("_order_id"),
            "left",
        )
    )
    suite.add_table(name, joined)
    suite.check_condition(
        name,
        check_name="item_context_matches_order",
        required_columns=[
            "currency_key",
            "order_item_created_at",
            "_order_currency_key",
            "_order_created_at",
        ],
        invalid_when=(F.col("currency_key") != F.col("_order_currency_key"))
        | (F.col("order_item_created_at") < F.col("_order_created_at")),
        expectation="item currency and creation chronology agree with the order header",
    )


def _add_payment_order_checks(suite: DataQualitySuite, tables: Mapping[str, DataFrame]) -> None:
    name = "reconciliation.payments_to_orders"
    joined = (
        tables["fct_payments"]
        .alias("payment")
        .join(
            tables["fct_orders"].select(
                F.col("order_id").alias("_order_id"),
                F.col("currency_key").alias("_order_currency_key"),
                F.col("net_amount").alias("_order_net_amount"),
            ),
            F.col("payment.order_id") == F.col("_order_id"),
            "left",
        )
    )
    suite.add_table(name, joined)
    suite.check_condition(
        name,
        check_name="payment_within_order",
        required_columns=[
            "currency_key",
            "amount",
            "_order_currency_key",
            "_order_net_amount",
        ],
        invalid_when=(F.col("currency_key") != F.col("_order_currency_key"))
        | (F.col("amount") > F.col("_order_net_amount")),
        expectation="payment currency and amount are valid for its order",
    )


def _add_refund_checks(suite: DataQualitySuite, tables: Mapping[str, DataFrame]) -> None:
    name = "reconciliation.refunds_to_payments_and_orders"
    joined = (
        tables["fct_refunds"]
        .alias("refund")
        .join(
            tables["fct_payments"].select(
                F.col("payment_id").alias("_payment_id"),
                F.col("order_id").alias("_payment_order_id"),
                F.col("currency_key").alias("_payment_currency_key"),
                F.col("amount").alias("_payment_amount"),
                F.col("processed_at").alias("_payment_processed_at"),
            ),
            F.col("refund.payment_id") == F.col("_payment_id"),
            "left",
        )
        .join(
            tables["fct_orders"].select(
                F.col("order_id").alias("_order_id"),
                F.col("currency_key").alias("_order_currency_key"),
                F.col("net_amount").alias("_order_net_amount"),
            ),
            F.col("refund.order_id") == F.col("_order_id"),
            "left",
        )
    )
    suite.add_table(name, joined)
    suite.check_condition(
        name,
        check_name="refund_within_payment_and_order",
        required_columns=[
            "order_id",
            "currency_key",
            "refund_amount",
            "created_at",
            "_payment_order_id",
            "_payment_currency_key",
            "_payment_amount",
            "_payment_processed_at",
            "_order_currency_key",
            "_order_net_amount",
        ],
        invalid_when=(F.col("order_id") != F.col("_payment_order_id"))
        | (F.col("currency_key") != F.col("_payment_currency_key"))
        | (F.col("currency_key") != F.col("_order_currency_key"))
        | (F.col("refund_amount") > F.col("_payment_amount"))
        | (F.col("refund_amount") > F.col("_order_net_amount"))
        | (F.col("created_at") < F.col("_payment_processed_at")),
        expectation="refund lineage, currency, amount, and chronology agree with payment and order",
    )

    cumulative_name = "reconciliation.processed_refunds_to_payments"
    processed_refunds = (
        tables["fct_refunds"]
        .filter(F.col("refund_status") == "processed")
        .groupBy("payment_id")
        .agg(F.sum("refund_amount").alias("_processed_refund_amount"))
    )
    cumulative = processed_refunds.join(
        tables["fct_payments"].select(
            F.col("payment_id").alias("_payment_id"),
            F.col("amount").alias("_payment_amount"),
        ),
        F.col("payment_id") == F.col("_payment_id"),
        "left",
    )
    suite.add_table(cumulative_name, cumulative)
    suite.check_condition(
        cumulative_name,
        check_name="processed_refunds_do_not_exceed_payment",
        required_columns=["_processed_refund_amount", "_payment_amount"],
        invalid_when=F.col("_processed_refund_amount") > F.col("_payment_amount"),
        expectation="cumulative processed refunds do not exceed the referenced payment",
    )


def _add_shipment_order_checks(suite: DataQualitySuite, tables: Mapping[str, DataFrame]) -> None:
    name = "reconciliation.shipments_to_orders"
    joined = (
        tables["fct_shipments"]
        .alias("shipment")
        .join(
            tables["fct_orders"].select(
                F.col("order_id").alias("_order_id"),
                F.col("order_created_at").alias("_order_created_at"),
            ),
            F.col("shipment.order_id") == F.col("_order_id"),
            "left",
        )
    )
    suite.add_table(name, joined)
    suite.check_condition(
        name,
        check_name="shipment_not_before_order",
        required_columns=["shipped_at", "_order_created_at"],
        invalid_when=F.col("shipped_at") < F.col("_order_created_at"),
        expectation="shipment timestamps do not precede order creation",
    )


def _add_pln_order_reconciliation(suite: DataQualitySuite, tables: Mapping[str, DataFrame]) -> None:
    name = "reconciliation.pln_orders_to_items"
    item_totals = (
        tables["fct_order_items"]
        .groupBy("order_id")
        .agg(
            F.sum("gross_amount").cast("decimal(18,2)").alias("_item_gross_amount"),
            F.sum("discount_amount").cast("decimal(18,2)").alias("_item_discount_amount"),
            F.sum("line_total").cast("decimal(18,2)").alias("_item_net_amount"),
        )
    )
    currency = tables["dim_currency"].select(
        F.col("currency_key").alias("_currency_key"),
        "currency_code",
    )
    reconciled = (
        tables["fct_orders"]
        .join(currency, F.col("currency_key") == F.col("_currency_key"), "left")
        .join(item_totals, "order_id", "left")
        .filter(F.col("currency_code") == "PLN")
    )
    suite.add_table(name, reconciled)
    suite.check_condition(
        name,
        check_name="pln_order_totals_match_items",
        required_columns=[
            "gross_amount",
            "discount_amount",
            "net_amount",
            "_item_gross_amount",
            "_item_discount_amount",
            "_item_net_amount",
        ],
        invalid_when=F.col("_item_gross_amount").isNull()
        | (F.col("gross_amount") != F.col("_item_gross_amount"))
        | (F.col("discount_amount") != F.col("_item_discount_amount"))
        | (F.col("net_amount") != F.col("_item_net_amount")),
        expectation="PLN order header totals exactly reconcile to item totals",
    )


def _any_null(*columns: str) -> Column:
    return reduce(or_, (F.col(column).isNull() for column in columns))


def _has_contracts(tables: Mapping[str, DataFrame], *names: str) -> bool:
    return all(
        name in tables and set(_GOLD_SCHEMAS[name]) <= set(tables[name].columns) for name in names
    )


_TABLE_BUILDERS = {
    "dim_calendar": _add_dim_calendar_checks,
    "dim_customers": _add_dim_customers_checks,
    "dim_products": _add_dim_products_checks,
    "dim_campaigns": _add_dim_campaigns_checks,
    "dim_currency": _add_dim_currency_checks,
    "fct_orders": _add_fct_orders_checks,
    "fct_order_items": _add_fct_order_items_checks,
    "fct_payments": _add_fct_payments_checks,
    "fct_refunds": _add_fct_refunds_checks,
    "fct_shipments": _add_fct_shipments_checks,
    "fct_web_events": _add_fct_web_events_checks,
    "fct_exchange_rates": _add_fct_exchange_rates_checks,
}

__all__ = ["ORDERFLOW_GOLD_TABLES", "build_orderflow_quality_suite"]
