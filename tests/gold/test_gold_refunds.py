from datetime import datetime
from decimal import Decimal

import pytest
from helpers.gold import (
    calendar_dimension_df,
    campaign_dimension_df,
    currency_dimension_df,
    customer_dimension_df,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from orderflow.gold.common import GUEST_CUSTOMER_KEY, NO_CAMPAIGN_KEY, UNKNOWN_KEY
from orderflow.gold.fct_refunds import transform_fct_refunds

SILVER_REFUNDS_SCHEMA = T.StructType(
    [
        T.StructField("refund_id", T.StringType(), nullable=True),
        T.StructField("order_id", T.StringType(), nullable=True),
        T.StructField("payment_id", T.StringType(), nullable=True),
        T.StructField("currency", T.StringType(), nullable=True),
        T.StructField("created_at", T.TimestampType(), nullable=True),
        T.StructField("processed_at", T.TimestampType(), nullable=True),
        T.StructField("refund_reason", T.StringType(), nullable=True),
        T.StructField("refund_status", T.StringType(), nullable=True),
        T.StructField("refund_amount", T.DecimalType(18, 2), nullable=True),
        T.StructField("_source_file_name", T.StringType(), nullable=True),
    ]
)

SILVER_PAYMENTS_SCHEMA = T.StructType(
    [
        T.StructField("payment_id", T.StringType(), nullable=True),
        T.StructField("order_id", T.StringType(), nullable=True),
    ]
)

SILVER_ORDERS_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.StringType(), nullable=True),
        T.StructField("customer_id", T.StringType(), nullable=True),
        T.StructField("campaign_id", T.StringType(), nullable=True),
    ]
)

EXPECTED_GOLD_COLUMNS = [
    "refund_key",
    "refund_id",
    "order_id",
    "payment_id",
    "customer_key",
    "campaign_key",
    "currency_key",
    "refund_created_date_key",
    "refund_processed_date_key",
    "refund_reason",
    "refund_status",
    "refund_amount",
    "created_at",
    "processed_at",
    "_gold_processed_at",
]


def refund_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "refund_id": "ref_001",
        "order_id": "ord_001",
        "payment_id": "pay_001",
        "currency": "PLN",
        "created_at": datetime(2026, 6, 10, 12, 0),
        "processed_at": datetime(2026, 6, 11, 12, 0),
        "refund_reason": "customer_request",
        "refund_status": "processed",
        "refund_amount": Decimal("25.50"),
        "_source_file_name": "refunds.csv",
    }
    row.update(overrides)
    return row


def payment_row(
    payment_id: str = "pay_001",
    order_id: str = "ord_001",
) -> dict[str, object]:
    return {"payment_id": payment_id, "order_id": order_id}


def order_row(
    order_id: str = "ord_001",
    customer_id: str | None = "cust_001",
    campaign_id: str | None = "cmp_001",
) -> dict[str, object]:
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "campaign_id": campaign_id,
    }


def transform_refunds(
    spark: SparkSession,
    *,
    refunds: list[dict[str, object]] | None = None,
    payments: list[dict[str, object]] | None = None,
    orders: list[dict[str, object]] | None = None,
) -> DataFrame:
    return transform_fct_refunds(
        silver_refunds_df=spark.createDataFrame(
            refunds or [refund_row()],
            schema=SILVER_REFUNDS_SCHEMA,
        ),
        silver_payments_df=spark.createDataFrame(
            payments or [payment_row()],
            schema=SILVER_PAYMENTS_SCHEMA,
        ),
        silver_orders_df=spark.createDataFrame(
            orders or [order_row()],
            schema=SILVER_ORDERS_SCHEMA,
        ),
        dim_customers_df=customer_dimension_df(spark),
        dim_campaigns_df=campaign_dimension_df(spark),
        dim_currency_df=currency_dimension_df(spark),
        dim_calendar_df=calendar_dimension_df(spark),
    )


def test_transform_fct_refunds_matches_contract_and_enriches_order_context(
    spark: SparkSession,
) -> None:
    result_df = transform_refunds(spark)
    result = result_df.first()

    assert result is not None
    assert result_df.columns == EXPECTED_GOLD_COLUMNS
    assert isinstance(result["refund_key"], int)
    assert result["refund_key"] >= 0
    assert result["customer_key"] == 101
    assert result["campaign_key"] == 201
    assert result["currency_key"] == 401
    assert result["refund_created_date_key"] == 20260610
    assert result["refund_processed_date_key"] == 20260611
    assert result["refund_amount"] == Decimal("25.50")
    assert result["refund_status"] == "processed"
    assert result["_gold_processed_at"] is not None
    assert "_source_file_name" not in result_df.columns
    assert result_df.schema["refund_amount"].dataType == T.DecimalType(18, 2)


def test_transform_fct_refunds_routes_order_member_fallbacks(
    spark: SparkSession,
) -> None:
    result_df = transform_refunds(
        spark,
        refunds=[
            refund_row(),
            refund_row(refund_id="ref_002", order_id="ord_002", payment_id="pay_002"),
        ],
        payments=[payment_row(), payment_row("pay_002", "ord_002")],
        orders=[
            order_row(customer_id=None, campaign_id=None),
            order_row("ord_002", "missing_customer", "missing_campaign"),
        ],
    )
    rows = {row["refund_id"]: row for row in result_df.collect()}

    assert rows["ref_001"]["customer_key"] == GUEST_CUSTOMER_KEY
    assert rows["ref_001"]["campaign_key"] == NO_CAMPAIGN_KEY
    assert rows["ref_002"]["customer_key"] == UNKNOWN_KEY
    assert rows["ref_002"]["campaign_key"] == UNKNOWN_KEY


@pytest.mark.parametrize(
    ("scenario", "expected_message"),
    [
        ("payment", "unresolved payment references"),
        ("payment_order", "different order"),
        ("order", "unresolved order references"),
        ("currency", "unresolved currency references"),
        ("calendar", "unresolved processed dates"),
    ],
)
def test_transform_fct_refunds_rejects_missing_or_inconsistent_relationships(
    spark: SparkSession,
    scenario: str,
    expected_message: str,
) -> None:
    refunds = [refund_row()]
    payments = [payment_row()]
    orders = [order_row()]
    if scenario == "payment":
        refunds = [refund_row(payment_id="missing_payment")]
    elif scenario == "payment_order":
        payments = [payment_row(order_id="ord_002")]
    elif scenario == "order":
        refunds = [refund_row(order_id="missing_order")]
        payments = [payment_row(order_id="missing_order")]
    elif scenario == "currency":
        refunds = [refund_row(currency="USD")]
    else:
        refunds = [refund_row(processed_at=datetime(2026, 6, 16, 12, 0))]

    with pytest.raises(ValueError, match=expected_message):
        transform_refunds(
            spark,
            refunds=refunds,
            payments=payments,
            orders=orders,
        )


@pytest.mark.parametrize(
    "overrides",
    [
        {"refund_amount": Decimal("-0.01")},
        {"processed_at": datetime(2026, 6, 10, 11, 59)},
    ],
)
def test_transform_fct_refunds_rejects_invalid_measures_or_chronology(
    spark: SparkSession,
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="invalid refund measures or chronology"):
        transform_refunds(spark, refunds=[refund_row(**overrides)])


def test_transform_fct_refunds_rejects_null_required_value(
    spark: SparkSession,
) -> None:
    with pytest.raises(ValueError, match="null required fields"):
        transform_refunds(spark, refunds=[refund_row(refund_status=None)])


def test_transform_fct_refunds_rejects_duplicate_grain(
    spark: SparkSession,
) -> None:
    with pytest.raises(ValueError, match="duplicate refund_id values"):
        transform_refunds(
            spark,
            refunds=[refund_row(), refund_row(refund_reason="duplicate")],
        )
