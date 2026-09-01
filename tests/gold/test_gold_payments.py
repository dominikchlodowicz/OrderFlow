from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest
from helpers.gold import (
    calendar_dimension_df,
    campaign_dimension_df,
    currency_dimension_df,
    customer_dimension_df,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from orderflow.gold.common import (
    GUEST_CUSTOMER_KEY,
    NO_CAMPAIGN_KEY,
    UNKNOWN_KEY,
)
from orderflow.gold.fct_payments import run_fct_payments, transform_fct_payments

SILVER_PAYMENTS_SCHEMA = T.StructType(
    [
        T.StructField("payment_id", T.StringType(), nullable=True),
        T.StructField("order_id", T.StringType(), nullable=True),
        T.StructField("currency", T.StringType(), nullable=True),
        T.StructField("created_at", T.TimestampType(), nullable=True),
        T.StructField("processed_at", T.TimestampType(), nullable=True),
        T.StructField("payment_attempt_number", T.IntegerType(), nullable=True),
        T.StructField("payment_method", T.StringType(), nullable=True),
        T.StructField("payment_status", T.StringType(), nullable=True),
        T.StructField("failure_reason", T.StringType(), nullable=True),
        T.StructField("amount", T.DecimalType(18, 2), nullable=True),
        T.StructField("_source_file_name", T.StringType(), nullable=True),
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
    "failure_reason",
    "amount",
    "created_at",
    "processed_at",
    "_gold_processed_at",
]

EXPECTED_GOLD_TYPES = {
    "payment_key": T.LongType(),
    "payment_id": T.StringType(),
    "order_id": T.StringType(),
    "customer_key": T.LongType(),
    "campaign_key": T.LongType(),
    "currency_key": T.LongType(),
    "payment_created_date_key": T.IntegerType(),
    "payment_processed_date_key": T.IntegerType(),
    "payment_attempt_number": T.IntegerType(),
    "payment_method": T.StringType(),
    "payment_status": T.StringType(),
    "failure_reason": T.StringType(),
    "amount": T.DecimalType(18, 2),
    "created_at": T.TimestampType(),
    "processed_at": T.TimestampType(),
    "_gold_processed_at": T.TimestampType(),
}


def payment_row(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "payment_id": "pay_001",
        "order_id": "ord_001",
        "currency": "PLN",
        "created_at": datetime(2026, 6, 10, 10, 0),
        "processed_at": datetime(2026, 6, 11, 11, 0),
        "payment_attempt_number": 1,
        "payment_method": "on delivery",
        "payment_status": "captured",
        "failure_reason": None,
        "amount": Decimal("90.00"),
        "_source_file_name": "payments.csv",
    }
    values.update(overrides)
    return values


def order_row(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "order_id": "ord_001",
        "customer_id": "cust_001",
        "campaign_id": "cmp_001",
    }
    values.update(overrides)
    return values


def create_payments_df(
    spark: SparkSession,
    rows: list[dict[str, object]],
) -> DataFrame:
    return spark.createDataFrame(rows, schema=SILVER_PAYMENTS_SCHEMA)


def create_orders_df(
    spark: SparkSession,
    rows: list[dict[str, object]],
) -> DataFrame:
    return spark.createDataFrame(rows, schema=SILVER_ORDERS_SCHEMA)


def transform_payments(
    spark: SparkSession,
    *,
    payments: list[dict[str, object]] | None = None,
    orders: list[dict[str, object]] | None = None,
    currency_df: DataFrame | None = None,
    calendar_df: DataFrame | None = None,
) -> DataFrame:
    return transform_fct_payments(
        silver_payments_df=create_payments_df(spark, payments or [payment_row()]),
        silver_orders_df=create_orders_df(spark, orders or [order_row()]),
        dim_customers_df=customer_dimension_df(spark),
        dim_campaigns_df=campaign_dimension_df(spark),
        dim_currency_df=currency_df or currency_dimension_df(spark),
        dim_calendar_df=calendar_df or calendar_dimension_df(spark),
    )


def test_transform_fct_payments_matches_contract_and_enriches_order_context(
    spark: SparkSession,
) -> None:
    result_df = transform_payments(
        spark,
        payments=[
            payment_row(),
            payment_row(
                payment_id="pay_002",
                payment_method="online installments",
                payment_status="refunded",
            ),
        ],
    )
    rows = {row["payment_id"]: row for row in result_df.collect()}

    assert result_df.columns == EXPECTED_GOLD_COLUMNS
    assert {field.name: field.dataType for field in result_df.schema} == EXPECTED_GOLD_TYPES
    assert rows["pay_001"]["customer_key"] == 101
    assert rows["pay_001"]["campaign_key"] == 201
    assert rows["pay_001"]["currency_key"] == 401
    assert rows["pay_001"]["payment_created_date_key"] == 20260610
    assert rows["pay_001"]["payment_processed_date_key"] == 20260611
    assert rows["pay_001"]["payment_method"] == "cash_on_delivery"
    assert rows["pay_002"]["payment_method"] == "online_installments"
    assert rows["pay_002"]["payment_status"] == "refunded"
    assert all(row["payment_key"] >= 0 for row in rows.values())
    assert all(row["_gold_processed_at"] is not None for row in rows.values())
    assert "_source_file_name" not in result_df.columns


def test_transform_fct_payments_uses_guest_and_unknown_fallbacks(
    spark: SparkSession,
) -> None:
    result_df = transform_payments(
        spark,
        payments=[
            payment_row(),
            payment_row(payment_id="pay_002", order_id="ord_002"),
        ],
        orders=[
            order_row(customer_id=None, campaign_id=None),
            order_row(
                order_id="ord_002",
                customer_id="missing_customer",
                campaign_id="missing_campaign",
            ),
        ],
    )
    rows = {row["payment_id"]: row for row in result_df.collect()}

    assert rows["pay_001"]["customer_key"] == GUEST_CUSTOMER_KEY
    assert rows["pay_001"]["campaign_key"] == NO_CAMPAIGN_KEY
    assert rows["pay_002"]["customer_key"] == UNKNOWN_KEY
    assert rows["pay_002"]["campaign_key"] == UNKNOWN_KEY


def test_transform_fct_payments_accepts_failed_payment_with_reason(
    spark: SparkSession,
) -> None:
    result = transform_payments(
        spark,
        payments=[
            payment_row(
                payment_status="failed",
                failure_reason="card_declined",
                payment_method="card",
            )
        ],
    ).first()

    assert result is not None
    assert result["payment_status"] == "failed"
    assert result["failure_reason"] == "card_declined"


@pytest.mark.parametrize(
    "overrides",
    [
        {"payment_status": "failed", "failure_reason": None},
        {"payment_status": "captured", "failure_reason": "timeout"},
    ],
)
def test_transform_fct_payments_rejects_inconsistent_failure_reason(
    spark: SparkSession,
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="failure reasons inconsistent"):
        transform_payments(spark, payments=[payment_row(**overrides)])


@pytest.mark.parametrize(
    ("scenario", "expected_message"),
    [
        ("order", "unresolved order references"),
        ("currency", "unresolved currency references"),
        ("calendar", "unresolved processed dates"),
    ],
)
def test_transform_fct_payments_rejects_missing_required_relationships(
    spark: SparkSession,
    scenario: str,
    expected_message: str,
) -> None:
    payments = [payment_row()]
    orders = [order_row()]
    if scenario == "order":
        payments = [payment_row(order_id="missing_order")]
    elif scenario == "currency":
        payments = [payment_row(currency="USD")]
    else:
        payments = [payment_row(processed_at=datetime(2026, 6, 16, 11, 0))]

    with pytest.raises(ValueError, match=expected_message):
        transform_payments(spark, payments=payments, orders=orders)


@pytest.mark.parametrize(
    "overrides",
    [
        {"payment_attempt_number": 0},
        {"amount": Decimal("-0.01")},
        {"processed_at": datetime(2026, 6, 10, 9, 59)},
    ],
)
def test_transform_fct_payments_rejects_invalid_measures_or_chronology(
    spark: SparkSession,
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="invalid payment measures or chronology"):
        transform_payments(spark, payments=[payment_row(**overrides)])


def test_transform_fct_payments_rejects_duplicate_payment_grain(
    spark: SparkSession,
) -> None:
    with pytest.raises(ValueError, match="duplicate payment_id values"):
        transform_payments(
            spark,
            payments=[payment_row(), payment_row(payment_method="card")],
        )


def write_delta(df: DataFrame, path: Path) -> None:
    df.write.format("delta").mode("overwrite").save(str(path))


def test_run_fct_payments_writes_delta_output(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    input_paths = {
        "payments": tmp_path / "silver_payments",
        "orders": tmp_path / "silver_orders",
        "customers": tmp_path / "dim_customers",
        "campaigns": tmp_path / "dim_campaigns",
        "currency": tmp_path / "dim_currency",
        "calendar": tmp_path / "dim_calendar",
    }
    output_path = tmp_path / "fct_payments"
    write_delta(
        create_payments_df(spark, [payment_row(payment_method="card")]),
        input_paths["payments"],
    )
    write_delta(create_orders_df(spark, [order_row()]), input_paths["orders"])
    write_delta(customer_dimension_df(spark), input_paths["customers"])
    write_delta(campaign_dimension_df(spark), input_paths["campaigns"])
    write_delta(currency_dimension_df(spark), input_paths["currency"])
    write_delta(calendar_dimension_df(spark), input_paths["calendar"])

    run_fct_payments(
        spark,
        payments_input_path=input_paths["payments"],
        orders_input_path=input_paths["orders"],
        customers_input_path=input_paths["customers"],
        campaigns_input_path=input_paths["campaigns"],
        currency_input_path=input_paths["currency"],
        calendar_input_path=input_paths["calendar"],
        output_path=output_path,
    )

    result_df = spark.read.format("delta").load(str(output_path))
    assert result_df.count() == 1
    assert result_df.columns == EXPECTED_GOLD_COLUMNS
