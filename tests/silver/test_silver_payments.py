from datetime import date
from decimal import Decimal

import pytest
from helpers.silver import SILVER_LINEAGE_COLUMNS, assert_silver_lineage, with_bronze_lineage
from pyspark.sql import SparkSession

from orderflow.silver.payments import transform_payments_silver

EXPECTED_COLUMNS = [
    "payment_id",
    "order_id",
    "payment_attempt_number",
    "payment_method",
    "payment_status",
    "failure_reason",
    "amount",
    "currency",
    "created_at",
    "processed_at",
    "load_date",
    "loaded_at",
    *SILVER_LINEAGE_COLUMNS,
]


def payment_row(**overrides: object) -> dict[str, object]:
    values = {
        "payment_id": " PAY_001 ",
        "order_id": " ORD_001 ",
        "payment_attempt_number": "1",
        "payment_method": " CARD ",
        "payment_status": " CAPTURED ",
        "amount": "100.00",
        "currency": " pln ",
        "created_at": "2026-06-15 09:05:00",
        "processed_at": "2026-06-15 09:06:00",
        "failure_reason": "card_declined",
        "load_date": "2026-06-15",
        "loaded_at": "2026-06-15 10:00:00",
        "source_event_at": "2026-06-15 09:06:00",
    }
    values.update(overrides)
    return with_bronze_lineage(values, source_entity="payments")


def test_payments_applies_conditional_failure_reason_and_contract_types(
    spark: SparkSession,
) -> None:
    result_df = transform_payments_silver(spark.createDataFrame([payment_row()]))
    row = result_df.first()

    assert result_df.columns == EXPECTED_COLUMNS
    assert row["payment_id"] == "pay_001"
    assert row["payment_method"] == "card"
    assert row["payment_status"] == "captured"
    assert row["failure_reason"] is None
    assert row["amount"] == Decimal("100.00")
    assert row["currency"] == "PLN"
    assert row["load_date"] == date(2026, 6, 15)
    assert_silver_lineage(row)


def test_payments_keeps_latest_payment_version(spark: SparkSession) -> None:
    result_df = transform_payments_silver(
        spark.createDataFrame(
            [
                payment_row(
                    payment_status="authorized",
                    processed_at="2026-06-15 09:06:00",
                ),
                payment_row(
                    payment_status="captured",
                    processed_at="2026-06-15 09:07:00",
                ),
            ]
        )
    )

    assert result_df.count() == 1
    assert result_df.first()["payment_status"] == "captured"


def test_payments_preserves_valid_failed_reason(spark: SparkSession) -> None:
    result_df = transform_payments_silver(
        spark.createDataFrame(
            [payment_row(payment_status="failed", failure_reason=" CARD_DECLINED ")]
        )
    )

    assert result_df.first()["failure_reason"] == "card_declined"


@pytest.mark.parametrize(
    ("overrides", "expected_message"),
    [
        (
            {"payment_status": "failed", "failure_reason": ""},
            "invalid failure reason",
        ),
        ({"payment_status": "invalid_payment_status"}, "invalid payment method or status"),
        ({"payment_attempt_number": "0"}, "invalid payment values"),
        ({"processed_at": "2026-06-15 09:04:00"}, "invalid payment values"),
    ],
)
def test_payments_rejects_contract_rule_violations(
    spark: SparkSession,
    overrides: dict[str, object],
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        transform_payments_silver(spark.createDataFrame([payment_row(**overrides)]))
