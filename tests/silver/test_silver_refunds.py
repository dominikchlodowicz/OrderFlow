from datetime import date
from decimal import Decimal

import pytest
from helpers.silver import SILVER_LINEAGE_COLUMNS, assert_silver_lineage, with_bronze_lineage
from pyspark.sql import SparkSession

from orderflow.silver.refunds import transform_refunds_silver

EXPECTED_COLUMNS = [
    "refund_id",
    "order_id",
    "payment_id",
    "refund_reason",
    "refund_status",
    "refund_amount",
    "currency",
    "created_at",
    "processed_at",
    "load_date",
    "loaded_at",
    "source_event_at",
    *SILVER_LINEAGE_COLUMNS,
]


def refund_row(**overrides: object) -> dict[str, object]:
    values = {
        "refund_id": " REF_001 ",
        "order_id": " ORD_001 ",
        "payment_id": " PAY_001 ",
        "refund_reason": " CUSTOMER_CHANGED_MIND ",
        "refund_amount": "25.00",
        "currency": " pln ",
        "created_at": "2026-06-18 09:00:00",
        "processed_at": "2026-06-18 10:00:00",
        "refund_status": " PROCESSED ",
        "load_date": "2026-06-18",
        "loaded_at": "2026-06-18 11:00:00",
        "source_event_at": "2026-06-18 10:00:00",
    }
    values.update(overrides)
    return with_bronze_lineage(values, source_entity="refunds", load_date="2026-06-18")


def test_refunds_casts_and_normalizes_contract_fields(spark: SparkSession) -> None:
    result_df = transform_refunds_silver(spark.createDataFrame([refund_row()]))
    row = result_df.first()

    assert result_df.columns == EXPECTED_COLUMNS
    assert row["refund_id"] == "ref_001"
    assert row["refund_reason"] == "customer_changed_mind"
    assert row["refund_status"] == "processed"
    assert row["refund_amount"] == Decimal("25.00")
    assert row["currency"] == "PLN"
    assert row["load_date"] == date(2026, 6, 18)
    assert_silver_lineage(row)


def test_refunds_keeps_latest_processed_version(spark: SparkSession) -> None:
    result_df = transform_refunds_silver(
        spark.createDataFrame(
            [
                refund_row(refund_status="pending", processed_at="2026-06-18 10:00:00"),
                refund_row(refund_status="processed", processed_at="2026-06-18 11:00:00"),
            ]
        )
    )

    assert result_df.count() == 1
    assert result_df.first()["refund_status"] == "processed"


@pytest.mark.parametrize(
    "overrides",
    [
        {"refund_amount": "-0.01"},
        {"processed_at": "2026-06-18 08:59:59"},
    ],
)
def test_refunds_rejects_invalid_amount_or_chronology(
    spark: SparkSession,
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="invalid refund amount or chronology"):
        transform_refunds_silver(spark.createDataFrame([refund_row(**overrides)]))
