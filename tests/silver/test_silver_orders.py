from datetime import datetime
from decimal import Decimal

import pytest
from helpers.silver import SILVER_LINEAGE_COLUMNS, assert_silver_lineage, with_bronze_lineage
from pyspark.sql import SparkSession

from orderflow.silver.orders import transform_orders_silver

EXPECTED_COLUMNS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_created_at",
    "order_updated_at",
    "country_code",
    "currency",
    "gross_amount",
    "discount_amount",
    "net_amount",
    "source_channel",
    "campaign_id",
    "source_event_at",
    *SILVER_LINEAGE_COLUMNS,
]


def order_row(**overrides: object) -> dict[str, object]:
    values = {
        "order_id": " ORD_001 ",
        "customer_id": " CUST_001 ",
        "order_status": " PAID ",
        "order_created_at": "2026-06-15 09:00:00",
        "order_updated_at": "2026-06-15 10:00:00",
        "country_code": " pl ",
        "currency": " pln ",
        "gross_amount": "100.00",
        "discount_amount": "",
        "net_amount": "100.00",
        "source_channel": " EMAIL ",
        "campaign_id": " CMP_001 ",
        "load_date": "2026-06-15",
        "loaded_at": "2026-06-15 11:00:00",
        "source_event_at": "2026-06-15 10:00:00",
    }
    values.update(overrides)
    return with_bronze_lineage(values, source_entity="orders")


def test_orders_casts_normalizes_and_defaults_contract_fields(spark: SparkSession) -> None:
    result_df = transform_orders_silver(spark.createDataFrame([order_row()]))
    row = result_df.first()

    assert result_df.columns == EXPECTED_COLUMNS
    assert row["order_id"] == "ord_001"
    assert row["customer_id"] == "cust_001"
    assert row["order_status"] == "paid"
    assert row["country_code"] == "PL"
    assert row["currency"] == "PLN"
    assert row["discount_amount"] == Decimal("0.00")
    assert row["net_amount"] == Decimal("100.00")
    assert row["order_created_at"] == datetime(2026, 6, 15, 9, 0, 0)
    assert_silver_lineage(row)


def test_orders_keeps_latest_business_update(spark: SparkSession) -> None:
    result_df = transform_orders_silver(
        spark.createDataFrame(
            [
                order_row(order_status="created", order_updated_at="2026-06-15 09:00:00"),
                order_row(order_status="paid", order_updated_at="2026-06-15 10:00:00"),
            ]
        )
    )

    assert result_df.count() == 1
    assert result_df.first()["order_status"] == "paid"


@pytest.mark.parametrize(
    ("overrides", "expected_message"),
    [
        ({"order_status": "delivered"}, "invalid order status"),
        ({"net_amount": "99.99"}, "inconsistent order amounts"),
        (
            {"order_updated_at": "2026-06-15 08:59:59"},
            "order_updated_at before order_created_at",
        ),
    ],
)
def test_orders_rejects_contract_rule_violations(
    spark: SparkSession,
    overrides: dict[str, object],
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        transform_orders_silver(spark.createDataFrame([order_row(**overrides)]))
