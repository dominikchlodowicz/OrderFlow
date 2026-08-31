from datetime import date, datetime
from decimal import Decimal

import pytest
from helpers.silver import SILVER_LINEAGE_COLUMNS, assert_silver_lineage, with_bronze_lineage
from pyspark.sql import SparkSession

from orderflow.silver.shipments import transform_shipments_silver

EXPECTED_COLUMNS = [
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
    "load_date",
    "loaded_at",
    "source_event_at",
    *SILVER_LINEAGE_COLUMNS,
]


def shipment_row(**overrides: object) -> dict[str, object]:
    values = {
        "shipment_id": " SHIP_001 ",
        "order_id": " ORD_001 ",
        "carrier": " InPost ",
        "shipment_status": " DELIVERED ",
        "shipped_at": "2026-06-15 09:00:00",
        "estimated_delivery_at": "2026-06-17 18:00:00",
        "delivered_at": "2026-06-17 12:00:00",
        "delivery_country": " pl ",
        "delivery_city": " Warszawa ",
        "shipping_cost": "15.00",
        "load_date": "2026-06-17",
        "loaded_at": "2026-06-17 13:00:00",
        "source_event_at": "2026-06-17 12:00:00",
    }
    values.update(overrides)
    return with_bronze_lineage(values, source_entity="shipments", load_date="2026-06-17")


def test_shipments_casts_dates_and_normalizes_contract_fields(spark: SparkSession) -> None:
    result_df = transform_shipments_silver(spark.createDataFrame([shipment_row()]))
    row = result_df.first()

    assert result_df.columns == EXPECTED_COLUMNS
    assert row["shipment_id"] == "ship_001"
    assert row["shipment_status"] == "delivered"
    assert row["estimated_delivery_at"] == date(2026, 6, 17)
    assert row["delivered_at"] == datetime(2026, 6, 17, 12, 0, 0)
    assert row["delivery_country"] == "PL"
    assert row["shipping_cost"] == Decimal("15.00")
    assert_silver_lineage(row)


def test_shipments_keeps_latest_lifecycle_version(spark: SparkSession) -> None:
    result_df = transform_shipments_silver(
        spark.createDataFrame(
            [
                shipment_row(
                    shipment_status="shipped",
                    delivered_at="",
                    source_event_at="2026-06-15 09:00:00",
                ),
                shipment_row(
                    shipment_status="delivered",
                    source_event_at="2026-06-17 12:00:00",
                ),
            ]
        )
    )

    assert result_df.count() == 1
    assert result_df.first()["shipment_status"] == "delivered"


@pytest.mark.parametrize(
    ("overrides", "expected_message"),
    [
        ({"shipment_status": "invalid_shipment_status"}, "invalid shipment status or cost"),
        ({"shipping_cost": "-0.01"}, "invalid shipment status or cost"),
        ({"estimated_delivery_at": "2026-06-14"}, "estimated delivery before shipment"),
        ({"delivered_at": ""}, "invalid delivered_at"),
        ({"delivered_at": "2026-06-15 08:59:59"}, "invalid delivered_at"),
    ],
)
def test_shipments_rejects_lifecycle_rule_violations(
    spark: SparkSession,
    overrides: dict[str, object],
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        transform_shipments_silver(spark.createDataFrame([shipment_row(**overrides)]))
