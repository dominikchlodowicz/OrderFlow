from decimal import Decimal

import pytest
from helpers.silver import SILVER_LINEAGE_COLUMNS, assert_silver_lineage, with_bronze_lineage
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from orderflow.silver.order_items import transform_order_items_silver

EXPECTED_COLUMNS = [
    "order_item_id",
    "order_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount_amount",
    "gross_amount",
    "line_total",
    "created_at",
    "source_event_at",
    *SILVER_LINEAGE_COLUMNS,
]


def order_item_row(**overrides: object) -> dict[str, object]:
    values = {
        "order_item_id": " ITEM_001 ",
        "order_id": " ORD_001 ",
        "product_id": " PROD_001 ",
        "quantity": "2",
        "unit_price": "10.00",
        "discount_amount": "",
        "line_total": "999.99",
        "created_at": "2026-06-15 09:00:00",
        "load_date": "2026-06-15",
        "loaded_at": "2026-06-15 10:00:00",
        "source_event_at": "2026-06-15 09:00:00",
    }
    values.update(overrides)
    return with_bronze_lineage(values, source_entity="order_items")


def test_order_items_derives_contract_amounts_from_raw_components(
    spark: SparkSession,
) -> None:
    result_df = transform_order_items_silver(spark.createDataFrame([order_item_row()]))
    row = result_df.first()

    assert result_df.columns == EXPECTED_COLUMNS
    assert row["order_item_id"] == "item_001"
    assert row["order_id"] == "ord_001"
    assert row["product_id"] == "prod_001"
    assert row["quantity"] == 2
    assert row["unit_price"] == Decimal("10.00")
    assert row["discount_amount"] == Decimal("0.00")
    assert row["gross_amount"] == Decimal("20.00")
    assert row["line_total"] == Decimal("20.00")
    assert isinstance(result_df.schema["gross_amount"].dataType, T.DecimalType)
    assert_silver_lineage(row)


def test_order_items_keeps_latest_source_event_version(spark: SparkSession) -> None:
    result_df = transform_order_items_silver(
        spark.createDataFrame(
            [
                order_item_row(unit_price="10.00", source_event_at="2026-06-15 09:00:00"),
                order_item_row(unit_price="12.00", source_event_at="2026-06-15 10:00:00"),
            ]
        )
    )

    assert result_df.count() == 1
    assert result_df.first()["unit_price"] == Decimal("12.00")


@pytest.mark.parametrize(
    ("overrides", "expected_message"),
    [
        ({"quantity": "0"}, "quantity less than or equal to zero"),
        ({"unit_price": "-1.00"}, "invalid monetary values"),
        ({"discount_amount": "21.00"}, "invalid monetary values"),
        ({"discount_amount": "not-a-number"}, "null required fields"),
    ],
)
def test_order_items_rejects_invalid_business_values(
    spark: SparkSession,
    overrides: dict[str, object],
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        transform_order_items_silver(spark.createDataFrame([order_item_row(**overrides)]))
