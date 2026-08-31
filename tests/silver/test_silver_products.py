from decimal import Decimal

import pytest
from helpers.silver import SILVER_LINEAGE_COLUMNS, assert_silver_lineage, with_bronze_lineage
from pyspark.sql import SparkSession

from orderflow.silver.products import transform_products_silver

EXPECTED_COLUMNS = [
    "product_id",
    "sku",
    "product_name",
    "category",
    "brand",
    "unit_price",
    "currency",
    "is_active",
    "created_at",
    "updated_at",
    "load_date",
    "loaded_at",
    "source_event_at",
    *SILVER_LINEAGE_COLUMNS,
]


def product_row(**overrides: object) -> dict[str, object]:
    values = {
        "product_id": " PROD_001 ",
        "sku": " sku-00001 ",
        "product_name": " VistulaTech Phone ",
        "category": " ELECTRONICS ",
        "brand": " VistulaTech ",
        "unit_price": "120.00",
        "currency": " pln ",
        "is_active": "True",
        "created_at": "2025-01-10 09:00:00",
        "updated_at": "2026-06-15 08:00:00",
        "load_date": "2026-06-15",
        "loaded_at": "2026-06-15 09:00:00",
        "source_event_at": "2026-06-15 08:00:00",
    }
    values.update(overrides)
    return with_bronze_lineage(values, source_entity="products")


def test_products_casts_and_normalizes_contract_fields(spark: SparkSession) -> None:
    result_df = transform_products_silver(spark.createDataFrame([product_row()]))
    row = result_df.first()

    assert result_df.columns == EXPECTED_COLUMNS
    assert row["product_id"] == "prod_001"
    assert row["sku"] == "SKU-00001"
    assert row["product_name"] == "VistulaTech Phone"
    assert row["category"] == "electronics"
    assert row["brand"] == "VistulaTech"
    assert row["unit_price"] == Decimal("120.00")
    assert row["currency"] == "PLN"
    assert row["is_active"] is True
    assert_silver_lineage(row)


def test_products_keeps_latest_updated_version(spark: SparkSession) -> None:
    result_df = transform_products_silver(
        spark.createDataFrame(
            [
                product_row(unit_price="120.00", updated_at="2026-06-15 08:00:00"),
                product_row(unit_price="125.00", updated_at="2026-06-16 08:00:00"),
            ]
        )
    )

    assert result_df.count() == 1
    assert result_df.first()["unit_price"] == Decimal("125.00")


@pytest.mark.parametrize(
    ("overrides", "expected_message"),
    [
        ({"unit_price": "-0.01"}, "negative unit prices"),
        ({"unit_price": "not-a-number"}, "null required fields"),
        ({"updated_at": "2025-01-10 08:59:59"}, "updated_at before created_at"),
    ],
)
def test_products_rejects_contract_rule_violations(
    spark: SparkSession,
    overrides: dict[str, object],
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        transform_products_silver(spark.createDataFrame([product_row(**overrides)]))
