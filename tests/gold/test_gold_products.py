from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from orderflow.gold.common import (
    NOT_APPLICABLE_PRODUCT_ID,
    NOT_APPLICABLE_PRODUCT_KEY,
    UNKNOWN_KEY,
    UNKNOWN_MEMBER_ID,
)
from orderflow.gold.dim_products import transform_dim_products

SILVER_PRODUCTS_SCHEMA = T.StructType(
    [
        T.StructField("product_id", T.StringType(), nullable=True),
        T.StructField("sku", T.StringType(), nullable=True),
        T.StructField("product_name", T.StringType(), nullable=True),
        T.StructField("category", T.StringType(), nullable=True),
        T.StructField("brand", T.StringType(), nullable=True),
        T.StructField("unit_price", T.DecimalType(18, 2), nullable=True),
        T.StructField("currency", T.StringType(), nullable=True),
        T.StructField("is_active", T.BooleanType(), nullable=True),
        T.StructField("created_at", T.TimestampType(), nullable=True),
        T.StructField("updated_at", T.TimestampType(), nullable=True),
    ]
)

EXPECTED_GOLD_COLUMNS = [
    "product_key",
    "product_id",
    "sku",
    "product_name",
    "category",
    "brand",
    "unit_price",
    "currency_code",
    "is_active",
    "created_at",
    "updated_at",
    "_gold_processed_at",
]


def product_row(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "product_id": "prod_001",
        "sku": "SKU-00001",
        "product_name": "VistulaTech Phone",
        "category": "electronics",
        "brand": "VistulaTech",
        "unit_price": Decimal("120.00"),
        "currency": "PLN",
        "is_active": True,
        "created_at": datetime(2025, 1, 10, 9, 0),
        "updated_at": datetime(2026, 6, 15, 8, 0),
    }
    values.update(overrides)
    return values


def create_silver_products_df(
    spark: SparkSession,
    rows: list[dict[str, object]],
) -> DataFrame:
    return spark.createDataFrame(rows, schema=SILVER_PRODUCTS_SCHEMA)


def test_transform_dim_products_matches_contract_and_preserves_attributes(
    spark: SparkSession,
) -> None:
    result_df = transform_dim_products(create_silver_products_df(spark, [product_row()]))
    result = result_df.filter(F.col("product_id") == "prod_001").first()

    assert result_df.columns == EXPECTED_GOLD_COLUMNS
    assert result is not None
    assert result["product_key"] >= 0
    assert result["currency_code"] == "PLN"
    assert result["unit_price"] == Decimal("120.00")
    assert result["is_active"] is True
    assert result["_gold_processed_at"] is not None
    assert result_df.schema["product_key"].dataType == T.LongType()
    assert result_df.schema["unit_price"].dataType == T.DecimalType(18, 2)


def test_transform_dim_products_adds_reserved_physical_members(
    spark: SparkSession,
) -> None:
    result_df = transform_dim_products(create_silver_products_df(spark, [product_row()]))
    special_members = {
        row["product_id"]: row for row in result_df.filter(F.col("product_key") < 0).collect()
    }

    assert {member_id: row["product_key"] for member_id, row in special_members.items()} == {
        UNKNOWN_MEMBER_ID: UNKNOWN_KEY,
        NOT_APPLICABLE_PRODUCT_ID: NOT_APPLICABLE_PRODUCT_KEY,
    }
    assert all(row["unit_price"] == Decimal("0.00") for row in special_members.values())
    assert all(row["is_active"] is False for row in special_members.values())


def test_transform_dim_products_generates_stable_regular_key(
    spark: SparkSession,
) -> None:
    silver_df = create_silver_products_df(spark, [product_row()])

    first_key = (
        transform_dim_products(silver_df)
        .filter(F.col("product_id") == "prod_001")
        .first()["product_key"]
    )
    second_key = (
        transform_dim_products(silver_df)
        .filter(F.col("product_id") == "prod_001")
        .first()["product_key"]
    )

    assert first_key == second_key
    assert first_key >= 0


@pytest.mark.parametrize(
    ("overrides", "expected_message"),
    [
        ({"unit_price": Decimal("-0.01")}, "negative unit prices"),
        (
            {"updated_at": datetime(2025, 1, 10, 8, 59)},
            "updated_at before created_at",
        ),
        ({"currency": "pln"}, "invalid currency codes"),
    ],
)
def test_transform_dim_products_rejects_contract_rule_violations(
    spark: SparkSession,
    overrides: dict[str, object],
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        transform_dim_products(create_silver_products_df(spark, [product_row(**overrides)]))


def test_transform_dim_products_rejects_duplicate_skus(spark: SparkSession) -> None:
    silver_df = create_silver_products_df(
        spark,
        [
            product_row(),
            product_row(product_id="prod_002"),
        ],
    )

    with pytest.raises(ValueError, match="duplicate sku values"):
        transform_dim_products(silver_df)
