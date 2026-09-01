from datetime import datetime
from decimal import Decimal

import pytest
from helpers.gold import (
    calendar_dimension_df,
    campaign_dimension_df,
    currency_dimension_df,
    customer_dimension_df,
    product_dimension_df,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from orderflow.gold.common import GUEST_CUSTOMER_KEY, NO_CAMPAIGN_KEY, UNKNOWN_KEY
from orderflow.gold.fct_order_items import transform_fct_order_items

SILVER_ORDER_ITEMS_SCHEMA = T.StructType(
    [
        T.StructField("order_item_id", T.StringType(), nullable=True),
        T.StructField("order_id", T.StringType(), nullable=True),
        T.StructField("product_id", T.StringType(), nullable=True),
        T.StructField("quantity", T.IntegerType(), nullable=True),
        T.StructField("unit_price", T.DecimalType(18, 2), nullable=True),
        T.StructField("discount_amount", T.DecimalType(18, 2), nullable=True),
        T.StructField("gross_amount", T.DecimalType(18, 2), nullable=True),
        T.StructField("line_total", T.DecimalType(18, 2), nullable=True),
        T.StructField("created_at", T.TimestampType(), nullable=True),
        T.StructField("source_event_at", T.TimestampType(), nullable=True),
        T.StructField("_source_file_name", T.StringType(), nullable=False),
        T.StructField("_source_file_path", T.StringType(), nullable=False),
        T.StructField("_ingestion_run_id", T.StringType(), nullable=False),
        T.StructField("_bronze_ingested_at", T.TimestampType(), nullable=False),
        T.StructField("_raw_record_hash", T.StringType(), nullable=False),
        T.StructField("_silver_processed_at", T.TimestampType(), nullable=False),
    ]
)

SILVER_ORDER_HEADERS_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.StringType(), nullable=True),
        T.StructField("customer_id", T.StringType(), nullable=True),
        T.StructField("campaign_id", T.StringType(), nullable=True),
        T.StructField("currency", T.StringType(), nullable=True),
        T.StructField("country_code", T.StringType(), nullable=True),
        T.StructField("order_created_at", T.TimestampType(), nullable=True),
    ]
)

EXPECTED_GOLD_SCHEMA = [
    ("order_item_key", "bigint"),
    ("order_item_id", "string"),
    ("order_id", "string"),
    ("customer_key", "bigint"),
    ("product_key", "bigint"),
    ("campaign_key", "bigint"),
    ("currency_key", "bigint"),
    ("order_date_key", "int"),
    ("order_item_created_date_key", "int"),
    ("order_country_code", "string"),
    ("order_item_created_at", "timestamp"),
    ("quantity", "int"),
    ("unit_price", "decimal(18,2)"),
    ("discount_amount", "decimal(18,2)"),
    ("gross_amount", "decimal(18,2)"),
    ("line_total", "decimal(18,2)"),
    ("_gold_processed_at", "timestamp"),
]

SILVER_LINEAGE_COLUMNS = {
    "source_event_at",
    "_source_file_name",
    "_source_file_path",
    "_ingestion_run_id",
    "_bronze_ingested_at",
    "_raw_record_hash",
    "_silver_processed_at",
}


def silver_order_item_row(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "order_item_id": "item_001",
        "order_id": "ord_001",
        "product_id": "prod_001",
        "quantity": 2,
        "unit_price": Decimal("47.50"),
        "discount_amount": Decimal("5.00"),
        "gross_amount": Decimal("95.00"),
        "line_total": Decimal("90.00"),
        "created_at": datetime(2026, 6, 11, 8, 15),
        "source_event_at": datetime(2026, 6, 11, 8, 15),
        "_source_file_name": "order_items.csv",
        "_source_file_path": "/bronze/order_items.csv",
        "_ingestion_run_id": "run-001",
        "_bronze_ingested_at": datetime(2026, 6, 11, 8, 20),
        "_raw_record_hash": "item-hash",
        "_silver_processed_at": datetime(2026, 6, 11, 8, 25),
    }
    values.update(overrides)
    return values


def silver_order_header_row(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "order_id": "ord_001",
        "customer_id": "cust_001",
        "campaign_id": "cmp_001",
        "currency": "PLN",
        "country_code": "DE",
        "order_created_at": datetime(2026, 6, 10, 9, 30),
    }
    values.update(overrides)
    return values


def create_silver_order_items_df(
    spark: SparkSession,
    rows: list[dict[str, object]],
) -> DataFrame:
    return spark.createDataFrame(rows, schema=SILVER_ORDER_ITEMS_SCHEMA)


def create_silver_order_headers_df(
    spark: SparkSession,
    rows: list[dict[str, object]],
) -> DataFrame:
    return spark.createDataFrame(rows, schema=SILVER_ORDER_HEADERS_SCHEMA)


def transform_order_items(
    spark: SparkSession,
    *,
    item_rows: list[dict[str, object]] | None = None,
    order_rows: list[dict[str, object]] | None = None,
    products_df: DataFrame | None = None,
    currency_df: DataFrame | None = None,
    calendar_df: DataFrame | None = None,
) -> DataFrame:
    return transform_fct_order_items(
        silver_order_items_df=create_silver_order_items_df(
            spark,
            item_rows if item_rows is not None else [silver_order_item_row()],
        ),
        silver_orders_df=create_silver_order_headers_df(
            spark,
            order_rows if order_rows is not None else [silver_order_header_row()],
        ),
        dim_customers_df=customer_dimension_df(spark),
        dim_products_df=products_df or product_dimension_df(spark),
        dim_campaigns_df=campaign_dimension_df(spark),
        dim_currency_df=currency_df or currency_dimension_df(spark),
        dim_calendar_df=calendar_df or calendar_dimension_df(spark),
    )


def test_transform_fct_order_items_matches_contract_and_inherits_order_context(
    spark: SparkSession,
) -> None:
    result_df = transform_order_items(spark)
    result = result_df.first()

    assert result_df.dtypes == EXPECTED_GOLD_SCHEMA
    assert result is not None
    assert result["order_item_key"] >= 0
    assert result["customer_key"] == 101
    assert result["product_key"] == 301
    assert result["campaign_key"] == 201
    assert result["currency_key"] == 401
    assert result["order_date_key"] == 20260610
    assert result["order_item_created_date_key"] == 20260611
    assert result["order_country_code"] == "DE"
    assert result["unit_price"] == Decimal("47.50")
    assert result["gross_amount"] == Decimal("95.00")
    assert result["line_total"] == Decimal("90.00")
    assert result["_gold_processed_at"] is not None
    assert SILVER_LINEAGE_COLUMNS.isdisjoint(result_df.columns)


def test_transform_fct_order_items_inherits_absent_and_unresolved_header_members(
    spark: SparkSession,
) -> None:
    result_df = transform_order_items(
        spark,
        item_rows=[
            silver_order_item_row(order_item_id="item_guest", order_id="ord_guest"),
            silver_order_item_row(order_item_id="item_unknown", order_id="ord_unknown"),
        ],
        order_rows=[
            silver_order_header_row(
                order_id="ord_guest",
                customer_id=None,
                campaign_id=None,
            ),
            silver_order_header_row(
                order_id="ord_unknown",
                customer_id="cust_missing",
                campaign_id="cmp_missing",
            ),
        ],
    )
    rows = {row["order_item_id"]: row for row in result_df.collect()}

    assert rows["item_guest"]["customer_key"] == GUEST_CUSTOMER_KEY
    assert rows["item_guest"]["campaign_key"] == NO_CAMPAIGN_KEY
    assert rows["item_unknown"]["customer_key"] == UNKNOWN_KEY
    assert rows["item_unknown"]["campaign_key"] == UNKNOWN_KEY


@pytest.mark.parametrize(
    ("missing_reference", "expected_message"),
    [
        ("order", "unresolved order references"),
        ("product", "unresolved product references"),
        ("currency", "unresolved currency references"),
        ("order_calendar", "unresolved order calendar dates"),
        ("item_calendar", "unresolved order-item calendar dates"),
    ],
)
def test_transform_fct_order_items_rejects_missing_required_relationships(
    spark: SparkSession,
    missing_reference: str,
    expected_message: str,
) -> None:
    item_rows = [silver_order_item_row()]
    order_rows = [silver_order_header_row()]
    if missing_reference == "order":
        order_rows = []
    elif missing_reference == "product":
        item_rows = [silver_order_item_row(product_id="prod_missing")]
    elif missing_reference == "currency":
        order_rows = [silver_order_header_row(currency="USD")]
    elif missing_reference == "order_calendar":
        order_rows = [silver_order_header_row(order_created_at=datetime(2026, 6, 30, 9, 30))]
    else:
        item_rows = [silver_order_item_row(created_at=datetime(2026, 6, 30, 8, 15))]

    with pytest.raises(ValueError, match=expected_message):
        transform_order_items(
            spark,
            item_rows=item_rows,
            order_rows=order_rows,
        )


def test_transform_fct_order_items_rejects_duplicate_grain(
    spark: SparkSession,
) -> None:
    with pytest.raises(ValueError, match="duplicate order_item_id values"):
        transform_order_items(
            spark,
            item_rows=[silver_order_item_row(), silver_order_item_row()],
        )


def test_transform_fct_order_items_rejects_invalid_measures(
    spark: SparkSession,
) -> None:
    with pytest.raises(ValueError, match="inconsistent order-item measures"):
        transform_order_items(
            spark,
            item_rows=[silver_order_item_row(line_total=Decimal("91.00"))],
        )
