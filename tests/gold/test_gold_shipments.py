from datetime import date, datetime
from decimal import Decimal

import pytest
from helpers.gold import (
    calendar_dimension_df,
    campaign_dimension_df,
    customer_dimension_df,
)
from helpers.silver import SILVER_LINEAGE_COLUMNS
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from orderflow.gold.common import GUEST_CUSTOMER_KEY, NO_CAMPAIGN_KEY, UNKNOWN_KEY
from orderflow.gold.fct_shipments import transform_fct_shipments

SILVER_SHIPMENTS_SCHEMA = T.StructType(
    [
        T.StructField("shipment_id", T.StringType(), nullable=True),
        T.StructField("order_id", T.StringType(), nullable=True),
        T.StructField("carrier", T.StringType(), nullable=True),
        T.StructField("shipment_status", T.StringType(), nullable=True),
        T.StructField("shipped_at", T.TimestampType(), nullable=True),
        T.StructField("estimated_delivery_at", T.DateType(), nullable=True),
        T.StructField("delivered_at", T.TimestampType(), nullable=True),
        T.StructField("delivery_country", T.StringType(), nullable=True),
        T.StructField("delivery_city", T.StringType(), nullable=True),
        T.StructField("shipping_cost", T.DecimalType(18, 2), nullable=True),
        T.StructField("_source_file_name", T.StringType(), nullable=False),
        T.StructField("_source_file_path", T.StringType(), nullable=False),
        T.StructField("_ingestion_run_id", T.StringType(), nullable=False),
        T.StructField("_bronze_ingested_at", T.TimestampType(), nullable=False),
        T.StructField("_raw_record_hash", T.StringType(), nullable=False),
        T.StructField("_silver_processed_at", T.TimestampType(), nullable=False),
    ]
)

SILVER_ORDERS_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.StringType(), nullable=True),
        T.StructField("customer_id", T.StringType(), nullable=True),
        T.StructField("campaign_id", T.StringType(), nullable=True),
    ]
)

EXPECTED_COLUMNS = [
    "shipment_key",
    "shipment_id",
    "order_id",
    "customer_key",
    "campaign_key",
    "shipped_date_key",
    "estimated_delivery_date_key",
    "delivered_date_key",
    "carrier",
    "shipment_status",
    "shipped_at",
    "delivered_at",
    "delivery_country_code",
    "delivery_city",
    "shipping_cost",
    "_gold_processed_at",
]

EXPECTED_TYPES = [
    T.LongType(),
    T.StringType(),
    T.StringType(),
    T.LongType(),
    T.LongType(),
    T.IntegerType(),
    T.IntegerType(),
    T.IntegerType(),
    T.StringType(),
    T.StringType(),
    T.TimestampType(),
    T.TimestampType(),
    T.StringType(),
    T.StringType(),
    T.DecimalType(18, 2),
    T.TimestampType(),
]


def shipment_row(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "shipment_id": "ship_001",
        "order_id": "ord_001",
        "carrier": "DHL",
        "shipment_status": "delivered",
        "shipped_at": datetime(2026, 6, 10, 9, 0),
        "estimated_delivery_at": date(2026, 6, 12),
        "delivered_at": datetime(2026, 6, 12, 14, 30),
        "delivery_country": "PL",
        "delivery_city": "Warszawa",
        "shipping_cost": Decimal("14.50"),
        "_source_file_name": "shipments.csv",
        "_source_file_path": "/landing/shipments.csv",
        "_ingestion_run_id": "run-001",
        "_bronze_ingested_at": datetime(2026, 6, 12, 15, 0),
        "_raw_record_hash": "shipment-hash",
        "_silver_processed_at": datetime(2026, 6, 12, 15, 5),
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


def create_shipments_df(
    spark: SparkSession,
    rows: list[dict[str, object]],
) -> DataFrame:
    return spark.createDataFrame(rows, schema=SILVER_SHIPMENTS_SCHEMA)


def create_orders_df(
    spark: SparkSession,
    rows: list[dict[str, object]],
) -> DataFrame:
    return spark.createDataFrame(rows, schema=SILVER_ORDERS_SCHEMA)


def transform_shipments(
    spark: SparkSession,
    *,
    shipment_rows: list[dict[str, object]] | None = None,
    order_rows: list[dict[str, object]] | None = None,
) -> DataFrame:
    return transform_fct_shipments(
        silver_shipments_df=create_shipments_df(
            spark,
            shipment_rows or [shipment_row()],
        ),
        silver_orders_df=create_orders_df(
            spark,
            order_rows or [order_row()],
        ),
        dim_customers_df=customer_dimension_df(spark),
        dim_campaigns_df=campaign_dimension_df(spark),
        dim_calendar_df=calendar_dimension_df(spark),
    )


def test_transform_fct_shipments_matches_contract_and_excludes_silver_lineage(
    spark: SparkSession,
) -> None:
    result_df = transform_shipments(spark)
    result = result_df.first()

    assert result is not None
    assert result_df.columns == EXPECTED_COLUMNS
    assert [field.dataType for field in result_df.schema] == EXPECTED_TYPES
    assert set(SILVER_LINEAGE_COLUMNS).isdisjoint(result_df.columns)
    assert result["shipment_key"] >= 0
    assert result["customer_key"] == 101
    assert result["campaign_key"] == 201
    assert result["shipped_date_key"] == 20260610
    assert result["estimated_delivery_date_key"] == 20260612
    assert result["delivered_date_key"] == 20260612
    assert result["delivery_country_code"] == "PL"
    assert result["shipping_cost"] == Decimal("14.50")
    assert result["_gold_processed_at"] is not None


def test_transform_fct_shipments_preserves_null_optional_dates(
    spark: SparkSession,
) -> None:
    result = transform_shipments(
        spark,
        shipment_rows=[
            shipment_row(
                shipment_status="shipped",
                estimated_delivery_at=None,
                delivered_at=None,
            )
        ],
    ).first()

    assert result is not None
    assert result["estimated_delivery_date_key"] is None
    assert result["delivered_date_key"] is None
    assert result["delivered_at"] is None


def test_transform_fct_shipments_accepts_returned_status(
    spark: SparkSession,
) -> None:
    result = transform_shipments(
        spark,
        shipment_rows=[shipment_row(shipment_status="returned")],
    ).first()

    assert result is not None
    assert result["shipment_status"] == "returned"
    assert result["delivered_at"] == datetime(2026, 6, 12, 14, 30)


def test_transform_fct_shipments_enriches_orders_and_routes_fallbacks(
    spark: SparkSession,
) -> None:
    result_df = transform_shipments(
        spark,
        shipment_rows=[
            shipment_row(),
            shipment_row(shipment_id="ship_guest", order_id="ord_guest"),
            shipment_row(shipment_id="ship_unknown", order_id="ord_unknown"),
        ],
        order_rows=[
            order_row(),
            order_row(order_id="ord_guest", customer_id=None, campaign_id=None),
            order_row(
                order_id="ord_unknown",
                customer_id="missing_customer",
                campaign_id="missing_campaign",
            ),
        ],
    )
    rows = {row["shipment_id"]: row for row in result_df.collect()}

    assert (rows["ship_001"]["customer_key"], rows["ship_001"]["campaign_key"]) == (
        101,
        201,
    )
    assert (
        rows["ship_guest"]["customer_key"],
        rows["ship_guest"]["campaign_key"],
    ) == (GUEST_CUSTOMER_KEY, NO_CAMPAIGN_KEY)
    assert (
        rows["ship_unknown"]["customer_key"],
        rows["ship_unknown"]["campaign_key"],
    ) == (UNKNOWN_KEY, UNKNOWN_KEY)


@pytest.mark.parametrize(
    "delivered_at",
    [
        pytest.param(None, id="missing"),
        pytest.param(datetime(2026, 6, 9, 10, 0), id="before-shipped"),
    ],
)
def test_transform_fct_shipments_rejects_invalid_delivered_behavior(
    spark: SparkSession,
    delivered_at: datetime | None,
) -> None:
    with pytest.raises(ValueError, match="invalid delivered_at values"):
        transform_shipments(
            spark,
            shipment_rows=[shipment_row(delivered_at=delivered_at)],
        )


def test_transform_fct_shipments_rejects_missing_accepted_order(
    spark: SparkSession,
) -> None:
    with pytest.raises(ValueError, match="missing accepted orders"):
        transform_shipments(
            spark,
            shipment_rows=[shipment_row(order_id="ord_missing")],
        )


@pytest.mark.parametrize(
    ("overrides", "expected_message"),
    [
        (
            {"shipped_at": datetime(2026, 6, 30, 9, 0)},
            "shipped dates missing from dim_calendar",
        ),
        (
            {"estimated_delivery_at": date(2026, 6, 30)},
            "estimated delivery dates missing from dim_calendar",
        ),
        (
            {"delivered_at": datetime(2026, 6, 30, 14, 30)},
            "delivered dates missing from dim_calendar",
        ),
    ],
)
def test_transform_fct_shipments_rejects_dates_missing_from_calendar(
    spark: SparkSession,
    overrides: dict[str, object],
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        transform_shipments(
            spark,
            shipment_rows=[shipment_row(**overrides)],
        )
