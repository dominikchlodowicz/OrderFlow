from datetime import datetime

import pytest
from helpers.gold import (
    calendar_dimension_df,
    campaign_dimension_df,
    customer_dimension_df,
    product_dimension_df,
)
from helpers.silver import SILVER_LINEAGE_COLUMNS
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from orderflow.gold.common import (
    ANONYMOUS_CUSTOMER_KEY,
    NO_CAMPAIGN_KEY,
    NOT_APPLICABLE_PRODUCT_KEY,
    UNKNOWN_KEY,
)
from orderflow.gold.fct_web_events import transform_fct_web_events

SILVER_WEB_EVENTS_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), nullable=True),
        T.StructField("session_id", T.StringType(), nullable=True),
        T.StructField("customer_id", T.StringType(), nullable=True),
        T.StructField("anonymous_id", T.StringType(), nullable=True),
        T.StructField("event_type", T.StringType(), nullable=True),
        T.StructField("event_timestamp", T.TimestampType(), nullable=True),
        T.StructField("product_id", T.StringType(), nullable=True),
        T.StructField("campaign_id", T.StringType(), nullable=True),
        T.StructField("device_type", T.StringType(), nullable=True),
        T.StructField("country_code", T.StringType(), nullable=True),
        T.StructField("page_url", T.StringType(), nullable=True),
        T.StructField("_source_file_name", T.StringType(), nullable=False),
        T.StructField("_source_file_path", T.StringType(), nullable=False),
        T.StructField("_ingestion_run_id", T.StringType(), nullable=False),
        T.StructField("_bronze_ingested_at", T.TimestampType(), nullable=False),
        T.StructField("_raw_record_hash", T.StringType(), nullable=False),
        T.StructField("_silver_processed_at", T.TimestampType(), nullable=False),
    ]
)

EXPECTED_COLUMNS = [
    "event_key",
    "event_id",
    "session_id",
    "anonymous_id",
    "customer_key",
    "product_key",
    "campaign_key",
    "event_date_key",
    "event_type",
    "event_timestamp",
    "device_type",
    "country_code",
    "page_url",
    "_gold_processed_at",
]

EXPECTED_TYPES = [
    T.LongType(),
    T.StringType(),
    T.StringType(),
    T.StringType(),
    T.LongType(),
    T.LongType(),
    T.LongType(),
    T.IntegerType(),
    T.StringType(),
    T.TimestampType(),
    T.StringType(),
    T.StringType(),
    T.StringType(),
    T.TimestampType(),
]


def web_event_row(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "event_id": "evt_001",
        "session_id": "sess_001",
        "customer_id": "cust_001",
        "anonymous_id": "anon_001",
        "event_type": "product_view",
        "event_timestamp": datetime(2026, 6, 10, 10, 15),
        "product_id": "prod_001",
        "campaign_id": "cmp_001",
        "device_type": "mobile",
        "country_code": "PL",
        "page_url": "/products/prod_001",
        "_source_file_name": "web_events.csv",
        "_source_file_path": "/landing/web_events.csv",
        "_ingestion_run_id": "run-001",
        "_bronze_ingested_at": datetime(2026, 6, 10, 11, 0),
        "_raw_record_hash": "web-event-hash",
        "_silver_processed_at": datetime(2026, 6, 10, 11, 5),
    }
    values.update(overrides)
    return values


def create_web_events_df(
    spark: SparkSession,
    rows: list[dict[str, object]],
) -> DataFrame:
    return spark.createDataFrame(rows, schema=SILVER_WEB_EVENTS_SCHEMA)


def transform_web_events(
    spark: SparkSession,
    rows: list[dict[str, object]],
) -> DataFrame:
    return transform_fct_web_events(
        silver_web_events_df=create_web_events_df(spark, rows),
        dim_customers_df=customer_dimension_df(spark),
        dim_products_df=product_dimension_df(spark),
        dim_campaigns_df=campaign_dimension_df(spark),
        dim_calendar_df=calendar_dimension_df(spark),
    )


def test_transform_fct_web_events_matches_contract_and_excludes_silver_lineage(
    spark: SparkSession,
) -> None:
    result_df = transform_web_events(spark, [web_event_row()])
    result = result_df.first()

    assert result is not None
    assert result_df.columns == EXPECTED_COLUMNS
    assert [field.dataType for field in result_df.schema] == EXPECTED_TYPES
    assert set(SILVER_LINEAGE_COLUMNS).isdisjoint(result_df.columns)
    assert result["event_key"] >= 0
    assert result["customer_key"] == 101
    assert result["product_key"] == 301
    assert result["campaign_key"] == 201
    assert result["event_date_key"] == 20260610
    assert result["event_timestamp"] == datetime(2026, 6, 10, 10, 15)
    assert result["_gold_processed_at"] is not None


def test_transform_fct_web_events_routes_every_documented_fallback(
    spark: SparkSession,
) -> None:
    result_df = transform_web_events(
        spark,
        [
            web_event_row(),
            web_event_row(
                event_id="evt_null",
                customer_id=None,
                event_type="page_view",
                product_id=None,
                campaign_id=None,
            ),
            web_event_row(
                event_id="evt_unresolved",
                customer_id="missing_customer",
                product_id="missing_product",
                campaign_id="missing_campaign",
            ),
            web_event_row(
                event_id="evt_null_product_event",
                product_id=None,
            ),
            web_event_row(
                event_id="evt_non_product_with_product",
                event_type="checkout_started",
            ),
        ],
    )
    rows = {row["event_id"]: row for row in result_df.collect()}

    assert (
        rows["evt_001"]["customer_key"],
        rows["evt_001"]["product_key"],
        rows["evt_001"]["campaign_key"],
    ) == (101, 301, 201)
    assert (
        rows["evt_null"]["customer_key"],
        rows["evt_null"]["product_key"],
        rows["evt_null"]["campaign_key"],
    ) == (ANONYMOUS_CUSTOMER_KEY, NOT_APPLICABLE_PRODUCT_KEY, NO_CAMPAIGN_KEY)
    assert (
        rows["evt_unresolved"]["customer_key"],
        rows["evt_unresolved"]["product_key"],
        rows["evt_unresolved"]["campaign_key"],
    ) == (UNKNOWN_KEY, UNKNOWN_KEY, UNKNOWN_KEY)
    assert rows["evt_null_product_event"]["product_key"] == UNKNOWN_KEY
    assert rows["evt_non_product_with_product"]["product_key"] == NOT_APPLICABLE_PRODUCT_KEY


@pytest.mark.parametrize(
    "event_timestamp",
    [
        pytest.param(None, id="missing-timestamp"),
        pytest.param(datetime(2026, 6, 30, 10, 15), id="date-not-in-calendar"),
    ],
)
def test_transform_fct_web_events_rejects_missing_event_calendar_resolution(
    spark: SparkSession,
    event_timestamp: datetime | None,
) -> None:
    with pytest.raises(ValueError, match="event dates missing from dim_calendar"):
        transform_web_events(
            spark,
            [web_event_row(event_timestamp=event_timestamp)],
        )


def test_transform_fct_web_events_rejects_invalid_device_type(
    spark: SparkSession,
) -> None:
    with pytest.raises(ValueError, match="invalid device type values"):
        transform_web_events(
            spark,
            [web_event_row(device_type="smart_fridge")],
        )
