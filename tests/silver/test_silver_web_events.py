from datetime import datetime

import pytest
from helpers.silver import SILVER_LINEAGE_COLUMNS, assert_silver_lineage, with_bronze_lineage
from pyspark.sql import SparkSession

from orderflow.silver.web_events import transform_web_events_silver

EXPECTED_COLUMNS = [
    "event_id",
    "session_id",
    "customer_id",
    "anonymous_id",
    "event_type",
    "event_timestamp",
    "product_id",
    "campaign_id",
    "device_type",
    "country_code",
    "page_url",
    "loaded_at",
    "source_event_at",
    *SILVER_LINEAGE_COLUMNS,
]


def web_event_row(**overrides: object) -> dict[str, object]:
    values = {
        "event_id": " EVT_001 ",
        "session_id": " SESS_001 ",
        "customer_id": " CUST_001 ",
        "anonymous_id": " ANON_001 ",
        "event_type": " PRODUCT_VIEW ",
        "event_timestamp": "2026-06-15 09:00:00",
        "product_id": " PROD_001 ",
        "campaign_id": " CMP_001 ",
        "device_type": " MOBILE ",
        "country_code": " pl ",
        "page_url": " /product/SKU-00001 ",
        "load_date": "2026-06-15",
        "loaded_at": "2026-06-15 10:00:00",
        "source_event_at": "2026-06-15 09:00:00",
    }
    values.update(overrides)
    return with_bronze_lineage(values, source_entity="web_events")


def test_web_events_normalizes_contract_fields(spark: SparkSession) -> None:
    result_df = transform_web_events_silver(spark.createDataFrame([web_event_row()]))
    row = result_df.first()

    assert result_df.columns == EXPECTED_COLUMNS
    assert row["event_id"] == "evt_001"
    assert row["event_type"] == "product_view"
    assert row["device_type"] == "mobile"
    assert row["country_code"] == "PL"
    assert row["page_url"] == "/product/SKU-00001"
    assert row["event_timestamp"] == datetime(2026, 6, 15, 9, 0, 0)
    assert_silver_lineage(row)


def test_web_events_keeps_latest_source_event_version(spark: SparkSession) -> None:
    result_df = transform_web_events_silver(
        spark.createDataFrame(
            [
                web_event_row(page_url="/old", source_event_at="2026-06-15 09:00:00"),
                web_event_row(page_url="/new", source_event_at="2026-06-15 09:01:00"),
            ]
        )
    )

    assert result_df.count() == 1
    assert result_df.first()["page_url"] == "/new"


@pytest.mark.parametrize(
    ("overrides", "expected_message"),
    [
        ({"device_type": "smart_tv"}, "invalid device type"),
        ({"product_id": ""}, "product events without product_id"),
    ],
)
def test_web_events_rejects_behavior_rule_violations(
    spark: SparkSession,
    overrides: dict[str, object],
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        transform_web_events_silver(spark.createDataFrame([web_event_row(**overrides)]))
