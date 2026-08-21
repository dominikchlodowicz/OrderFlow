from datetime import date
from decimal import Decimal

import pytest
from helpers.silver import SILVER_LINEAGE_COLUMNS, assert_silver_lineage, with_bronze_lineage
from pyspark.sql import SparkSession

from orderflow.silver.marketing_campaigns import transform_marketing_campaigns_silver

EXPECTED_COLUMNS = [
    "campaign_id",
    "campaign_name",
    "source_channel",
    "start_date",
    "end_date",
    "budget_amount",
    "currency",
    "is_active",
    "created_at",
    "updated_at",
    "load_date",
    "loaded_at",
    "source_event_at",
    *SILVER_LINEAGE_COLUMNS,
]


def marketing_campaign_row(**overrides: object) -> dict[str, object]:
    values = {
        "campaign_id": " CMP_SUMMER ",
        "campaign_name": " Summer Sale ",
        "source_channel": " PAID_SEARCH ",
        "start_date": "2026-06-01",
        "end_date": "2026-06-30",
        "budget_amount": "5000.00",
        "currency": " pln ",
        "created_at": "2026-05-20 09:00:00",
        "updated_at": "2026-05-21 10:00:00",
        "is_active": "True",
        "load_date": "2026-06-15",
        "loaded_at": "2026-06-15 11:00:00",
        "source_event_at": "2026-05-21 10:00:00",
    }
    values.update(overrides)
    return with_bronze_lineage(values, source_entity="marketing_campaigns")


def test_marketing_campaigns_casts_and_normalizes_contract_fields(
    spark: SparkSession,
) -> None:
    result_df = transform_marketing_campaigns_silver(
        spark.createDataFrame([marketing_campaign_row()])
    )
    row = result_df.first()

    assert result_df.columns == EXPECTED_COLUMNS
    assert row["campaign_id"] == "cmp_summer"
    assert row["campaign_name"] == "Summer Sale"
    assert row["source_channel"] == "paid_search"
    assert row["start_date"] == date(2026, 6, 1)
    assert row["budget_amount"] == Decimal("5000.00")
    assert row["currency"] == "PLN"
    assert row["is_active"] is True
    assert_silver_lineage(row)


def test_marketing_campaigns_keeps_latest_updated_version(spark: SparkSession) -> None:
    result_df = transform_marketing_campaigns_silver(
        spark.createDataFrame(
            [
                marketing_campaign_row(
                    campaign_name="Old Name",
                    updated_at="2026-05-21 10:00:00",
                ),
                marketing_campaign_row(
                    campaign_name="New Name",
                    updated_at="2026-05-22 10:00:00",
                ),
            ]
        )
    )

    assert result_df.count() == 1
    assert result_df.first()["campaign_name"] == "New Name"


@pytest.mark.parametrize(
    ("overrides", "expected_message"),
    [
        ({"budget_amount": "0.00"}, "non-positive campaign budgets"),
        ({"end_date": "2026-05-31"}, "invalid campaign chronology"),
        ({"updated_at": "2026-05-19 09:00:00"}, "invalid campaign chronology"),
    ],
)
def test_marketing_campaigns_rejects_contract_rule_violations(
    spark: SparkSession,
    overrides: dict[str, object],
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        transform_marketing_campaigns_silver(
            spark.createDataFrame([marketing_campaign_row(**overrides)])
        )
