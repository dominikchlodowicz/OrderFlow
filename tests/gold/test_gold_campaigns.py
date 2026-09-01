from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from orderflow.gold.common import (
    NO_CAMPAIGN_ID,
    NO_CAMPAIGN_KEY,
    UNKNOWN_KEY,
    UNKNOWN_MEMBER_ID,
)
from orderflow.gold.dim_campaigns import transform_dim_campaigns

SILVER_CAMPAIGNS_SCHEMA = T.StructType(
    [
        T.StructField("campaign_id", T.StringType(), nullable=True),
        T.StructField("campaign_name", T.StringType(), nullable=True),
        T.StructField("source_channel", T.StringType(), nullable=True),
        T.StructField("start_date", T.DateType(), nullable=True),
        T.StructField("end_date", T.DateType(), nullable=True),
        T.StructField("budget_amount", T.DecimalType(18, 2), nullable=True),
        T.StructField("currency", T.StringType(), nullable=True),
        T.StructField("is_active", T.BooleanType(), nullable=True),
        T.StructField("created_at", T.TimestampType(), nullable=True),
        T.StructField("updated_at", T.TimestampType(), nullable=True),
    ]
)

EXPECTED_GOLD_COLUMNS = [
    "campaign_key",
    "campaign_id",
    "campaign_name",
    "source_channel",
    "start_date",
    "end_date",
    "budget_amount",
    "budget_currency_code",
    "is_active",
    "created_at",
    "updated_at",
    "_gold_processed_at",
]


def campaign_row(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "campaign_id": "cmp_summer",
        "campaign_name": "Summer Sale",
        "source_channel": "paid_search",
        "start_date": date(2026, 6, 1),
        "end_date": date(2026, 6, 30),
        "budget_amount": Decimal("5000.00"),
        "currency": "PLN",
        "is_active": True,
        "created_at": datetime(2026, 5, 20, 9, 0),
        "updated_at": datetime(2026, 5, 21, 10, 0),
    }
    values.update(overrides)
    return values


def create_silver_campaigns_df(
    spark: SparkSession,
    rows: list[dict[str, object]],
) -> DataFrame:
    return spark.createDataFrame(rows, schema=SILVER_CAMPAIGNS_SCHEMA)


def test_transform_dim_campaigns_matches_contract_and_preserves_attributes(
    spark: SparkSession,
) -> None:
    result_df = transform_dim_campaigns(create_silver_campaigns_df(spark, [campaign_row()]))
    result = result_df.filter(F.col("campaign_id") == "cmp_summer").first()

    assert result_df.columns == EXPECTED_GOLD_COLUMNS
    assert result is not None
    assert result["campaign_key"] >= 0
    assert result["budget_currency_code"] == "PLN"
    assert result["budget_amount"] == Decimal("5000.00")
    assert result["_gold_processed_at"] is not None
    assert result_df.schema["campaign_key"].dataType == T.LongType()
    assert result_df.schema["budget_amount"].dataType == T.DecimalType(18, 2)


def test_transform_dim_campaigns_adds_reserved_physical_members(
    spark: SparkSession,
) -> None:
    result_df = transform_dim_campaigns(create_silver_campaigns_df(spark, [campaign_row()]))
    special_members = {
        row["campaign_id"]: row for row in result_df.filter(F.col("campaign_key") < 0).collect()
    }

    assert {member_id: row["campaign_key"] for member_id, row in special_members.items()} == {
        UNKNOWN_MEMBER_ID: UNKNOWN_KEY,
        NO_CAMPAIGN_ID: NO_CAMPAIGN_KEY,
    }
    assert all(row["budget_amount"] == Decimal("0.00") for row in special_members.values())
    assert all(row["is_active"] is False for row in special_members.values())


@pytest.mark.parametrize(
    ("overrides", "expected_message"),
    [
        ({"budget_amount": Decimal("0.00")}, "non-positive campaign budgets"),
        ({"end_date": date(2026, 5, 31)}, "invalid campaign chronology"),
        (
            {"updated_at": datetime(2026, 5, 19, 9, 0)},
            "invalid campaign chronology",
        ),
        ({"currency": "pln"}, "invalid currency codes"),
    ],
)
def test_transform_dim_campaigns_rejects_contract_rule_violations(
    spark: SparkSession,
    overrides: dict[str, object],
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        transform_dim_campaigns(create_silver_campaigns_df(spark, [campaign_row(**overrides)]))


def test_transform_dim_campaigns_rejects_duplicate_ids(spark: SparkSession) -> None:
    silver_df = create_silver_campaigns_df(
        spark,
        [
            campaign_row(),
            campaign_row(campaign_name="Different name"),
        ],
    )

    with pytest.raises(ValueError, match="duplicate campaign_id values"):
        transform_dim_campaigns(silver_df)
