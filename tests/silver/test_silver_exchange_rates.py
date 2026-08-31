from datetime import date, datetime
from decimal import Decimal

import pytest
from helpers.silver import SILVER_LINEAGE_COLUMNS, assert_silver_lineage, with_bronze_lineage
from pyspark.sql import SparkSession

from orderflow.silver.exchange_rates import transform_exchange_rates_silver

EXPECTED_COLUMNS = [
    "rate_date",
    "currency",
    "rate_to_pln",
    "source",
    "load_date",
    "loaded_at",
    *SILVER_LINEAGE_COLUMNS,
]


def exchange_rate_row(**overrides: object) -> dict[str, object]:
    values = {
        "rate_date": "2026-06-15",
        "currency": " eur ",
        "rate_to_pln": "4.3210",
        "source": " Synthetic NBP ",
        "load_date": "2026-06-15",
        "loaded_at": "2026-06-15 02:00:00",
    }
    values.update(overrides)
    return with_bronze_lineage(values, source_entity="exchange_rates")


def test_exchange_rates_casts_rounds_and_normalizes_contract_fields(
    spark: SparkSession,
) -> None:
    result_df = transform_exchange_rates_silver(
        spark.createDataFrame([exchange_rate_row()])
    )
    row = result_df.first()

    assert result_df.columns == EXPECTED_COLUMNS
    assert row["rate_date"] == date(2026, 6, 15)
    assert row["currency"] == "EUR"
    assert row["rate_to_pln"] == Decimal("4.32")
    assert row["source"] == "Synthetic NBP"
    assert row["loaded_at"] == datetime(2026, 6, 15, 2, 0, 0)
    assert_silver_lineage(row)


def test_exchange_rates_keeps_latest_grain_version(spark: SparkSession) -> None:
    result_df = transform_exchange_rates_silver(
        spark.createDataFrame(
            [
                exchange_rate_row(rate_to_pln="4.31", loaded_at="2026-06-15 01:00:00"),
                exchange_rate_row(rate_to_pln="4.32", loaded_at="2026-06-15 02:00:00"),
            ]
        )
    )

    assert result_df.count() == 1
    assert result_df.first()["rate_to_pln"] == Decimal("4.32")


@pytest.mark.parametrize(
    ("rate_to_pln", "expected_message"),
    [
        ("-0.01", "negative exchange rates"),
        ("not-a-number", "null required fields"),
    ],
)
def test_exchange_rates_rejects_invalid_rate_values(
    spark: SparkSession,
    rate_to_pln: str,
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        transform_exchange_rates_silver(
            spark.createDataFrame([exchange_rate_row(rate_to_pln=rate_to_pln)])
        )
