from datetime import date, datetime
from decimal import Decimal

import pytest
from helpers.gold import calendar_dimension_df, currency_dimension_df
from helpers.silver import SILVER_LINEAGE_COLUMNS
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from orderflow.gold.fct_exchange_rates import transform_fct_exchange_rates

SILVER_EXCHANGE_RATES_SCHEMA = T.StructType(
    [
        T.StructField("rate_date", T.DateType(), nullable=True),
        T.StructField("currency", T.StringType(), nullable=True),
        T.StructField("rate_to_pln", T.DecimalType(18, 2), nullable=True),
        T.StructField("source", T.StringType(), nullable=True),
        T.StructField("_source_file_name", T.StringType(), nullable=False),
        T.StructField("_source_file_path", T.StringType(), nullable=False),
        T.StructField("_ingestion_run_id", T.StringType(), nullable=False),
        T.StructField("_bronze_ingested_at", T.TimestampType(), nullable=False),
        T.StructField("_raw_record_hash", T.StringType(), nullable=False),
        T.StructField("_silver_processed_at", T.TimestampType(), nullable=False),
    ]
)

EXPECTED_COLUMNS = [
    "exchange_rate_key",
    "rate_date_key",
    "currency_key",
    "rate_to_pln",
    "source",
    "_gold_processed_at",
]

EXPECTED_TYPES = [
    T.LongType(),
    T.IntegerType(),
    T.LongType(),
    T.DecimalType(18, 6),
    T.StringType(),
    T.TimestampType(),
]


def exchange_rate_row(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "rate_date": date(2026, 6, 10),
        "currency": "EUR",
        "rate_to_pln": Decimal("4.32"),
        "source": "NBP",
        "_source_file_name": "exchange_rates.csv",
        "_source_file_path": "/landing/exchange_rates.csv",
        "_ingestion_run_id": "run-001",
        "_bronze_ingested_at": datetime(2026, 6, 10, 2, 0),
        "_raw_record_hash": "exchange-rate-hash",
        "_silver_processed_at": datetime(2026, 6, 10, 2, 5),
    }
    values.update(overrides)
    return values


def create_exchange_rates_df(
    spark: SparkSession,
    rows: list[dict[str, object]],
) -> DataFrame:
    return spark.createDataFrame(rows, schema=SILVER_EXCHANGE_RATES_SCHEMA)


def transform_exchange_rates(
    spark: SparkSession,
    rows: list[dict[str, object]],
) -> DataFrame:
    return transform_fct_exchange_rates(
        silver_exchange_rates_df=create_exchange_rates_df(spark, rows),
        dim_currency_df=currency_dimension_df(spark),
        dim_calendar_df=calendar_dimension_df(spark),
    )


def test_transform_fct_exchange_rates_matches_contract_and_excludes_silver_lineage(
    spark: SparkSession,
) -> None:
    result_df = transform_exchange_rates(spark, [exchange_rate_row()])
    result = result_df.first()

    assert result is not None
    assert result_df.columns == EXPECTED_COLUMNS
    assert [field.dataType for field in result_df.schema] == EXPECTED_TYPES
    assert set(SILVER_LINEAGE_COLUMNS).isdisjoint(result_df.columns)
    assert result["exchange_rate_key"] >= 0
    assert result["rate_date_key"] == 20260610
    assert result["currency_key"] == 402
    assert result["rate_to_pln"] == Decimal("4.320000")
    assert result["source"] == "NBP"
    assert result["_gold_processed_at"] is not None


def test_transform_fct_exchange_rates_builds_stable_composite_grain_keys(
    spark: SparkSession,
) -> None:
    rows = [
        exchange_rate_row(),
        exchange_rate_row(
            rate_date=date(2026, 6, 11),
            _raw_record_hash="different-date",
        ),
        exchange_rate_row(
            currency="PLN",
            rate_to_pln=Decimal("1.00"),
            _raw_record_hash="different-currency",
        ),
        exchange_rate_row(
            source="ECB",
            _raw_record_hash="different-source",
        ),
    ]

    def keys_by_grain() -> dict[tuple[int, int, str], int]:
        return {
            (row["rate_date_key"], row["currency_key"], row["source"]): row["exchange_rate_key"]
            for row in transform_exchange_rates(spark, rows).collect()
        }

    first_keys = keys_by_grain()
    second_keys = keys_by_grain()

    assert first_keys == second_keys
    assert len(first_keys) == 4
    assert len(set(first_keys.values())) == 4


def test_transform_fct_exchange_rates_rejects_duplicate_composite_grain(
    spark: SparkSession,
) -> None:
    with pytest.raises(
        ValueError,
        match="duplicate rate_date_key, currency_key, source values",
    ):
        transform_exchange_rates(
            spark,
            [
                exchange_rate_row(),
                exchange_rate_row(
                    rate_to_pln=Decimal("4.33"),
                    _raw_record_hash="duplicate-grain",
                ),
            ],
        )


@pytest.mark.parametrize(
    ("overrides", "expected_message"),
    [
        ({"currency": "USD"}, "currencies missing from dim_currency"),
        ({"currency": None}, "currencies missing from dim_currency"),
        ({"rate_date": date(2026, 6, 30)}, "rate dates missing from dim_calendar"),
        ({"rate_date": None}, "rate dates missing from dim_calendar"),
        ({"source": None}, "null required fields"),
    ],
)
def test_transform_fct_exchange_rates_rejects_missing_dimensions_or_grain_values(
    spark: SparkSession,
    overrides: dict[str, object],
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        transform_exchange_rates(
            spark,
            [exchange_rate_row(**overrides)],
        )


@pytest.mark.parametrize(
    "rate_to_pln",
    [
        pytest.param(Decimal("0.00"), id="zero"),
        pytest.param(Decimal("-0.01"), id="negative"),
    ],
)
def test_transform_fct_exchange_rates_rejects_non_positive_rates(
    spark: SparkSession,
    rate_to_pln: Decimal,
) -> None:
    with pytest.raises(ValueError, match="non-positive exchange rates"):
        transform_exchange_rates(
            spark,
            [exchange_rate_row(rate_to_pln=rate_to_pln)],
        )
