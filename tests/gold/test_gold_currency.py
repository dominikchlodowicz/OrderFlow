import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from orderflow.gold.dim_currency import transform_dim_currency

EXPECTED_GOLD_COLUMNS = [
    "currency_key",
    "currency_code",
    "is_reporting_currency",
    "_gold_processed_at",
]


def currency_df(spark: SparkSession, *currencies: str | None) -> DataFrame:
    schema = T.StructType([T.StructField("currency", T.StringType(), nullable=True)])
    return spark.createDataFrame([(currency,) for currency in currencies], schema=schema)


def transform_currencies(
    spark: SparkSession,
    *,
    products: tuple[str | None, ...] = ("PLN", "EUR"),
    campaigns: tuple[str | None, ...] = ("PLN",),
    orders: tuple[str | None, ...] = ("USD",),
    payments: tuple[str | None, ...] = ("EUR",),
    refunds: tuple[str | None, ...] = ("GBP",),
    exchange_rates: tuple[str | None, ...] = ("CHF",),
) -> DataFrame:
    return transform_dim_currency(
        currency_df(spark, *products),
        currency_df(spark, *campaigns),
        currency_df(spark, *orders),
        currency_df(spark, *payments),
        currency_df(spark, *refunds),
        currency_df(spark, *exchange_rates),
    )


def test_transform_dim_currency_unions_distinct_codes_from_all_six_sources(
    spark: SparkSession,
) -> None:
    result_df = transform_currencies(spark)
    rows_by_code = {row["currency_code"]: row for row in result_df.collect()}

    assert result_df.columns == EXPECTED_GOLD_COLUMNS
    assert set(rows_by_code) == {"PLN", "EUR", "USD", "GBP", "CHF"}
    assert rows_by_code["PLN"]["is_reporting_currency"] is True
    assert all(
        row["is_reporting_currency"] is False for code, row in rows_by_code.items() if code != "PLN"
    )
    assert all(row["currency_key"] >= 0 for row in rows_by_code.values())
    assert all(row["_gold_processed_at"] is not None for row in rows_by_code.values())
    assert result_df.schema["currency_key"].dataType == T.LongType()


def test_transform_dim_currency_normalizes_codes_before_deduplication(
    spark: SparkSession,
) -> None:
    result_df = transform_currencies(
        spark,
        products=(" pln ", "EUR"),
        campaigns=("PlN",),
    )

    assert result_df.filter("currency_code = 'PLN'").count() == 1


def test_transform_dim_currency_generates_stable_keys(spark: SparkSession) -> None:
    first_keys = {
        row["currency_code"]: row["currency_key"] for row in transform_currencies(spark).collect()
    }
    second_keys = {
        row["currency_code"]: row["currency_key"] for row in transform_currencies(spark).collect()
    }

    assert first_keys == second_keys


@pytest.mark.parametrize(
    ("products", "expected_message"),
    [
        ((None,), "null required fields"),
        (("EU",), "invalid currency codes"),
    ],
)
def test_transform_dim_currency_rejects_invalid_codes(
    spark: SparkSession,
    products: tuple[str | None, ...],
    expected_message: str,
) -> None:
    with pytest.raises(ValueError, match=expected_message):
        transform_currencies(spark, products=products)


def test_transform_dim_currency_rejects_missing_currency_column(
    spark: SparkSession,
) -> None:
    products_df = spark.createDataFrame([("prod_001",)], ["product_id"])

    with pytest.raises(ValueError, match="Silver products.*missing required columns"):
        transform_dim_currency(
            products_df,
            currency_df(spark, "PLN"),
            currency_df(spark, "PLN"),
            currency_df(spark, "PLN"),
            currency_df(spark, "PLN"),
            currency_df(spark, "PLN"),
        )
