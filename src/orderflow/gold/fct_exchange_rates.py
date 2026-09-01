from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from orderflow.common.delta import read_delta, read_delta_table
from orderflow.common.validation import validate_required_columns
from orderflow.gold.common import (
    surrogate_key,
    validate_required_values,
    validate_rule,
    validate_unique_key,
    with_gold_processed_at,
    write_gold,
    write_gold_table,
)

RATE_DECIMAL_TYPE = "decimal(18,6)"

SILVER_EXCHANGE_RATES_REQUIRED_COLUMNS = [
    "rate_date",
    "currency",
    "rate_to_pln",
    "source",
]
DIM_CURRENCY_REQUIRED_COLUMNS = ["currency_key", "currency_code"]
DIM_CALENDAR_REQUIRED_COLUMNS = ["date_key", "date_day"]

FCT_EXCHANGE_RATES_REQUIRED_COLUMNS = [
    "exchange_rate_key",
    "rate_date_key",
    "currency_key",
    "rate_to_pln",
    "source",
    "_gold_processed_at",
]


def transform_fct_exchange_rates(
    *,
    silver_exchange_rates_df: DataFrame,
    dim_currency_df: DataFrame,
    dim_calendar_df: DataFrame,
) -> DataFrame:
    """Build the periodic exchange-rate fact at date, currency, and source grain."""
    validate_required_columns(
        silver_exchange_rates_df,
        SILVER_EXCHANGE_RATES_REQUIRED_COLUMNS,
        dataset_name="Silver exchange_rates",
    )
    validate_required_columns(
        dim_currency_df,
        DIM_CURRENCY_REQUIRED_COLUMNS,
        dataset_name="Gold dim_currency",
    )
    validate_required_columns(
        dim_calendar_df,
        DIM_CALENDAR_REQUIRED_COLUMNS,
        dataset_name="Gold dim_calendar",
    )

    rates = silver_exchange_rates_df.select(*SILVER_EXCHANGE_RATES_REQUIRED_COLUMNS)
    currencies = dim_currency_df.select(
        F.col("currency_code").alias("_dimension_currency_code"),
        F.col("currency_key").alias("_resolved_currency_key"),
    )
    calendar = dim_calendar_df.select("date_day", "date_key")

    joined = (
        rates.alias("rate")
        .join(
            currencies.alias("currency"),
            F.col("rate.currency") == F.col("currency._dimension_currency_code"),
            "left",
        )
        .join(
            calendar.alias("rate_calendar"),
            F.col("rate.rate_date") == F.col("rate_calendar.date_day"),
            "left",
        )
    )
    resolved = joined.select(
        "rate.*",
        F.col("currency._resolved_currency_key").alias("_resolved_currency_key"),
        F.col("rate_calendar.date_key").alias("_resolved_rate_date_key"),
    )

    _validate_exchange_rate_lookups(resolved)

    fact_df = resolved.select(
        surrogate_key(
            F.col("_resolved_rate_date_key"),
            F.col("_resolved_currency_key"),
            F.col("source"),
        ).alias("exchange_rate_key"),
        F.col("_resolved_rate_date_key").alias("rate_date_key"),
        F.col("_resolved_currency_key").alias("currency_key"),
        F.col("rate_to_pln").cast(RATE_DECIMAL_TYPE).alias("rate_to_pln"),
        F.col("source"),
    )
    fact_df = with_gold_processed_at(fact_df)
    validate_fct_exchange_rates(fact_df)
    return fact_df


def _validate_exchange_rate_lookups(df: DataFrame) -> None:
    validate_rule(
        df,
        invalid_when=F.col("rate_date").isNull() | F.col("_resolved_rate_date_key").isNull(),
        dataset_name="Exchange rates",
        rule_description="have rate dates missing from dim_calendar",
    )
    validate_rule(
        df,
        invalid_when=F.col("currency").isNull() | F.col("_resolved_currency_key").isNull(),
        dataset_name="Exchange rates",
        rule_description="have currencies missing from dim_currency",
    )


def validate_fct_exchange_rates(df: DataFrame) -> None:
    validate_required_values(
        df,
        required_columns=FCT_EXCHANGE_RATES_REQUIRED_COLUMNS,
        dataset_name="Exchange rates",
    )
    validate_rule(
        df,
        invalid_when=F.col("rate_to_pln") <= 0,
        dataset_name="Exchange rates",
        rule_description="have non-positive exchange rates",
    )
    validate_unique_key(
        df,
        key_columns=["rate_date_key", "currency_key", "source"],
        dataset_name="Exchange rates",
    )
    validate_unique_key(
        df,
        key_columns=["exchange_rate_key"],
        dataset_name="Exchange rates",
    )


def write_fct_exchange_rates(
    fct_exchange_rates_df: DataFrame,
    output_path: str | Path,
) -> None:
    write_gold(fct_exchange_rates_df, output_path)


def write_fct_exchange_rates_table(
    fct_exchange_rates_df: DataFrame,
    output_table: str,
) -> None:
    write_gold_table(fct_exchange_rates_df, output_table)


def run_fct_exchange_rates(
    spark: SparkSession,
    *,
    exchange_rates_input_path: str | Path,
    currency_input_path: str | Path,
    calendar_input_path: str | Path,
    output_path: str | Path,
) -> None:
    fact_df = transform_fct_exchange_rates(
        silver_exchange_rates_df=read_delta(spark, exchange_rates_input_path),
        dim_currency_df=read_delta(spark, currency_input_path),
        dim_calendar_df=read_delta(spark, calendar_input_path),
    )
    write_fct_exchange_rates(fact_df, output_path)


def run_fct_exchange_rates_tables(
    spark: SparkSession,
    *,
    exchange_rates_input_table: str,
    currency_input_table: str,
    calendar_input_table: str,
    output_table: str,
) -> None:
    fact_df = transform_fct_exchange_rates(
        silver_exchange_rates_df=read_delta_table(spark, exchange_rates_input_table),
        dim_currency_df=read_delta_table(spark, currency_input_table),
        dim_calendar_df=read_delta_table(spark, calendar_input_table),
    )
    write_fct_exchange_rates_table(fact_df, output_table)
