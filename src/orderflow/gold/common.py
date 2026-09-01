"""Shared transformation, validation, and persistence helpers for Gold models."""

from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from orderflow.common.delta import write_delta, write_delta_table

NON_NEGATIVE_BIGINT_MASK = (1 << 63) - 1

# Regular business keys are hashed into the non-negative BIGINT range. Negative
# values are therefore safe, stable members for explicitly documented fallbacks.
UNKNOWN_KEY = -1
GUEST_CUSTOMER_KEY = -2
ANONYMOUS_CUSTOMER_KEY = -3
NO_CAMPAIGN_KEY = -2
NOT_APPLICABLE_PRODUCT_KEY = -2

UNKNOWN_MEMBER_ID = "__unknown__"
GUEST_CUSTOMER_ID = "__guest__"
ANONYMOUS_CUSTOMER_ID = "__anonymous__"
NO_CAMPAIGN_ID = "__no_campaign__"
NOT_APPLICABLE_PRODUCT_ID = "__not_applicable__"


def surrogate_key(*columns: str | Column) -> Column:
    """Build a deterministic non-negative BIGINT key from one or more values."""
    key_columns = [F.col(column) if isinstance(column, str) else column for column in columns]
    return F.xxhash64(*key_columns).bitwiseAND(F.lit(NON_NEGATIVE_BIGINT_MASK))


def date_key(column: str | Column) -> Column:
    """Convert a date or timestamp to the Gold YYYYMMDD integer key."""
    value = F.col(column) if isinstance(column, str) else column
    return F.date_format(value, "yyyyMMdd").cast("int")


def with_gold_processed_at(df: DataFrame) -> DataFrame:
    return df.withColumn("_gold_processed_at", F.current_timestamp())


def validate_required_values(
    df: DataFrame,
    *,
    required_columns: Sequence[str],
    dataset_name: str,
) -> None:
    if not required_columns:
        return

    invalid_when = F.col(required_columns[0]).isNull()
    for column_name in required_columns[1:]:
        invalid_when = invalid_when | F.col(column_name).isNull()

    invalid_rows = df.filter(invalid_when).count()
    if invalid_rows > 0:
        raise ValueError(
            f"{dataset_name} Gold validation failed: "
            f"{invalid_rows} rows have null required fields."
        )


def validate_unique_key(
    df: DataFrame,
    *,
    key_columns: Sequence[str],
    dataset_name: str,
) -> None:
    duplicate_keys = df.groupBy(*key_columns).count().filter(F.col("count") > 1).count()
    if duplicate_keys > 0:
        key_name = ", ".join(key_columns)
        raise ValueError(
            f"{dataset_name} Gold validation failed: "
            f"{duplicate_keys} duplicate {key_name} values found."
        )


def validate_rule(
    df: DataFrame,
    *,
    invalid_when: Column,
    dataset_name: str,
    rule_description: str,
) -> None:
    invalid_rows = df.filter(invalid_when).count()
    if invalid_rows > 0:
        raise ValueError(
            f"{dataset_name} Gold validation failed: " f"{invalid_rows} rows {rule_description}."
        )


def write_gold(df: DataFrame, output_path: str | Path) -> None:
    write_delta(
        df=df,
        path=output_path,
        mode="overwrite",
        overwrite_schema=True,
    )


def write_gold_table(df: DataFrame, output_table: str) -> None:
    write_delta_table(
        df=df,
        table_name=output_table,
        mode="overwrite",
    )
