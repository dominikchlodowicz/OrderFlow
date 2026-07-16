from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from orderflow.gold.dim_calendar import (
    run_dim_calendar,
    transform_dim_calendar,
)


SILVER_CALENDAR_SCHEMA = T.StructType(
    [
        T.StructField("date_day", T.DateType(), nullable=True),
        T.StructField("year", T.IntegerType(), nullable=True),
        T.StructField("quarter", T.IntegerType(), nullable=True),
        T.StructField("month", T.IntegerType(), nullable=True),
        T.StructField("day_of_month", T.IntegerType(), nullable=True),
        T.StructField("day_of_week", T.IntegerType(), nullable=True),
        T.StructField("day_name", T.StringType(), nullable=True),
        T.StructField("week_of_year", T.IntegerType(), nullable=True),
        T.StructField("is_weekend", T.BooleanType(), nullable=True),
        T.StructField(
            "is_polish_public_holiday",
            T.BooleanType(),
            nullable=True,
        ),
        T.StructField("holiday_name", T.StringType(), nullable=True),

        # Silver pipeline metadata that should not be copied to Gold.
        T.StructField("source_load_date", T.DateType(), nullable=True),
        T.StructField("source_loaded_at", T.TimestampType(), nullable=True),
        T.StructField("_silver_processed_at", T.TimestampType(), nullable=True),
    ]
)


EXPECTED_GOLD_COLUMNS = [
    "date_key",
    "date_day",
    "year",
    "quarter",
    "month",
    "month_name",
    "day_of_month",
    "day_of_week",
    "day_name",
    "week_of_year",
    "is_weekend",
    "is_polish_public_holiday",
    "holiday_name",
    "_gold_processed_at",
]


def silver_calendar_row(
    *,
    date_day: date | None = date(2026, 6, 1),
    year: int = 2026,
    quarter: int = 2,
    month: int = 6,
    day_of_month: int = 1,
    day_of_week: int = 1,
    day_name: str = "Monday",
    week_of_year: int = 23,
    is_weekend: bool = False,
    is_polish_public_holiday: bool = False,
    holiday_name: str | None = None,
) -> dict[str, Any]:
    return {
        "date_day": date_day,
        "year": year,
        "quarter": quarter,
        "month": month,
        "day_of_month": day_of_month,
        "day_of_week": day_of_week,
        "day_name": day_name,
        "week_of_year": week_of_year,
        "is_weekend": is_weekend,
        "is_polish_public_holiday": is_polish_public_holiday,
        "holiday_name": holiday_name,
        "source_load_date": date(2026, 6, 1),
        "source_loaded_at": datetime(2026, 6, 1, 1, 0, 0),
        "_silver_processed_at": datetime(2026, 6, 1, 1, 5, 0),
    }


def create_silver_calendar_df(
    spark: SparkSession,
    rows: list[dict[str, Any]],
) -> DataFrame:
    return spark.createDataFrame(
        rows,
        schema=SILVER_CALENDAR_SCHEMA,
    )


def write_silver_calendar_delta(
    spark: SparkSession,
    path: Path,
    rows: list[dict[str, Any]],
) -> None:
    silver_df = create_silver_calendar_df(
        spark=spark,
        rows=rows,
    )

    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .save(str(path))
    )


def read_gold_dim_calendar_delta(
    spark: SparkSession,
    path: Path,
) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(str(path))
    )


def test_transform_dim_calendar_creates_analytical_columns(
    spark: SparkSession,
) -> None:
    silver_df = create_silver_calendar_df(
        spark=spark,
        rows=[
            silver_calendar_row(),
        ],
    )

    result_df = transform_dim_calendar(silver_df)

    assert result_df.columns == EXPECTED_GOLD_COLUMNS

    result = result_df.first()

    assert result is not None
    assert result["date_key"] == 20260601
    assert result["date_day"] == date(2026, 6, 1)
    assert result["month_name"] == "June"
    assert result["is_weekend"] is False
    assert result["is_polish_public_holiday"] is False
    assert result["holiday_name"] is None
    assert result["_gold_processed_at"] is not None


def test_transform_dim_calendar_excludes_silver_metadata(
    spark: SparkSession,
) -> None:
    silver_df = create_silver_calendar_df(
        spark=spark,
        rows=[
            silver_calendar_row(),
        ],
    )

    result_df = transform_dim_calendar(silver_df)

    excluded_columns = {
        "source_load_date",
        "source_loaded_at",
        "_silver_processed_at",
    }

    assert excluded_columns.isdisjoint(result_df.columns)


def test_transform_dim_calendar_preserves_holiday_information(
    spark: SparkSession,
) -> None:
    silver_df = create_silver_calendar_df(
        spark=spark,
        rows=[
            silver_calendar_row(
                date_day=date(2026, 1, 1),
                quarter=1,
                month=1,
                day_of_month=1,
                day_of_week=4,
                day_name="Thursday",
                week_of_year=1,
                is_polish_public_holiday=True,
                holiday_name="New Year's Day",
            ),
        ],
    )

    result = transform_dim_calendar(silver_df).first()

    assert result is not None
    assert result["date_key"] == 20260101
    assert result["month_name"] == "January"
    assert result["is_polish_public_holiday"] is True
    assert result["holiday_name"] == "New Year's Day"


def test_transform_dim_calendar_rejects_missing_required_columns(
    spark: SparkSession,
) -> None:
    silver_df = create_silver_calendar_df(
        spark=spark,
        rows=[
            silver_calendar_row(),
        ],
    ).drop("holiday_name")

    with pytest.raises(
        ValueError,
        match="Missing required Silver calendar columns",
    ):
        transform_dim_calendar(silver_df)


def test_transform_dim_calendar_rejects_null_date_key(
    spark: SparkSession,
) -> None:
    silver_df = create_silver_calendar_df(
        spark=spark,
        rows=[
            silver_calendar_row(
                date_day=None,
            ),
        ],
    )

    with pytest.raises(
        ValueError,
        match="rows have null keys",
    ):
        transform_dim_calendar(silver_df)


def test_transform_dim_calendar_rejects_duplicate_date_keys(
    spark: SparkSession,
) -> None:
    silver_df = create_silver_calendar_df(
        spark=spark,
        rows=[
            silver_calendar_row(),
            silver_calendar_row(),
        ],
    )

    with pytest.raises(
        ValueError,
        match="duplicate date_key values",
    ):
        transform_dim_calendar(silver_df)


def test_run_dim_calendar_writes_delta_table(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "silver_calendar"
    output_path = tmp_path / "gold_dim_calendar"

    write_silver_calendar_delta(
        spark=spark,
        path=input_path,
        rows=[
            silver_calendar_row(),
            silver_calendar_row(
                date_day=date(2026, 6, 2),
                day_of_month=2,
                day_of_week=2,
                day_name="Tuesday",
            ),
        ],
    )

    run_dim_calendar(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
    )

    result_df = read_gold_dim_calendar_delta(
        spark=spark,
        path=output_path,
    )

    assert result_df.count() == 2
    assert result_df.columns == EXPECTED_GOLD_COLUMNS

    result_by_key = {
        row["date_key"]: row
        for row in result_df.collect()
    }

    assert set(result_by_key) == {
        20260601,
        20260602,
    }

    assert result_by_key[20260601]["month_name"] == "June"
    assert result_by_key[20260602]["day_name"] == "Tuesday"


def test_run_dim_calendar_overwrites_existing_output_on_rerun(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "silver_calendar"
    output_path = tmp_path / "gold_dim_calendar"

    write_silver_calendar_delta(
        spark=spark,
        path=input_path,
        rows=[
            silver_calendar_row(),
            silver_calendar_row(
                date_day=date(2026, 6, 2),
                day_of_month=2,
                day_of_week=2,
                day_name="Tuesday",
            ),
        ],
    )

    run_dim_calendar(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
    )

    run_dim_calendar(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
    )

    result_df = read_gold_dim_calendar_delta(
        spark=spark,
        path=output_path,
    )

    assert result_df.count() == 2

    duplicate_key_count = (
        result_df.groupBy("date_key")
        .count()
        .filter("count > 1")
        .count()
    )

    assert duplicate_key_count == 0