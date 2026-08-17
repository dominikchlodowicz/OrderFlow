from datetime import date, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from orderflow.silver.calendar import run_calendar_silver

EXPECTED_SILVER_COLUMNS = [
    "date_day",
    "year",
    "quarter",
    "month",
    "day_of_month",
    "day_of_week",
    "day_name",
    "week_of_year",
    "is_weekend",
    "is_polish_public_holiday",
    "holiday_name",
    "_source_file_name",
    "_source_file_path",
    "_ingestion_run_id",
    "_bronze_ingested_at",
    "_raw_record_hash",
    "_silver_processed_at",
]


def write_bronze_calendar_delta(
    spark: SparkSession,
    path: Path,
    rows: list[dict[str, object]],
) -> None:
    bronze_df = spark.createDataFrame(rows)

    (bronze_df.write.format("delta").mode("overwrite").save(str(path)))


def read_silver_calendar_delta(
    spark: SparkSession,
    path: Path,
) -> DataFrame:
    return spark.read.format("delta").load(str(path))


def calendar_row(
    *,
    date_day: str = "2026-06-01",
    year: str = "2026",
    quarter: str = "2",
    month: str = "6",
    day_of_month: str = "1",
    day_of_week: str = "1",
    day_name: str = "Monday",
    week_of_year: str = "23",
    is_weekend: str = "False",
    is_polish_public_holiday: str = "False",
    holiday_name: str = "",
    load_date: str = "2026-06-01",
    loaded_at: str = "2026-06-01 01:00:00",
    source_file_name: str = "calendar.csv",
    source_file_path: str = "/Volumes/orderflow_dev/bronze/landing/calendar.csv",
    ingestion_run_id: str = "run-001",
    ingested_at: datetime = datetime(2026, 6, 1, 1, 5, 0),
    raw_record_hash: str = "calendar-hash",
) -> dict[str, object]:
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
        "load_date": load_date,
        "loaded_at": loaded_at,
        "_source_file_name": source_file_name,
        "_source_file_path": source_file_path,
        "_source_load_date": date.fromisoformat(load_date),
        "_ingestion_run_id": ingestion_run_id,
        "_ingested_at": ingested_at,
        "_raw_record_hash": raw_record_hash,
    }


def test_run_calendar_silver_writes_typed_delta_table(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    bronze_path = tmp_path / "bronze" / "calendar"
    silver_path = tmp_path / "silver" / "calendar"

    write_bronze_calendar_delta(
        spark=spark,
        path=bronze_path,
        rows=[
            calendar_row(),
        ],
    )

    run_calendar_silver(
        spark=spark,
        input_path=bronze_path,
        output_path=silver_path,
    )

    result_df = read_silver_calendar_delta(
        spark=spark,
        path=silver_path,
    )

    assert result_df.count() == 1
    assert result_df.columns == EXPECTED_SILVER_COLUMNS

    schema = result_df.schema

    assert isinstance(schema["date_day"].dataType, T.DateType)
    assert isinstance(schema["year"].dataType, T.IntegerType)
    assert isinstance(schema["quarter"].dataType, T.IntegerType)
    assert isinstance(schema["month"].dataType, T.IntegerType)
    assert isinstance(schema["day_of_month"].dataType, T.IntegerType)
    assert isinstance(schema["day_of_week"].dataType, T.IntegerType)
    assert isinstance(schema["day_name"].dataType, T.StringType)
    assert isinstance(schema["week_of_year"].dataType, T.IntegerType)
    assert isinstance(schema["is_weekend"].dataType, T.BooleanType)
    assert isinstance(schema["is_polish_public_holiday"].dataType, T.BooleanType)
    assert isinstance(schema["holiday_name"].dataType, T.StringType)
    assert isinstance(schema["_source_file_name"].dataType, T.StringType)
    assert isinstance(schema["_source_file_path"].dataType, T.StringType)
    assert isinstance(schema["_ingestion_run_id"].dataType, T.StringType)
    assert isinstance(schema["_bronze_ingested_at"].dataType, T.TimestampType)
    assert isinstance(schema["_raw_record_hash"].dataType, T.StringType)
    assert isinstance(schema["_silver_processed_at"].dataType, T.TimestampType)

    row = result_df.first()

    assert row["date_day"] == date(2026, 6, 1)
    assert row["year"] == 2026
    assert row["quarter"] == 2
    assert row["month"] == 6
    assert row["day_of_month"] == 1
    assert row["day_of_week"] == 1
    assert row["day_name"] == "Monday"
    assert row["week_of_year"] == 23
    assert row["is_weekend"] is False
    assert row["is_polish_public_holiday"] is False
    assert row["holiday_name"] is None
    assert row["_source_file_name"] == "calendar.csv"
    assert row["_bronze_ingested_at"] == datetime(2026, 6, 1, 1, 5, 0)


def test_run_calendar_silver_preserves_weekend_and_holiday_values(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    bronze_path = tmp_path / "bronze" / "calendar"
    silver_path = tmp_path / "silver" / "calendar"

    write_bronze_calendar_delta(
        spark=spark,
        path=bronze_path,
        rows=[
            calendar_row(
                date_day="2026-06-06",
                day_of_month="6",
                day_of_week="6",
                day_name="Saturday",
                is_weekend="True",
                is_polish_public_holiday="False",
                load_date="2026-06-06",
                loaded_at="2026-06-06 01:00:00",
            ),
            calendar_row(
                date_day="2026-06-07",
                day_of_month="7",
                day_of_week="7",
                day_name="Sunday",
                is_weekend="True",
                is_polish_public_holiday="False",
                load_date="2026-06-07",
                loaded_at="2026-06-07 01:00:00",
            ),
        ],
    )

    run_calendar_silver(
        spark=spark,
        input_path=bronze_path,
        output_path=silver_path,
    )

    result_df = read_silver_calendar_delta(
        spark=spark,
        path=silver_path,
    )

    rows = {row["date_day"]: row for row in result_df.collect()}

    assert rows[date(2026, 6, 6)]["is_weekend"] is True
    assert rows[date(2026, 6, 7)]["is_weekend"] is True
    assert result_df.count() == 2


def test_run_calendar_silver_deduplicates_by_date_day_using_latest_loaded_at(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    bronze_path = tmp_path / "bronze" / "calendar"
    silver_path = tmp_path / "silver" / "calendar"

    write_bronze_calendar_delta(
        spark=spark,
        path=bronze_path,
        rows=[
            calendar_row(
                date_day="2026-06-01",
                day_name="Old Monday",
                loaded_at="2026-06-01 01:00:00",
                source_file_name="calendar-old.csv",
                raw_record_hash="old-hash",
            ),
            calendar_row(
                date_day="2026-06-01",
                day_name="Monday",
                loaded_at="2026-06-01 02:00:00",
                source_file_name="calendar-new.csv",
                raw_record_hash="new-hash",
            ),
        ],
    )

    run_calendar_silver(
        spark=spark,
        input_path=bronze_path,
        output_path=silver_path,
    )

    result_df = read_silver_calendar_delta(
        spark=spark,
        path=silver_path,
    )

    assert result_df.count() == 1

    row = result_df.first()

    assert row["date_day"] == date(2026, 6, 1)
    assert row["day_name"] == "Monday"
    assert row["_source_file_name"] == "calendar-new.csv"
    assert row["_raw_record_hash"] == "new-hash"


def test_run_calendar_silver_rejects_invalid_calendar_values(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    bronze_path = tmp_path / "bronze" / "calendar"
    silver_path = tmp_path / "silver" / "calendar"

    write_bronze_calendar_delta(
        spark=spark,
        path=bronze_path,
        rows=[
            calendar_row(
                month="13",
            ),
        ],
    )

    with pytest.raises(ValueError, match="invalid calendar values"):
        run_calendar_silver(
            spark=spark,
            input_path=bronze_path,
            output_path=silver_path,
        )


def test_run_calendar_silver_rejects_null_required_values_after_casting(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    bronze_path = tmp_path / "bronze" / "calendar"
    silver_path = tmp_path / "silver" / "calendar"

    write_bronze_calendar_delta(
        spark=spark,
        path=bronze_path,
        rows=[
            calendar_row(
                date_day="not-a-date",
            ),
        ],
    )

    with pytest.raises(ValueError, match="null required fields"):
        run_calendar_silver(
            spark=spark,
            input_path=bronze_path,
            output_path=silver_path,
        )


def test_run_calendar_silver_rerun_does_not_duplicate_rows(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    bronze_path = tmp_path / "bronze" / "calendar"
    silver_path = tmp_path / "silver" / "calendar"

    write_bronze_calendar_delta(
        spark=spark,
        path=bronze_path,
        rows=[
            calendar_row(),
            calendar_row(
                date_day="2026-06-02",
                day_of_month="2",
                day_of_week="2",
                day_name="Tuesday",
                load_date="2026-06-02",
                loaded_at="2026-06-02 01:00:00",
            ),
        ],
    )

    run_calendar_silver(
        spark=spark,
        input_path=bronze_path,
        output_path=silver_path,
    )

    run_calendar_silver(
        spark=spark,
        input_path=bronze_path,
        output_path=silver_path,
    )

    result_df = read_silver_calendar_delta(
        spark=spark,
        path=silver_path,
    )

    assert result_df.count() == 2
