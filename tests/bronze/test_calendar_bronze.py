import csv
import hashlib
from pathlib import Path

from orderflow.bronze.calendar import CALENDAR_COLUMNS, run_calendar_bronze

STANDARD_BRONZE_METADATA_COLUMNS = [
    "_source_file_path",
    "_source_file_name",
    "_source_load_date",
    "_source_system",
    "_source_entity",
    "_ingestion_run_id",
    "_ingested_at",
    "_raw_record_hash",
]


def _calendar_row(
    *,
    date_day: str,
    day_of_month: str,
    day_of_week: str,
    day_name: str,
    week_of_year: str,
    is_weekend: str,
    is_polish_public_holiday: str,
    holiday_name: str,
) -> dict[str, str]:
    return {
        "date_day": date_day,
        "year": "2026",
        "quarter": "2",
        "month": date_day[5:7].lstrip("0"),
        "day_of_month": day_of_month,
        "day_of_week": day_of_week,
        "day_name": day_name,
        "week_of_year": week_of_year,
        "is_weekend": is_weekend,
        "is_polish_public_holiday": is_polish_public_holiday,
        "holiday_name": holiday_name,
        "load_date": date_day,
        "loaded_at": f"{date_day} 01:00:00",
    }


def _write_calendar_csv(csv_path: Path, row: dict[str, str]) -> None:
    with csv_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=CALENDAR_COLUMNS)
        writer.writeheader()
        writer.writerow(row)


def _expected_raw_record_hash(row: dict[str, str]) -> str:
    payload = "||".join(row[column] for column in CALENDAR_COLUMNS)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def test_run_calendar_bronze_writes_delta_table(spark, tmp_path: Path) -> None:
    load_date = "2026-05-01"
    input_path = tmp_path / "landing" / "calendar" / f"load_date={load_date}"
    output_path = tmp_path / "bronze" / "calendar"

    input_path.mkdir(parents=True)

    csv_path = input_path / f"calendar_{load_date}.csv"
    expected_raw_row = _calendar_row(
        date_day=load_date,
        day_of_month="1",
        day_of_week="5",
        day_name="Friday",
        week_of_year="18",
        is_weekend="False",
        is_polish_public_holiday="True",
        holiday_name="Labour Day",
    )
    _write_calendar_csv(csv_path, expected_raw_row)

    run_calendar_bronze(
        spark=spark,
        input_path=input_path.parent,
        output_path=output_path,
    )

    result_df = spark.read.format("delta").load(str(output_path))

    assert result_df.count() == 1

    assert set(result_df.columns) == set(CALENDAR_COLUMNS + STANDARD_BRONZE_METADATA_COLUMNS)

    row = result_df.first().asDict()

    for column_name, expected_value in expected_raw_row.items():
        assert row[column_name] == expected_value

    assert row["_source_load_date"] == load_date
    assert row["_source_file_name"] == f"calendar_{load_date}.csv"
    assert row["_source_file_path"].endswith(f"load_date={load_date}/calendar_{load_date}.csv")
    assert row["_source_system"] == "local_files"
    assert row["_source_entity"] == "calendar"
    assert row["_ingestion_run_id"] == "manual-local-run"
    assert row["_ingested_at"] is not None
    assert row["_raw_record_hash"] == _expected_raw_record_hash(expected_raw_row)


def test_run_calendar_bronze_extracts_multiple_load_dates(
    spark,
    tmp_path: Path,
) -> None:
    base_input_path = tmp_path / "landing" / "calendar"
    output_path = tmp_path / "bronze" / "calendar"

    first_load_date = "2026-05-01"
    second_load_date = "2026-05-03"
    first_day = base_input_path / f"load_date={first_load_date}"
    second_day = base_input_path / f"load_date={second_load_date}"

    first_day.mkdir(parents=True)
    second_day.mkdir(parents=True)

    _write_calendar_csv(
        first_day / f"calendar_{first_load_date}.csv",
        _calendar_row(
            date_day=first_load_date,
            day_of_month="1",
            day_of_week="5",
            day_name="Friday",
            week_of_year="18",
            is_weekend="False",
            is_polish_public_holiday="True",
            holiday_name="Labour Day",
        ),
    )
    _write_calendar_csv(
        second_day / f"calendar_{second_load_date}.csv",
        _calendar_row(
            date_day=second_load_date,
            day_of_month="3",
            day_of_week="7",
            day_name="Sunday",
            week_of_year="18",
            is_weekend="True",
            is_polish_public_holiday="True",
            holiday_name="Constitution Day",
        ),
    )

    run_calendar_bronze(
        spark=spark,
        input_path=base_input_path,
        output_path=output_path,
    )

    result_df = spark.read.format("delta").load(str(output_path))

    load_dates = {
        row["_source_load_date"]
        for row in result_df.select("_source_load_date").distinct().collect()
    }

    assert load_dates == {first_load_date, second_load_date}
    assert result_df.count() == 2


def test_run_calendar_bronze_rerun_does_not_duplicate_rows(
    spark,
    tmp_path: Path,
) -> None:
    load_date = "2026-05-01"
    input_path = tmp_path / "landing" / "calendar" / f"load_date={load_date}"
    output_path = tmp_path / "bronze" / "calendar"

    input_path.mkdir(parents=True)

    _write_calendar_csv(
        input_path / f"calendar_{load_date}.csv",
        _calendar_row(
            date_day=load_date,
            day_of_month="1",
            day_of_week="5",
            day_name="Friday",
            week_of_year="18",
            is_weekend="False",
            is_polish_public_holiday="True",
            holiday_name="Labour Day",
        ),
    )

    run_calendar_bronze(
        spark=spark,
        input_path=input_path.parent,
        output_path=output_path,
    )

    run_calendar_bronze(
        spark=spark,
        input_path=input_path.parent,
        output_path=output_path,
    )

    result_df = spark.read.format("delta").load(str(output_path))

    assert result_df.count() == 1
