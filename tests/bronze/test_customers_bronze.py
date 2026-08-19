import csv
import hashlib
from datetime import date
from pathlib import Path

from helpers.constants import STANDARD_BRONZE_METADATA_COLUMNS
from pyspark.sql import types as T

from orderflow.bronze.customers import CUSTOMERS_COLUMNS, run_customers_bronze


def _customers_row(
    *,
    customer_id: str,
    email: str,
    first_name: str,
    last_name: str,
    country_code: str,
    city: str,
    load_date: str,
    customer_status: str = "active",
    marketing_consent: str = "True",
) -> dict[str, str]:
    created_at = "2025-01-15 09:30:00"

    return {
        "customer_id": customer_id,
        "email": email,
        "first_name": first_name,
        "last_name": last_name,
        "country_code": country_code,
        "city": city,
        "created_at": created_at,
        "updated_at": created_at,
        "customer_status": customer_status,
        "marketing_consent": marketing_consent,
        "load_date": load_date,
        "loaded_at": f"{load_date} 01:00:00",
        "source_event_at": created_at,
    }


def _write_customers_csv(csv_path: Path, row: dict[str, str]) -> None:
    with csv_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=CUSTOMERS_COLUMNS)
        writer.writeheader()
        writer.writerow(row)


def _expected_raw_record_hash(row: dict[str, str]) -> str:
    payload = "||".join(row[column] for column in CUSTOMERS_COLUMNS)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def test_run_customers_bronze_writes_delta_table(spark, tmp_path: Path) -> None:
    load_date = "2026-06-01"
    input_path = tmp_path / "landing" / "customers" / f"load_date={load_date}"
    output_path = tmp_path / "bronze" / "customers"

    input_path.mkdir(parents=True)

    csv_path = input_path / "customers.csv"
    expected_raw_row = _customers_row(
        customer_id="cust_001",
        email="anna.kowalska@example.com",
        first_name="Anna",
        last_name="Kowalska",
        country_code="PL",
        city="Warszawa",
        load_date=load_date,
    )
    _write_customers_csv(csv_path, expected_raw_row)

    run_customers_bronze(
        spark=spark,
        input_path=input_path.parent,
        output_path=output_path,
    )

    result_df = spark.read.format("delta").load(str(output_path))

    assert result_df.count() == 1
    assert result_df.columns == CUSTOMERS_COLUMNS + STANDARD_BRONZE_METADATA_COLUMNS
    assert isinstance(result_df.schema["_source_load_date"].dataType, T.DateType)

    row = result_df.first().asDict()

    for column_name, expected_value in expected_raw_row.items():
        assert row[column_name] == expected_value

    assert row["_source_load_date"] == date(2026, 6, 1)
    assert row["_source_file_name"] == "customers.csv"
    assert row["_source_file_path"].endswith(f"load_date={load_date}/customers.csv")
    assert row["_source_system"] == "local_files"
    assert row["_source_entity"] == "customers"
    assert row["_ingestion_run_id"]
    assert row["_ingested_at"] is not None
    assert row["_raw_record_hash"] == _expected_raw_record_hash(expected_raw_row)


def test_run_customers_bronze_extracts_multiple_load_dates(
    spark,
    tmp_path: Path,
) -> None:
    base_input_path = tmp_path / "landing" / "customers"
    output_path = tmp_path / "bronze" / "customers"

    first_load_date = "2026-06-01"
    second_load_date = "2026-06-02"
    first_day = base_input_path / f"load_date={first_load_date}"
    second_day = base_input_path / f"load_date={second_load_date}"

    first_day.mkdir(parents=True)
    second_day.mkdir(parents=True)

    _write_customers_csv(
        first_day / "customers.csv",
        _customers_row(
            customer_id="cust_001",
            email="anna.kowalska@example.com",
            first_name="Anna",
            last_name="Kowalska",
            country_code="PL",
            city="Warszawa",
            load_date=first_load_date,
        ),
    )
    _write_customers_csv(
        second_day / "customers.csv",
        _customers_row(
            customer_id="cust_002",
            email="max.mustermann@example.com",
            first_name="Max",
            last_name="Mustermann",
            country_code="DE",
            city="Berlin",
            load_date=second_load_date,
            customer_status="inactive",
            marketing_consent="False",
        ),
    )

    run_customers_bronze(
        spark=spark,
        input_path=base_input_path,
        output_path=output_path,
    )

    result_df = spark.read.format("delta").load(str(output_path))

    load_dates = {
        row["_source_load_date"]
        for row in result_df.select("_source_load_date").distinct().collect()
    }

    assert load_dates == {
        date.fromisoformat(first_load_date),
        date.fromisoformat(second_load_date),
    }
    assert result_df.count() == 2


def test_run_customers_bronze_replay_replaces_only_target_load_date(
    spark,
    tmp_path: Path,
) -> None:
    first_load_date = "2026-06-01"
    second_load_date = "2026-06-02"
    base_input_path = tmp_path / "landing" / "customers"
    first_day = base_input_path / f"load_date={first_load_date}"
    second_day = base_input_path / f"load_date={second_load_date}"
    output_path = tmp_path / "bronze" / "customers"

    first_day.mkdir(parents=True)
    second_day.mkdir(parents=True)

    _write_customers_csv(
        first_day / "customers.csv",
        _customers_row(
            customer_id="cust_001",
            email="anna.kowalska@example.com",
            first_name="Anna",
            last_name="Kowalska",
            country_code="PL",
            city="Warszawa",
            load_date=first_load_date,
        ),
    )
    _write_customers_csv(
        second_day / "customers.csv",
        _customers_row(
            customer_id="cust_002",
            email="max.mustermann@example.com",
            first_name="Max",
            last_name="Mustermann",
            country_code="DE",
            city="Berlin",
            load_date=second_load_date,
        ),
    )

    run_customers_bronze(
        spark=spark,
        input_path=base_input_path,
        output_path=output_path,
    )

    _write_customers_csv(
        second_day / "customers.csv",
        _customers_row(
            customer_id="cust_002",
            email="max.mustermann@example.com",
            first_name="Max",
            last_name="Mustermann",
            country_code="DE",
            city="Hamburg",
            load_date=second_load_date,
        ),
    )

    run_customers_bronze(
        spark=spark,
        input_path=second_day,
        output_path=output_path,
    )

    result_df = spark.read.format("delta").load(str(output_path))

    assert {
        row["_source_load_date"]: (row["customer_id"], row["city"])
        for row in result_df.select("_source_load_date", "customer_id", "city").collect()
    } == {
        date.fromisoformat(first_load_date): ("cust_001", "Warszawa"),
        date.fromisoformat(second_load_date): ("cust_002", "Hamburg"),
    }
