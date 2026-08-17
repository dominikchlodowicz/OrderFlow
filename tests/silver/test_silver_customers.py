from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from orderflow.silver.customers import (
    transform_customers_silver,
    validate_customers_silver,
)

EXPECTED_SILVER_CUSTOMER_COLUMNS = [
    "customer_id",
    "email",
    "first_name",
    "last_name",
    "country_code",
    "city",
    "created_at",
    "updated_at",
    "customer_status",
    "marketing_consent",
    "load_date",
    "loaded_at",
    "source_event_at",
    "_source_file_name",
    "_source_file_path",
    "_ingestion_run_id",
    "_bronze_ingested_at",
    "_raw_record_hash",
    "_silver_processed_at",
]


def customer_row(
    *,
    customer_id: str = "cust_000000000001",
    email: str = "customer@example.com",
    first_name: str = "Jan",
    last_name: str = "Kowalski",
    country_code: str = "PL",
    city: str = "Warszawa",
    created_at: str = "2026-06-01 08:00:00",
    updated_at: str = "2026-06-01 08:00:00",
    customer_status: str = "active",
    marketing_consent: str = "True",
    load_date: str = "2026-06-01",
    loaded_at: str = "2026-06-01 09:00:00",
    source_event_at: str = "2026-06-01 08:00:00",
    source_file_name: str = "customers.csv",
    source_file_path: str = "/Volumes/orderflow_dev/bronze/landing/customers.csv",
    ingestion_run_id: str = "run-001",
    ingested_at: datetime = datetime(2026, 6, 1, 9, 5, 0),
    raw_record_hash: str = "customers-hash",
) -> dict[str, object]:
    return {
        "customer_id": customer_id,
        "email": email,
        "first_name": first_name,
        "last_name": last_name,
        "country_code": country_code,
        "city": city,
        "created_at": created_at,
        "updated_at": updated_at,
        "customer_status": customer_status,
        "marketing_consent": marketing_consent,
        "load_date": load_date,
        "loaded_at": loaded_at,
        "source_event_at": source_event_at,
        "_source_file_name": source_file_name,
        "_source_file_path": source_file_path,
        "_source_load_date": date.fromisoformat(load_date),
        "_ingestion_run_id": ingestion_run_id,
        "_ingested_at": ingested_at,
        "_raw_record_hash": raw_record_hash,
    }


@pytest.mark.parametrize(
    "field_name",
    [
        "customer_id",
        "email",
        "first_name",
        "last_name",
    ],
)
def test_transform_customers_silver_rejects_blank_required_fields(
    spark: SparkSession,
    field_name: str,
) -> None:
    row = customer_row()
    row[field_name] = "   "
    bronze_df = spark.createDataFrame([row])

    with pytest.raises(ValueError, match="null required fields"):
        transform_customers_silver(bronze_df)


def test_transform_customers_silver_rejects_duplicate_normalized_emails(
    spark: SparkSession,
) -> None:
    bronze_df = spark.createDataFrame(
        [
            customer_row(),
            customer_row(
                customer_id="cust_000000000002",
                email=" CUSTOMER@EXAMPLE.COM ",
            ),
        ]
    )

    with pytest.raises(ValueError, match="duplicate email values"):
        transform_customers_silver(bronze_df)


def test_validate_customers_silver_rejects_duplicate_customer_ids(
    spark: SparkSession,
) -> None:
    bronze_df = spark.createDataFrame(
        [
            customer_row(),
            customer_row(
                customer_id="cust_000000000002",
                email="another@example.com",
            ),
        ]
    )
    silver_df = transform_customers_silver(bronze_df).withColumn(
        "customer_id",
        F.lit("cust_000000000001"),
    )

    with pytest.raises(ValueError, match="duplicate customer_id values"):
        validate_customers_silver(silver_df)


def test_transform_customers_silver_keeps_latest_customer_version(
    spark: SparkSession,
) -> None:
    bronze_df = spark.createDataFrame(
        [
            customer_row(
                city="Kraków",
                updated_at="2026-06-01 08:00:00",
                loaded_at="2026-06-03 10:00:00",
                source_event_at="2026-06-01 08:00:00",
            ),
            customer_row(
                city="Gdańsk",
                updated_at="2026-06-02 08:00:00",
                load_date="2026-06-02",
                loaded_at="2026-06-02 09:00:00",
                source_event_at="2026-06-02 08:00:00",
            ),
        ]
    )

    result_df = transform_customers_silver(bronze_df)
    result = result_df.first()

    assert result_df.columns == EXPECTED_SILVER_CUSTOMER_COLUMNS
    assert result["customer_id"] == "cust_000000000001"
    assert result["created_at"] == datetime(2026, 6, 1, 8, 0, 0)
    assert result["city"] == "Gdańsk"
    assert result["country_code"] == "PL"
