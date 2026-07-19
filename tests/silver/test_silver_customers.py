from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from orderflow.silver.customers import (
    transform_customers_silver,
    validate_customers_silver,
)


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
) -> dict[str, str]:
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
    silver_df = spark.createDataFrame(
        [
            customer_row(),
            customer_row(
                email="another@example.com",
            ),
        ]
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

    result = transform_customers_silver(bronze_df).first()

    assert result["customer_id"] == "cust_000000000001"
    assert result["created_at"] == datetime(2026, 6, 1, 8, 0, 0)
    assert result["city"] == "Gdańsk"
