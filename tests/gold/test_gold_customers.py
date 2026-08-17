from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from orderflow.gold.dim_customers import (
    run_dim_customers,
    transform_dim_customers,
)

SILVER_CUSTOMERS_SCHEMA = T.StructType(
    [
        T.StructField("customer_id", T.StringType(), nullable=True),
        T.StructField("email", T.StringType(), nullable=True),
        T.StructField("first_name", T.StringType(), nullable=True),
        T.StructField("last_name", T.StringType(), nullable=True),
        T.StructField("country_code", T.StringType(), nullable=True),
        T.StructField("city", T.StringType(), nullable=True),
        T.StructField("created_at", T.TimestampType(), nullable=True),
        T.StructField("updated_at", T.TimestampType(), nullable=True),
        T.StructField("customer_status", T.StringType(), nullable=True),
        T.StructField("marketing_consent", T.BooleanType(), nullable=True),
        T.StructField("load_date", T.DateType(), nullable=True),
        T.StructField("loaded_at", T.TimestampType(), nullable=True),
        T.StructField("source_event_at", T.TimestampType(), nullable=True),
        T.StructField("_source_file_name", T.StringType(), nullable=False),
        T.StructField("_source_file_path", T.StringType(), nullable=False),
        T.StructField("_ingestion_run_id", T.StringType(), nullable=False),
        T.StructField("_bronze_ingested_at", T.TimestampType(), nullable=False),
        T.StructField("_raw_record_hash", T.StringType(), nullable=False),
        T.StructField("_silver_processed_at", T.TimestampType(), nullable=True),
    ]
)

EXPECTED_GOLD_COLUMNS = [
    "customer_key",
    "customer_id",
    "email",
    "email_domain",
    "first_name",
    "last_name",
    "full_name",
    "country_code",
    "city",
    "customer_status",
    "is_active_customer",
    "marketing_consent",
    "registered_at",
    "registration_date",
    "_gold_processed_at",
]


def silver_customer_row(
    *,
    customer_id: str | None = "cust_000000000001",
    email: str = "jan.kowalski@example.com",
    first_name: str = "Jan",
    last_name: str = "Kowalski",
    country_code: str = "PL",
    city: str = "Warszawa",
    created_at: datetime | None = datetime(2026, 6, 1, 8, 0, 0),
    customer_status: str = "active",
    marketing_consent: bool | None = True,
) -> dict[str, Any]:
    return {
        "customer_id": customer_id,
        "email": email,
        "first_name": first_name,
        "last_name": last_name,
        "country_code": country_code,
        "city": city,
        "created_at": created_at,
        "updated_at": datetime(2026, 6, 2, 8, 0, 0),
        "customer_status": customer_status,
        "marketing_consent": marketing_consent,
        "load_date": date(2026, 6, 2),
        "loaded_at": datetime(2026, 6, 2, 9, 0, 0),
        "source_event_at": datetime(2026, 6, 2, 8, 0, 0),
        "_source_file_name": "customers.csv",
        "_source_file_path": "/Volumes/orderflow_dev/bronze/landing/customers.csv",
        "_ingestion_run_id": "run-001",
        "_bronze_ingested_at": datetime(2026, 6, 2, 9, 0, 0),
        "_raw_record_hash": "customers-hash",
        "_silver_processed_at": datetime(2026, 6, 2, 9, 5, 0),
    }


def create_silver_customers_df(
    spark: SparkSession,
    rows: list[dict[str, Any]],
) -> DataFrame:
    return spark.createDataFrame(rows, schema=SILVER_CUSTOMERS_SCHEMA)


def test_transform_dim_customers_creates_analytical_columns(
    spark: SparkSession,
) -> None:
    silver_df = create_silver_customers_df(
        spark,
        [silver_customer_row()],
    )

    result_df = transform_dim_customers(silver_df)
    result = result_df.first()

    assert result is not None
    assert result_df.columns == EXPECTED_GOLD_COLUMNS
    assert isinstance(result["customer_key"], int)
    assert result["email_domain"] == "example.com"
    assert result["full_name"] == "Jan Kowalski"
    assert result["is_active_customer"] is True
    assert result["registered_at"] == datetime(2026, 6, 1, 8, 0, 0)
    assert result["registration_date"] == date(2026, 6, 1)
    assert result["_gold_processed_at"] is not None


def test_transform_dim_customers_excludes_silver_metadata(
    spark: SparkSession,
) -> None:
    silver_df = create_silver_customers_df(
        spark,
        [silver_customer_row()],
    )

    result_df = transform_dim_customers(silver_df)

    excluded_columns = {
        "updated_at",
        "load_date",
        "loaded_at",
        "source_event_at",
        "_source_file_name",
        "_source_file_path",
        "_ingestion_run_id",
        "_bronze_ingested_at",
        "_raw_record_hash",
        "_silver_processed_at",
    }
    assert excluded_columns.isdisjoint(result_df.columns)


def test_transform_dim_customers_generates_non_negative_keys(
    spark: SparkSession,
) -> None:
    silver_df = create_silver_customers_df(
        spark,
        [
            silver_customer_row(
                customer_id=f"cust_{customer_number:012d}",
                email=f"customer{customer_number}@example.com",
            )
            for customer_number in range(1, 11)
        ],
    )

    customer_keys = [
        row["customer_key"]
        for row in transform_dim_customers(silver_df).select("customer_key").collect()
    ]

    assert all(customer_key >= 0 for customer_key in customer_keys)


def test_transform_dim_customers_handles_inactive_customer_and_invalid_email(
    spark: SparkSession,
) -> None:
    silver_df = create_silver_customers_df(
        spark,
        [
            silver_customer_row(
                email="invalid-email",
                customer_status="inactive",
            )
        ],
    )

    result = transform_dim_customers(silver_df).first()

    assert result is not None
    assert result["email_domain"] is None
    assert result["is_active_customer"] is False


def test_transform_dim_customers_rejects_missing_required_columns(
    spark: SparkSession,
) -> None:
    silver_df = create_silver_customers_df(
        spark,
        [silver_customer_row()],
    ).drop("customer_status")

    with pytest.raises(ValueError) as exception_info:
        transform_dim_customers(silver_df)

    error_message = str(exception_info.value)
    assert "Silver customers" in error_message
    assert "missing required columns" in error_message
    assert "customer_status" in error_message


def test_transform_dim_customers_rejects_null_customer_id(
    spark: SparkSession,
) -> None:
    silver_df = create_silver_customers_df(
        spark,
        [silver_customer_row(customer_id=None)],
    )

    with pytest.raises(ValueError, match="rows have null keys"):
        transform_dim_customers(silver_df)


def test_transform_dim_customers_rejects_null_required_attribute(
    spark: SparkSession,
) -> None:
    silver_df = create_silver_customers_df(
        spark,
        [silver_customer_row(marketing_consent=None)],
    )

    with pytest.raises(ValueError, match="rows have null required attributes"):
        transform_dim_customers(silver_df)


def test_transform_dim_customers_rejects_duplicate_customer_ids(
    spark: SparkSession,
) -> None:
    silver_df = create_silver_customers_df(
        spark,
        [
            silver_customer_row(),
            silver_customer_row(email="another@example.com"),
        ],
    )

    with pytest.raises(ValueError, match="duplicate customer_id values"):
        transform_dim_customers(silver_df)


def test_run_dim_customers_writes_delta_table(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "silver_customers"
    output_path = tmp_path / "gold_dim_customers"
    silver_df = create_silver_customers_df(
        spark,
        [
            silver_customer_row(),
            silver_customer_row(
                customer_id="cust_000000000002",
                email="anna.nowak@shop.example",
                first_name="Anna",
                last_name="Nowak",
            ),
        ],
    )
    silver_df.write.format("delta").mode("overwrite").save(str(input_path))

    run_dim_customers(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
    )

    result_df = spark.read.format("delta").load(str(output_path))

    assert result_df.count() == 2
    assert result_df.columns == EXPECTED_GOLD_COLUMNS
    assert {row["email_domain"] for row in result_df.collect()} == {"example.com", "shop.example"}
