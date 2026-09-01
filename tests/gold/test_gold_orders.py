from datetime import datetime
from decimal import Decimal

import pytest
from helpers.gold import (
    calendar_dimension_df,
    campaign_dimension_df,
    currency_dimension_df,
    customer_dimension_df,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from orderflow.gold.common import GUEST_CUSTOMER_KEY, NO_CAMPAIGN_KEY, UNKNOWN_KEY
from orderflow.gold.fct_orders import transform_fct_orders

SILVER_ORDERS_SCHEMA = T.StructType(
    [
        T.StructField("order_id", T.StringType(), nullable=True),
        T.StructField("customer_id", T.StringType(), nullable=True),
        T.StructField("order_status", T.StringType(), nullable=True),
        T.StructField("order_created_at", T.TimestampType(), nullable=True),
        T.StructField("order_updated_at", T.TimestampType(), nullable=True),
        T.StructField("country_code", T.StringType(), nullable=True),
        T.StructField("currency", T.StringType(), nullable=True),
        T.StructField("gross_amount", T.DecimalType(18, 2), nullable=True),
        T.StructField("discount_amount", T.DecimalType(18, 2), nullable=True),
        T.StructField("net_amount", T.DecimalType(18, 2), nullable=True),
        T.StructField("source_channel", T.StringType(), nullable=True),
        T.StructField("campaign_id", T.StringType(), nullable=True),
        T.StructField("source_event_at", T.TimestampType(), nullable=True),
        T.StructField("_source_file_name", T.StringType(), nullable=False),
        T.StructField("_source_file_path", T.StringType(), nullable=False),
        T.StructField("_ingestion_run_id", T.StringType(), nullable=False),
        T.StructField("_bronze_ingested_at", T.TimestampType(), nullable=False),
        T.StructField("_raw_record_hash", T.StringType(), nullable=False),
        T.StructField("_silver_processed_at", T.TimestampType(), nullable=False),
    ]
)

EXPECTED_GOLD_SCHEMA = [
    ("order_key", "bigint"),
    ("order_id", "string"),
    ("customer_key", "bigint"),
    ("campaign_key", "bigint"),
    ("currency_key", "bigint"),
    ("order_date_key", "int"),
    ("order_created_at", "timestamp"),
    ("order_updated_at", "timestamp"),
    ("order_status", "string"),
    ("order_country_code", "string"),
    ("source_channel", "string"),
    ("gross_amount", "decimal(18,2)"),
    ("discount_amount", "decimal(18,2)"),
    ("net_amount", "decimal(18,2)"),
    ("_gold_processed_at", "timestamp"),
]

SILVER_LINEAGE_COLUMNS = {
    "source_event_at",
    "_source_file_name",
    "_source_file_path",
    "_ingestion_run_id",
    "_bronze_ingested_at",
    "_raw_record_hash",
    "_silver_processed_at",
}


def silver_order_row(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "order_id": "ord_001",
        "customer_id": "cust_001",
        "order_status": "paid",
        "order_created_at": datetime(2026, 6, 10, 9, 30),
        "order_updated_at": datetime(2026, 6, 10, 10, 0),
        "country_code": "PL",
        "currency": "PLN",
        "gross_amount": Decimal("100.00"),
        "discount_amount": Decimal("10.00"),
        "net_amount": Decimal("90.00"),
        "source_channel": "web",
        "campaign_id": "cmp_001",
        "source_event_at": datetime(2026, 6, 10, 10, 0),
        "_source_file_name": "orders.csv",
        "_source_file_path": "/bronze/orders.csv",
        "_ingestion_run_id": "run-001",
        "_bronze_ingested_at": datetime(2026, 6, 10, 10, 5),
        "_raw_record_hash": "order-hash",
        "_silver_processed_at": datetime(2026, 6, 10, 10, 10),
    }
    values.update(overrides)
    return values


def create_silver_orders_df(
    spark: SparkSession,
    rows: list[dict[str, object]],
) -> DataFrame:
    return spark.createDataFrame(rows, schema=SILVER_ORDERS_SCHEMA)


def transform_orders(
    spark: SparkSession,
    rows: list[dict[str, object]],
    *,
    currency_df: DataFrame | None = None,
    calendar_df: DataFrame | None = None,
) -> DataFrame:
    return transform_fct_orders(
        silver_orders_df=create_silver_orders_df(spark, rows),
        dim_customers_df=customer_dimension_df(spark),
        dim_campaigns_df=campaign_dimension_df(spark),
        dim_currency_df=currency_df or currency_dimension_df(spark),
        dim_calendar_df=calendar_df or calendar_dimension_df(spark),
    )


def test_transform_fct_orders_matches_contract_and_resolves_dimensions(
    spark: SparkSession,
) -> None:
    result_df = transform_orders(spark, [silver_order_row()])
    result = result_df.first()

    assert result_df.dtypes == EXPECTED_GOLD_SCHEMA
    assert result is not None
    assert result["order_key"] >= 0
    assert result["customer_key"] == 101
    assert result["campaign_key"] == 201
    assert result["currency_key"] == 401
    assert result["order_date_key"] == 20260610
    assert result["order_country_code"] == "PL"
    assert result["net_amount"] == Decimal("90.00")
    assert result["_gold_processed_at"] is not None
    assert SILVER_LINEAGE_COLUMNS.isdisjoint(result_df.columns)


def test_transform_fct_orders_distinguishes_absent_and_unresolved_members(
    spark: SparkSession,
) -> None:
    result_df = transform_orders(
        spark,
        [
            silver_order_row(
                order_id="ord_guest",
                customer_id=None,
                campaign_id=None,
            ),
            silver_order_row(
                order_id="ord_unknown",
                customer_id="cust_missing",
                campaign_id="cmp_missing",
            ),
        ],
    )
    rows = {row["order_id"]: row for row in result_df.collect()}

    assert rows["ord_guest"]["customer_key"] == GUEST_CUSTOMER_KEY
    assert rows["ord_guest"]["campaign_key"] == NO_CAMPAIGN_KEY
    assert rows["ord_unknown"]["customer_key"] == UNKNOWN_KEY
    assert rows["ord_unknown"]["campaign_key"] == UNKNOWN_KEY


@pytest.mark.parametrize(
    ("missing_reference", "expected_message"),
    [
        ("currency", "unresolved currency references"),
        ("calendar", "unresolved order calendar dates"),
    ],
)
def test_transform_fct_orders_rejects_missing_required_relationships(
    spark: SparkSession,
    missing_reference: str,
    expected_message: str,
) -> None:
    currency_df = currency_dimension_df(spark)
    calendar_df = calendar_dimension_df(spark)
    if missing_reference == "currency":
        currency_df = currency_df.filter(F.col("currency_code") != "PLN")
    else:
        calendar_df = calendar_df.filter(F.col("date_day") != F.lit("2026-06-10"))

    with pytest.raises(ValueError, match=expected_message):
        transform_orders(
            spark,
            [silver_order_row()],
            currency_df=currency_df,
            calendar_df=calendar_df,
        )


def test_transform_fct_orders_rejects_missing_required_order_column(
    spark: SparkSession,
) -> None:
    silver_orders_df = create_silver_orders_df(spark, [silver_order_row()]).drop("order_status")

    with pytest.raises(ValueError, match="Silver orders.*missing required columns"):
        transform_fct_orders(
            silver_orders_df=silver_orders_df,
            dim_customers_df=customer_dimension_df(spark),
            dim_campaigns_df=campaign_dimension_df(spark),
            dim_currency_df=currency_dimension_df(spark),
            dim_calendar_df=calendar_dimension_df(spark),
        )


def test_transform_fct_orders_rejects_duplicate_grain(spark: SparkSession) -> None:
    with pytest.raises(ValueError, match="duplicate order_id values"):
        transform_orders(spark, [silver_order_row(), silver_order_row()])


def test_transform_fct_orders_rejects_invalid_measures(spark: SparkSession) -> None:
    with pytest.raises(ValueError, match="inconsistent order amounts"):
        transform_orders(
            spark,
            [silver_order_row(net_amount=Decimal("91.00"))],
        )
