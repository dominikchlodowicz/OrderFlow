from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from orderflow.quality.orderflow import build_orderflow_quality_suite

ORDER_SCHEMA = T.StructType(
    [
        T.StructField("order_key", T.LongType(), nullable=False),
        T.StructField("order_id", T.StringType(), nullable=False),
        T.StructField("customer_key", T.LongType(), nullable=False),
        T.StructField("campaign_key", T.LongType(), nullable=False),
        T.StructField("currency_key", T.LongType(), nullable=False),
        T.StructField("order_date_key", T.IntegerType(), nullable=False),
        T.StructField("order_created_at", T.TimestampType(), nullable=False),
        T.StructField("order_updated_at", T.TimestampType(), nullable=True),
        T.StructField("order_status", T.StringType(), nullable=False),
        T.StructField("order_country_code", T.StringType(), nullable=False),
        T.StructField("source_channel", T.StringType(), nullable=False),
        T.StructField("gross_amount", T.DecimalType(18, 2), nullable=False),
        T.StructField("discount_amount", T.DecimalType(18, 2), nullable=False),
        T.StructField("net_amount", T.DecimalType(18, 2), nullable=False),
        T.StructField("_gold_processed_at", T.TimestampType(), nullable=False),
    ]
)


def order_row(status: str = "paid") -> tuple[object, ...]:
    return (
        1001,
        "ord_001",
        101,
        201,
        301,
        20260601,
        datetime(2026, 6, 1, 10, 0),
        datetime(2026, 6, 1, 10, 5),
        status,
        "PL",
        "direct",
        Decimal("100.00"),
        Decimal("10.00"),
        Decimal("90.00"),
        datetime(2026, 6, 1, 11, 0),
    )


def order_tables(spark: SparkSession, status: str = "paid"):
    return {
        "fct_orders": spark.createDataFrame([order_row(status)], ORDER_SCHEMA),
        "dim_customers": spark.createDataFrame([(101,)], ["customer_key"]),
        "dim_campaigns": spark.createDataFrame([(201,)], ["campaign_key"]),
        "dim_currency": spark.createDataFrame([(301,)], ["currency_key"]),
        "dim_calendar": spark.createDataFrame([(20260601,)], ["date_key"]),
    }


def test_orderflow_order_contract_passes_valid_order(spark: SparkSession) -> None:
    suite = build_orderflow_quality_suite(
        order_tables(spark),
        table_names=["fct_orders"],
    )

    report = suite.run()

    assert report.passed, report.format_summary()


def test_orderflow_order_contract_rejects_status_outside_executable_contract(
    spark: SparkSession,
) -> None:
    suite = build_orderflow_quality_suite(
        order_tables(spark, status="returned"),
        table_names=["fct_orders"],
    )

    report = suite.run()
    status_result = next(
        result for result in report.results if result.check_name == "accepted_order_status"
    )

    assert not status_result.passed
    assert status_result.violation_count == 1
