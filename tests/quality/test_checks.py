from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from orderflow.quality.checks import DataQualityFailure, DataQualitySuite

TEST_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType(), nullable=True),
        T.StructField("status", T.StringType(), nullable=True),
        T.StructField("amount", T.DecimalType(18, 2), nullable=True),
        T.StructField("started_on", T.DateType(), nullable=True),
        T.StructField("finished_on", T.DateType(), nullable=True),
        T.StructField("parent_id", T.LongType(), nullable=True),
    ]
)


def row(
    *,
    row_id: int | None = 1,
    status: str | None = "valid",
    amount: str | None = "10.00",
    started_on: date | None = date(2026, 6, 1),
    finished_on: date | None = date(2026, 6, 2),
    parent_id: int | None = 10,
) -> tuple[object, ...]:
    return (
        row_id,
        status,
        Decimal(amount) if amount is not None else None,
        started_on,
        finished_on,
        parent_id,
    )


def build_suite(spark: SparkSession, rows: list[tuple[object, ...]]) -> DataQualitySuite:
    data = spark.createDataFrame(rows, TEST_SCHEMA)
    parents = spark.createDataFrame([(10,)], ["id"])
    suite = DataQualitySuite({"test_data": data, "parents": parents})
    suite.check_required_columns("test_data", TEST_SCHEMA.fieldNames())
    suite.check_non_empty("test_data")
    suite.check_non_null("test_data", ["id"])
    suite.check_unique("test_data", ["id"])
    suite.check_accepted_values("test_data", "status", ["valid"])
    suite.check_numeric_range("test_data", "amount", minimum=0)
    suite.check_date_order("test_data", "started_on", "finished_on")
    suite.check_reference("test_data", ["parent_id"], "parents", ["id"])
    return suite


def result_for(report, check_name: str):
    return next(result for result in report.results if result.check_name == check_name)


def test_quality_suite_passes_valid_dataset(spark: SparkSession) -> None:
    report = build_suite(spark, [row()]).run()

    assert report.passed
    assert all(result.passed for result in report.results)


def test_required_columns_reports_all_missing_columns(spark: SparkSession) -> None:
    data = spark.createDataFrame([(1,)], ["id"])
    suite = DataQualitySuite({"test_data": data})
    suite.check_required_columns("test_data", ["id", "status", "amount"])

    result = result_for(suite.run(), "required_columns")

    assert not result.passed
    assert result.violation_count == 2
    assert "status" in result.details
    assert "amount" in result.details


def test_non_null_check_counts_null_rows(spark: SparkSession) -> None:
    report = build_suite(spark, [row(row_id=None)]).run()

    result = result_for(report, "required_values_non_null")

    assert not result.passed
    assert result.violation_count == 1


def test_unique_check_counts_duplicate_surplus_rows(spark: SparkSession) -> None:
    report = build_suite(spark, [row(), row()]).run()

    result = result_for(report, "unique_id")

    assert not result.passed
    assert result.violation_count == 1


def test_accepted_values_check_counts_invalid_rows(spark: SparkSession) -> None:
    report = build_suite(spark, [row(status="invalid")]).run()

    result = result_for(report, "accepted_status")

    assert not result.passed
    assert result.violation_count == 1


def test_numeric_range_check_counts_out_of_range_rows(spark: SparkSession) -> None:
    report = build_suite(spark, [row(amount="-0.01")]).run()

    result = result_for(report, "range_amount")

    assert not result.passed
    assert result.violation_count == 1


def test_date_order_check_counts_invalid_rows(spark: SparkSession) -> None:
    report = build_suite(
        spark,
        [row(started_on=date(2026, 6, 2), finished_on=date(2026, 6, 1))],
    ).run()

    result = result_for(report, "finished_on_not_before_started_on")

    assert not result.passed
    assert result.violation_count == 1


def test_reference_check_counts_unresolved_rows(spark: SparkSession) -> None:
    report = build_suite(spark, [row(parent_id=999)]).run()

    result = result_for(report, "reference_parent_id_to_parents")

    assert not result.passed
    assert result.violation_count == 1


def test_report_contains_multiple_simultaneous_failures(spark: SparkSession) -> None:
    report = build_suite(
        spark,
        [
            row(row_id=None, status="invalid", amount="-1.00", parent_id=999),
            row(row_id=None, status="invalid", amount="-2.00", parent_id=998),
        ],
    ).run()

    failed_names = {result.check_name for result in report.critical_failures}

    assert {
        "required_values_non_null",
        "unique_id",
        "accepted_status",
        "range_amount",
        "reference_parent_id_to_parents",
    } <= failed_names


def test_report_raises_dedicated_exception_after_checks_complete(
    spark: SparkSession,
) -> None:
    report = build_suite(spark, [row(status="invalid", amount="-1.00")]).run()

    with pytest.raises(DataQualityFailure) as exception_info:
        report.raise_for_failures()

    assert exception_info.value.report is report
    assert len(exception_info.value.report.critical_failures) == 2


def test_schema_check_reports_type_mismatch(spark: SparkSession) -> None:
    data = spark.createDataFrame([("1",)], ["id"])
    suite = DataQualitySuite({"test_data": data})
    suite.check_schema("test_data", {"id": "bigint"})

    result = result_for(suite.run(), "schema_types")

    assert not result.passed
    assert result.violation_count == 1
    assert "expected bigint, found string" in result.details
