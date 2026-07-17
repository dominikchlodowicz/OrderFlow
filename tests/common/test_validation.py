import pytest
from pyspark.sql import SparkSession

from orderflow.common.validation import validate_required_columns


def test_validate_required_columns_accepts_dataframe_with_all_columns(
    spark: SparkSession,
) -> None:
    df = spark.createDataFrame(
        [(1, "calendar")],
        ["id", "name"],
    )

    validate_required_columns(
        df,
        ["id", "name"],
        dataset_name="Test dataset",
    )


def test_validate_required_columns_accepts_extra_columns(
    spark: SparkSession,
) -> None:
    df = spark.createDataFrame(
        [(1, "calendar", "extra value")],
        ["id", "name", "extra_column"],
    )

    validate_required_columns(
        df,
        ["id", "name"],
        dataset_name="Test dataset",
    )


def test_validate_required_columns_accepts_empty_required_column_list(
    spark: SparkSession,
) -> None:
    df = spark.createDataFrame(
        [(1,)],
        ["id"],
    )

    validate_required_columns(
        df,
        [],
        dataset_name="Test dataset",
    )


def test_validate_required_columns_accepts_tuple_of_column_names(
    spark: SparkSession,
) -> None:
    df = spark.createDataFrame(
        [(1, "calendar")],
        ["id", "name"],
    )

    validate_required_columns(
        df,
        ("id", "name"),
        dataset_name="Test dataset",
    )


def test_validate_required_columns_rejects_missing_column(
    spark: SparkSession,
) -> None:
    df = spark.createDataFrame(
        [(1,)],
        ["id"],
    )

    with pytest.raises(
        ValueError,
        match="Test dataset is missing required columns",
    ):
        validate_required_columns(
            df,
            ["id", "name"],
            dataset_name="Test dataset",
        )


def test_validate_required_columns_reports_all_missing_columns(
    spark: SparkSession,
) -> None:
    df = spark.createDataFrame(
        [(1,)],
        ["id"],
    )

    with pytest.raises(ValueError) as exception_info:
        validate_required_columns(
            df,
            ["id", "name", "created_at"],
            dataset_name="Silver calendar",
        )

    error_message = str(exception_info.value)

    assert "Silver calendar" in error_message
    assert "name" in error_message
    assert "created_at" in error_message


def test_validate_required_columns_is_case_sensitive(
    spark: SparkSession,
) -> None:
    df = spark.createDataFrame(
        [(1,)],
        ["date_day"],
    )

    with pytest.raises(ValueError, match="Date_Day"):
        validate_required_columns(
            df,
            ["Date_Day"],
            dataset_name="Calendar",
        )