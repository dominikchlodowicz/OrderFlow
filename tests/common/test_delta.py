from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from orderflow.common.delta import (
    align_dataframe_to_table_contract,
    read_delta,
    read_delta_table,
    write_delta,
    write_delta_table,
)
from orderflow.config.constants import (
    CALENDAR_BRONZE_TABLE,
    CALENDAR_GOLD_TABLE,
    CALENDAR_SILVER_TABLE,
    CUSTOMERS_BRONZE_TABLE,
    CUSTOMERS_GOLD_TABLE,
    CUSTOMERS_SILVER_TABLE,
)


def collect_rows(
    df: DataFrame,
    *,
    order_by: str,
) -> list[dict[str, Any]]:
    return [row.asDict(recursive=True) for row in df.orderBy(order_by).collect()]


def test_write_and_read_delta_round_trip(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "round_trip_table"

    source_df = spark.createDataFrame(
        [
            (1, "calendar"),
            (2, "customers"),
        ],
        ["id", "entity_name"],
    )

    write_delta(
        df=source_df,
        path=output_path,
    )

    result_df = read_delta(
        spark=spark,
        path=output_path,
    )

    assert result_df.columns == [
        "id",
        "entity_name",
    ]

    assert collect_rows(result_df, order_by="id") == [
        {
            "id": 1,
            "entity_name": "calendar",
        },
        {
            "id": 2,
            "entity_name": "customers",
        },
    ]


def test_write_delta_accepts_string_path(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "string_path_table"

    source_df = spark.createDataFrame(
        [(1, "calendar")],
        ["id", "entity_name"],
    )

    write_delta(
        df=source_df,
        path=str(output_path),
    )

    result_df = read_delta(
        spark=spark,
        path=str(output_path),
    )

    assert result_df.count() == 1
    assert result_df.first()["entity_name"] == "calendar"


def test_write_delta_overwrites_existing_data_by_default(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "overwrite_table"

    original_df = spark.createDataFrame(
        [
            (1, "calendar"),
            (2, "customers"),
        ],
        ["id", "entity_name"],
    )

    replacement_df = spark.createDataFrame(
        [
            (3, "products"),
        ],
        ["id", "entity_name"],
    )

    write_delta(
        df=original_df,
        path=output_path,
    )

    write_delta(
        df=replacement_df,
        path=output_path,
    )

    result_df = read_delta(
        spark=spark,
        path=output_path,
    )

    assert collect_rows(result_df, order_by="id") == [
        {
            "id": 3,
            "entity_name": "products",
        },
    ]


def test_write_delta_can_append_rows(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "append_table"

    first_df = spark.createDataFrame(
        [(1, "calendar")],
        ["id", "entity_name"],
    )

    second_df = spark.createDataFrame(
        [(2, "customers")],
        ["id", "entity_name"],
    )

    write_delta(
        df=first_df,
        path=output_path,
    )

    write_delta(
        df=second_df,
        path=output_path,
        mode="append",
        overwrite_schema=False,
    )

    result_df = read_delta(
        spark=spark,
        path=output_path,
    )

    assert collect_rows(result_df, order_by="id") == [
        {
            "id": 1,
            "entity_name": "calendar",
        },
        {
            "id": 2,
            "entity_name": "customers",
        },
    ]


def test_write_delta_overwrites_schema_when_enabled(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "schema_overwrite_table"

    original_df = spark.createDataFrame(
        [(1, "calendar")],
        ["id", "entity_name"],
    )

    changed_df = spark.createDataFrame(
        [(2, "customers", True)],
        ["id", "entity_name", "is_active"],
    )

    write_delta(
        df=original_df,
        path=output_path,
    )

    write_delta(
        df=changed_df,
        path=output_path,
        mode="overwrite",
        overwrite_schema=True,
    )

    result_df = read_delta(
        spark=spark,
        path=output_path,
    )

    assert result_df.columns == [
        "id",
        "entity_name",
        "is_active",
    ]

    result = result_df.first()

    assert result is not None
    assert result["id"] == 2
    assert result["entity_name"] == "customers"
    assert result["is_active"] is True


def test_write_delta_partitions_table_by_selected_column(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "partitioned_table"

    source_df = spark.createDataFrame(
        [
            (1, "calendar", "reference"),
            (2, "customers", "master"),
            (3, "products", "master"),
        ],
        ["id", "entity_name", "entity_type"],
    )

    write_delta(
        df=source_df,
        path=output_path,
        partition_by=["entity_type"],
    )

    result_df = read_delta(
        spark=spark,
        path=output_path,
    )

    assert result_df.count() == 3

    entity_types = {
        row["entity_type"] for row in result_df.select("entity_type").distinct().collect()
    }

    assert entity_types == {
        "reference",
        "master",
    }

    partition_directories = {
        path.name for path in output_path.glob("entity_type=*") if path.is_dir()
    }

    assert partition_directories == {
        "entity_type=master",
        "entity_type=reference",
    }


def test_write_delta_supports_multiple_partition_columns(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "multi_partition_table"

    source_df = spark.createDataFrame(
        [
            (1, 2026, 6, "calendar"),
            (2, 2026, 7, "customers"),
        ],
        ["id", "year", "month", "entity_name"],
    )

    write_delta(
        df=source_df,
        path=output_path,
        partition_by=["year", "month"],
    )

    result_df = read_delta(
        spark=spark,
        path=output_path,
    )

    assert collect_rows(result_df, order_by="id") == [
        {
            "id": 1,
            "entity_name": "calendar",
            "year": 2026,
            "month": 6,
        },
        {
            "id": 2,
            "entity_name": "customers",
            "year": 2026,
            "month": 7,
        },
    ]

    assert (output_path / "year=2026" / "month=6").is_dir()
    assert (output_path / "year=2026" / "month=7").is_dir()


def test_write_delta_creates_delta_transaction_log(
    spark: SparkSession,
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "delta_log_table"

    source_df = spark.createDataFrame(
        [(1, "calendar")],
        ["id", "entity_name"],
    )

    write_delta(
        df=source_df,
        path=output_path,
    )

    delta_log_path = output_path / "_delta_log"

    assert delta_log_path.is_dir()
    assert any(delta_log_path.iterdir())


def test_read_delta_table_uses_registered_table_name() -> None:
    spark = Mock(spec=SparkSession)
    expected_df = Mock(spec=DataFrame)
    spark.table.return_value = expected_df

    result_df = read_delta_table(
        spark=spark,
        table_name=CALENDAR_SILVER_TABLE,
    )

    assert result_df is expected_df
    spark.table.assert_called_once_with(CALENDAR_SILVER_TABLE)


def test_write_delta_table_aligns_to_registered_contract_and_preserves_table() -> None:
    source_df = Mock(spec=DataFrame)
    aligned_df = Mock(spec=DataFrame)
    writer = aligned_df.write
    writer.mode.return_value = writer
    spark = Mock(spec=SparkSession)
    target_df = Mock(spec=DataFrame)
    target_schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("entity_name", T.StringType()),
        ]
    )
    source_df.sparkSession = spark
    source_df.columns = ["entity_name", "id"]
    source_df.schema = T.StructType(
        [
            T.StructField("entity_name", T.StringType()),
            T.StructField("id", T.IntegerType()),
        ]
    )
    source_df.select.return_value = aligned_df
    target_df.schema = target_schema
    spark.table.return_value = target_df

    write_delta_table(
        df=source_df,
        table_name=CALENDAR_BRONZE_TABLE,
    )

    spark.table.assert_called_once_with(CALENDAR_BRONZE_TABLE)
    source_df.select.assert_called_once_with("id", "entity_name")
    writer.mode.assert_called_once_with("overwrite")
    writer.insertInto.assert_called_once_with(CALENDAR_BRONZE_TABLE)


def test_align_dataframe_to_table_contract_rejects_column_drift() -> None:
    source_df = Mock(spec=DataFrame)
    spark = Mock(spec=SparkSession)
    target_df = Mock(spec=DataFrame)
    source_df.sparkSession = spark
    source_df.columns = ["id", "unexpected"]
    source_df.schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("unexpected", T.StringType()),
        ]
    )
    target_df.schema = T.StructType(
        [
            T.StructField("id", T.IntegerType()),
            T.StructField("entity_name", T.StringType()),
        ]
    )
    spark.table.return_value = target_df

    with pytest.raises(
        ValueError, match="missing columns=.*entity_name.*extra columns=.*unexpected"
    ):
        align_dataframe_to_table_contract(source_df, CALENDAR_BRONZE_TABLE)


def test_align_dataframe_to_table_contract_rejects_type_drift() -> None:
    source_df = Mock(spec=DataFrame)
    spark = Mock(spec=SparkSession)
    target_df = Mock(spec=DataFrame)
    source_df.sparkSession = spark
    source_df.columns = ["id"]
    source_df.schema = T.StructType([T.StructField("id", T.StringType())])
    target_df.schema = T.StructType([T.StructField("id", T.IntegerType())])
    spark.table.return_value = target_df

    with pytest.raises(ValueError, match="id: DataFrame=string, table=int"):
        align_dataframe_to_table_contract(source_df, CALENDAR_BRONZE_TABLE)


@pytest.mark.parametrize(
    ("actual_table", "contract_table"),
    [
        (CALENDAR_BRONZE_TABLE, "orderflow_dev.bronze.bronze_calendar"),
        (CUSTOMERS_BRONZE_TABLE, "orderflow_dev.bronze.bronze_customers"),
        (CALENDAR_SILVER_TABLE, "orderflow_dev.silver.silver_calendar"),
        (CUSTOMERS_SILVER_TABLE, "orderflow_dev.silver.silver_customers"),
        (CALENDAR_GOLD_TABLE, "orderflow_dev.gold.dim_calendar"),
        (CUSTOMERS_GOLD_TABLE, "orderflow_dev.gold.dim_customers"),
    ],
)
def test_unity_catalog_table_constants_match_contracts(
    actual_table: str,
    contract_table: str,
) -> None:
    assert actual_table == contract_table
