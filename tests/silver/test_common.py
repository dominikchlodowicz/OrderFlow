from pathlib import Path
from unittest.mock import Mock

import pytest
from pyspark.sql import DataFrame, SparkSession

from orderflow.config.constants import CALENDAR_BRONZE_TABLE, CALENDAR_SILVER_TABLE
from orderflow.silver import common


def test_write_silver_overwrites_delta_data_and_schema(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    silver_df = Mock(spec=DataFrame)
    output_path = tmp_path / "silver"
    write_delta = Mock()
    monkeypatch.setattr(common, "write_delta", write_delta)

    common.write_silver(
        silver_df=silver_df,
        output_path=output_path,
    )

    write_delta.assert_called_once_with(
        df=silver_df,
        path=output_path,
        mode="overwrite",
        overwrite_schema=True,
    )


def test_run_silver_pipeline_transforms_bronze_and_writes_silver(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spark = Mock(spec=SparkSession)
    bronze_df = Mock(spec=DataFrame)
    silver_df = Mock(spec=DataFrame)
    input_path = tmp_path / "bronze"
    output_path = tmp_path / "silver"
    read_delta = Mock(return_value=bronze_df)
    transform = Mock(return_value=silver_df)
    write_silver = Mock()
    monkeypatch.setattr(common, "read_delta", read_delta)
    monkeypatch.setattr(common, "write_silver", write_silver)

    common.run_silver_pipeline(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
        transform=transform,
    )

    read_delta.assert_called_once_with(
        spark=spark,
        path=input_path,
    )
    transform.assert_called_once_with(bronze_df)
    write_silver.assert_called_once_with(
        silver_df=silver_df,
        output_path=output_path,
    )


def test_run_silver_pipeline_does_not_write_when_transform_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    spark = Mock(spec=SparkSession)
    bronze_df = Mock(spec=DataFrame)
    transform = Mock(side_effect=ValueError("invalid Silver data"))
    write_silver = Mock()
    monkeypatch.setattr(
        common,
        "read_delta",
        Mock(return_value=bronze_df),
    )
    monkeypatch.setattr(common, "write_silver", write_silver)

    with pytest.raises(ValueError, match="invalid Silver data"):
        common.run_silver_pipeline(
            spark=spark,
            input_path=tmp_path / "bronze",
            output_path=tmp_path / "silver",
            transform=transform,
        )

    write_silver.assert_not_called()


def test_run_silver_table_pipeline_uses_registered_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spark = Mock(spec=SparkSession)
    bronze_df = Mock(spec=DataFrame)
    silver_df = Mock(spec=DataFrame)
    read_delta_table = Mock(return_value=bronze_df)
    transform = Mock(return_value=silver_df)
    write_silver_table = Mock()
    monkeypatch.setattr(common, "read_delta_table", read_delta_table)
    monkeypatch.setattr(common, "write_silver_table", write_silver_table)

    common.run_silver_table_pipeline(
        spark=spark,
        input_table=CALENDAR_BRONZE_TABLE,
        output_table=CALENDAR_SILVER_TABLE,
        transform=transform,
    )

    read_delta_table.assert_called_once_with(
        spark=spark,
        table_name=CALENDAR_BRONZE_TABLE,
    )
    transform.assert_called_once_with(bronze_df)
    write_silver_table.assert_called_once_with(
        silver_df=silver_df,
        output_table=CALENDAR_SILVER_TABLE,
    )
