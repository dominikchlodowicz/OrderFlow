from pathlib import Path
from unittest.mock import Mock

import pytest
from pyspark.sql import DataFrame, SparkSession

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
