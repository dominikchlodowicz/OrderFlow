from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from orderflow.bronze.common import (
    add_standard_bronze_metadata,
    select_bronze_contract_columns,
    validate_bronze_dataframe,
)
from orderflow.common.delta import write_delta, write_delta_table
from orderflow.common.validation import validate_required_columns
from orderflow.config.constants import ADLS_SOURCE_SYSTEM


@dataclass(frozen=True)
class BronzeDataset:
    """Declarative CSV ingestion for a single raw-to-Bronze contract."""

    source_entity: str
    columns: tuple[str, ...]

    @property
    def schema(self) -> StructType:
        return StructType(
            [
                StructField(
                    column_name,
                    StringType(),
                    nullable=True,
                )
                for column_name in self.columns
            ]
        )

    def read_raw(
        self,
        spark: SparkSession,
        input_path: str | Path,
    ) -> DataFrame:
        raw_df = (
            spark.read.format("csv")
            .schema(self.schema)
            .option("header", "true")
            .option("recursiveFileLookup", "true")
            .load(str(input_path))
        )

        validate_required_columns(
            raw_df,
            list(self.columns),
            dataset_name=f"Raw {self.source_entity}",
        )

        return raw_df

    def build_bronze(
        self,
        spark: SparkSession,
        input_path: str | Path,
        *,
        source_system: str = "local_files",
        ingestion_run_id: str | None = None,
    ) -> DataFrame:
        raw_columns = list(self.columns)
        raw_df = self.read_raw(spark=spark, input_path=input_path)

        bronze_df = add_standard_bronze_metadata(
            raw_df,
            source_system=source_system,
            source_entity=self.source_entity,
            ingestion_run_id=ingestion_run_id or uuid4().hex,
            raw_columns=raw_columns,
        )

        contract_df = select_bronze_contract_columns(
            bronze_df,
            raw_columns=raw_columns,
        )
        validate_bronze_dataframe(
            contract_df,
            source_entity=self.source_entity,
        )

        return contract_df

    def run_path(
        self,
        spark: SparkSession,
        input_path: str | Path,
        output_path: str | Path,
    ) -> None:
        bronze_df = self.build_bronze(
            spark=spark,
            input_path=input_path,
        )

        write_delta(
            df=bronze_df,
            path=output_path,
            mode="overwrite",
            overwrite_schema=True,
            partition_by=["_source_load_date"],
        )

    def run_table(
        self,
        spark: SparkSession,
        input_path: str | Path,
        output_table: str,
    ) -> None:
        bronze_df = self.build_bronze(
            spark=spark,
            input_path=input_path,
            source_system=ADLS_SOURCE_SYSTEM,
        )

        write_delta_table(
            df=bronze_df,
            table_name=output_table,
            mode="overwrite",
        )
