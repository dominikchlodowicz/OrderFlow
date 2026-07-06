# reusable bronze infra
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

@dataclass(frozen=True)
class BronzeWriteConfig:
    input_path: str | Path
    output_path: str | Path
    source_system: str
    source_entity: str
    ingestion_run_id: str


    FILE_NAME_PATTERN = r"([^/\\]+)$"

def add_standard_bronze_metadata(
        df: DataFrame,
        *,
        source_system: str,
        source_entity: str,
        ingestion_run_id: str,
        raw_columns: list[str],
) -> DataFrame:
    LOAD_DATE_PATTERN = r"load_date=(\d{4}-\d{2}-\d{2})"
    FILE_NAME_PATTERN = r"([^/\\]+)$"

    source_file_path = F.input_file_name()

    raw_record_hash = F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(column_name).cast("string"), F.lit("<NULL>"))
                for column_name in raw_columns
            ],
        ),
        256,
    )

    return  (
        df
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_source_entity", F.lit(source_entity))
        .withColumn(
            "_source_file_name",
            F.regexp_extract(source_file_path, FILE_NAME_PATTERN, 1),

        )
        .withColumn(
            "_source_load_date",
            F.regexp_extract(
                source_file_path,
                LOAD_DATE_PATTERN,
                1,
            )
        )
        .withColumn("_source_file_path", source_file_path)
        .withColumn("_ingestion_run_id", F.lit(ingestion_run_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_raw_record_hash", raw_record_hash)
    )


def validate_bronze_dataframe(df: DataFrame, *, source_entity: str) -> None:
    if df.limit(1).count() == 0:
        raise ValueError(f"Bronze batch for '{source_entity}' is empty.")
    
    missing_load_date_count = (
        df
        .filter(
            F.col("_source_load_date").isNull()
            | (F.col("_source_load_date") == "")
        )
        .limit(1)
        .count()
    )

    if missing_load_date_count > 0:
        raise ValueError(
            f"Bronze batch for '{source_entity}' contains rows without "
            "_source_load_date. Expected folder pattern: load_date=YYYY-MM-DD."
        )


def get_source_load_dates(df: DataFrame) -> list[str]:
    rows = (
        df
        .select("_source_load_date")
        .distinct()
        .orderBy("_source_load_date")
        .collect()
    )

    return [row["_source_load_date"] for row in rows]


def build_replace_where(load_dates: Iterable[str]) -> str:
    """
    When rewriting Bronze table, only replace the days that are currently getting loaded.
    """
    load_dates = list(load_dates)

    if not load_dates:
        raise ValueError("Cannot build replaceWhere predicate for empty load date list.")
    
    quoted_dates = ", ".join(f"'{load_date}'" for load_date in load_dates)

    return f"_source_load_date IN ({quoted_dates})"


def write_delta_idempotent_by_load_date(
        spark: SparkSession,
        df: DataFrame,
        *,
        output_path: str | Path,
        source_entity: str,
) -> None:
    """
    Writes Bronze Delta data idempotently.

    First run:
        Create the Delta table.

    Later runs:
        Replace only the _source_load_date partitions present in this batch.

    This avoids duplicate Bronze rows when the same batch is rerun.
    """

    output_path = str(output_path)

    validate_bronze_dataframe(df, source_entity=source_entity)

    if not DeltaTable.isDeltaTable(spark, output_path):
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("_source_load_date")
            .save(output_path)
        )
        return

    source_load_dates = get_source_load_dates(df)
    replace_where = build_replace_where(source_load_dates)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", replace_where)
        .save(output_path)
    )