from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from orderflow.bronze.dataset import BronzeDataset

EXCHANGE_RATES_DATASET = BronzeDataset(
    source_entity="exchange_rates",
    columns=(
        "rate_date",
        "currency",
        "rate_to_pln",
        "source",
        "load_date",
        "loaded_at",
    ),
)

EXCHANGE_RATES_COLUMNS = list(EXCHANGE_RATES_DATASET.columns)
EXCHANGE_RATES_SCHEMA = EXCHANGE_RATES_DATASET.schema


def read_exchange_rates_raw(
    spark: SparkSession,
    input_path: str | Path,
) -> DataFrame:
    return EXCHANGE_RATES_DATASET.read_raw(spark=spark, input_path=input_path)


def build_exchange_rates_bronze(
    spark: SparkSession,
    input_path: str | Path,
    *,
    source_system: str = "local_files",
    ingestion_run_id: str | None = None,
) -> DataFrame:
    return EXCHANGE_RATES_DATASET.build_bronze(
        spark=spark,
        input_path=input_path,
        source_system=source_system,
        ingestion_run_id=ingestion_run_id,
    )


def run_exchange_rates_bronze(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    EXCHANGE_RATES_DATASET.run_path(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
    )


def run_exchange_rates_bronze_table(
    spark: SparkSession,
    input_path: str | Path,
    output_table: str,
) -> None:
    EXCHANGE_RATES_DATASET.run_table(
        spark=spark,
        input_path=input_path,
        output_table=output_table,
    )
