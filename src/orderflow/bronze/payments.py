from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from orderflow.bronze.dataset import BronzeDataset

PAYMENTS_DATASET = BronzeDataset(
    source_entity="payments",
    columns=(
        "payment_id",
        "order_id",
        "payment_attempt_number",
        "payment_method",
        "payment_status",
        "amount",
        "currency",
        "created_at",
        "processed_at",
        "failure_reason",
        "load_date",
        "loaded_at",
        "source_event_at",
    ),
)

PAYMENTS_COLUMNS = list(PAYMENTS_DATASET.columns)
PAYMENTS_SCHEMA = PAYMENTS_DATASET.schema


def read_payments_raw(
    spark: SparkSession,
    input_path: str | Path,
) -> DataFrame:
    return PAYMENTS_DATASET.read_raw(spark=spark, input_path=input_path)


def build_payments_bronze(
    spark: SparkSession,
    input_path: str | Path,
    *,
    source_system: str = "local_files",
    ingestion_run_id: str | None = None,
) -> DataFrame:
    return PAYMENTS_DATASET.build_bronze(
        spark=spark,
        input_path=input_path,
        source_system=source_system,
        ingestion_run_id=ingestion_run_id,
    )


def run_payments_bronze(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    PAYMENTS_DATASET.run_path(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
    )


def run_payments_bronze_table(
    spark: SparkSession,
    input_path: str | Path,
    output_table: str,
) -> None:
    PAYMENTS_DATASET.run_table(
        spark=spark,
        input_path=input_path,
        output_table=output_table,
    )
