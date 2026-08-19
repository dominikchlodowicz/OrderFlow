from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from orderflow.bronze.dataset import BronzeDataset

REFUNDS_DATASET = BronzeDataset(
    source_entity="refunds",
    columns=(
        "refund_id",
        "order_id",
        "payment_id",
        "refund_reason",
        "refund_amount",
        "currency",
        "created_at",
        "processed_at",
        "refund_status",
        "load_date",
        "loaded_at",
        "source_event_at",
    ),
)

REFUNDS_COLUMNS = list(REFUNDS_DATASET.columns)
REFUNDS_SCHEMA = REFUNDS_DATASET.schema


def read_refunds_raw(
    spark: SparkSession,
    input_path: str | Path,
) -> DataFrame:
    return REFUNDS_DATASET.read_raw(spark=spark, input_path=input_path)


def build_refunds_bronze(
    spark: SparkSession,
    input_path: str | Path,
    *,
    source_system: str = "local_files",
    ingestion_run_id: str | None = None,
) -> DataFrame:
    return REFUNDS_DATASET.build_bronze(
        spark=spark,
        input_path=input_path,
        source_system=source_system,
        ingestion_run_id=ingestion_run_id,
    )


def run_refunds_bronze(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    REFUNDS_DATASET.run_path(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
    )


def run_refunds_bronze_table(
    spark: SparkSession,
    input_path: str | Path,
    output_table: str,
) -> None:
    REFUNDS_DATASET.run_table(
        spark=spark,
        input_path=input_path,
        output_table=output_table,
    )
