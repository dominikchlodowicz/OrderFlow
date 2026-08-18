from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from orderflow.bronze.dataset import BronzeDataset

SHIPMENTS_DATASET = BronzeDataset(
    source_entity="shipments",
    columns=(
        "shipment_id",
        "order_id",
        "carrier",
        "shipment_status",
        "shipped_at",
        "estimated_delivery_at",
        "delivered_at",
        "delivery_country",
        "delivery_city",
        "shipping_cost",
        "load_date",
        "loaded_at",
        "source_event_at",
    ),
)

SHIPMENTS_COLUMNS = list(SHIPMENTS_DATASET.columns)
SHIPMENTS_SCHEMA = SHIPMENTS_DATASET.schema


def read_shipments_raw(
    spark: SparkSession,
    input_path: str | Path,
) -> DataFrame:
    return SHIPMENTS_DATASET.read_raw(spark=spark, input_path=input_path)


def build_shipments_bronze(
    spark: SparkSession,
    input_path: str | Path,
    *,
    source_system: str = "local_files",
    ingestion_run_id: str | None = None,
) -> DataFrame:
    return SHIPMENTS_DATASET.build_bronze(
        spark=spark,
        input_path=input_path,
        source_system=source_system,
        ingestion_run_id=ingestion_run_id,
    )


def run_shipments_bronze(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    SHIPMENTS_DATASET.run_path(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
    )


def run_shipments_bronze_table(
    spark: SparkSession,
    input_path: str | Path,
    output_table: str,
) -> None:
    SHIPMENTS_DATASET.run_table(
        spark=spark,
        input_path=input_path,
        output_table=output_table,
    )
