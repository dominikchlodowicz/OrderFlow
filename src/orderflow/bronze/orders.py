from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from orderflow.bronze.dataset import BronzeDataset

ORDERS_DATASET = BronzeDataset(
    source_entity="orders",
    columns=(
        "order_id",
        "customer_id",
        "order_status",
        "order_created_at",
        "order_updated_at",
        "country_code",
        "currency",
        "gross_amount",
        "discount_amount",
        "net_amount",
        "source_channel",
        "campaign_id",
        "load_date",
        "loaded_at",
        "source_event_at",
    ),
)

ORDERS_COLUMNS = list(ORDERS_DATASET.columns)
ORDERS_SCHEMA = ORDERS_DATASET.schema


def read_orders_raw(
    spark: SparkSession,
    input_path: str | Path,
) -> DataFrame:
    return ORDERS_DATASET.read_raw(spark=spark, input_path=input_path)


def build_orders_bronze(
    spark: SparkSession,
    input_path: str | Path,
    *,
    source_system: str = "local_files",
    ingestion_run_id: str | None = None,
) -> DataFrame:
    return ORDERS_DATASET.build_bronze(
        spark=spark,
        input_path=input_path,
        source_system=source_system,
        ingestion_run_id=ingestion_run_id,
    )


def run_orders_bronze(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    ORDERS_DATASET.run_path(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
    )


def run_orders_bronze_table(
    spark: SparkSession,
    input_path: str | Path,
    output_table: str,
) -> None:
    ORDERS_DATASET.run_table(
        spark=spark,
        input_path=input_path,
        output_table=output_table,
    )
