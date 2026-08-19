from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from orderflow.bronze.dataset import BronzeDataset

ORDER_ITEMS_DATASET = BronzeDataset(
    source_entity="order_items",
    columns=(
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
        "line_total",
        "created_at",
        "load_date",
        "loaded_at",
        "source_event_at",
    ),
)

ORDER_ITEMS_COLUMNS = list(ORDER_ITEMS_DATASET.columns)
ORDER_ITEMS_SCHEMA = ORDER_ITEMS_DATASET.schema


def read_order_items_raw(
    spark: SparkSession,
    input_path: str | Path,
) -> DataFrame:
    return ORDER_ITEMS_DATASET.read_raw(spark=spark, input_path=input_path)


def build_order_items_bronze(
    spark: SparkSession,
    input_path: str | Path,
    *,
    source_system: str = "local_files",
    ingestion_run_id: str | None = None,
) -> DataFrame:
    return ORDER_ITEMS_DATASET.build_bronze(
        spark=spark,
        input_path=input_path,
        source_system=source_system,
        ingestion_run_id=ingestion_run_id,
    )


def run_order_items_bronze(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    ORDER_ITEMS_DATASET.run_path(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
    )


def run_order_items_bronze_table(
    spark: SparkSession,
    input_path: str | Path,
    output_table: str,
) -> None:
    ORDER_ITEMS_DATASET.run_table(
        spark=spark,
        input_path=input_path,
        output_table=output_table,
    )
