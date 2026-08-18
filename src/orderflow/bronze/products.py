from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from orderflow.bronze.dataset import BronzeDataset

PRODUCTS_DATASET = BronzeDataset(
    source_entity="products",
    columns=(
        "product_id",
        "sku",
        "product_name",
        "category",
        "brand",
        "unit_price",
        "currency",
        "is_active",
        "created_at",
        "updated_at",
        "load_date",
        "loaded_at",
        "source_event_at",
    ),
)

PRODUCTS_COLUMNS = list(PRODUCTS_DATASET.columns)
PRODUCTS_SCHEMA = PRODUCTS_DATASET.schema


def read_products_raw(
    spark: SparkSession,
    input_path: str | Path,
) -> DataFrame:
    return PRODUCTS_DATASET.read_raw(spark=spark, input_path=input_path)


def build_products_bronze(
    spark: SparkSession,
    input_path: str | Path,
    *,
    source_system: str = "local_files",
    ingestion_run_id: str | None = None,
) -> DataFrame:
    return PRODUCTS_DATASET.build_bronze(
        spark=spark,
        input_path=input_path,
        source_system=source_system,
        ingestion_run_id=ingestion_run_id,
    )


def run_products_bronze(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    PRODUCTS_DATASET.run_path(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
    )


def run_products_bronze_table(
    spark: SparkSession,
    input_path: str | Path,
    output_table: str,
) -> None:
    PRODUCTS_DATASET.run_table(
        spark=spark,
        input_path=input_path,
        output_table=output_table,
    )
