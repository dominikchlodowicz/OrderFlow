from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from orderflow.bronze.dataset import BronzeDataset

WEB_EVENTS_DATASET = BronzeDataset(
    source_entity="web_events",
    columns=(
        "event_id",
        "session_id",
        "customer_id",
        "anonymous_id",
        "event_type",
        "event_timestamp",
        "product_id",
        "campaign_id",
        "device_type",
        "country_code",
        "page_url",
        "load_date",
        "loaded_at",
        "source_event_at",
    ),
)

WEB_EVENTS_COLUMNS = list(WEB_EVENTS_DATASET.columns)
WEB_EVENTS_SCHEMA = WEB_EVENTS_DATASET.schema


def read_web_events_raw(
    spark: SparkSession,
    input_path: str | Path,
) -> DataFrame:
    return WEB_EVENTS_DATASET.read_raw(spark=spark, input_path=input_path)


def build_web_events_bronze(
    spark: SparkSession,
    input_path: str | Path,
    *,
    source_system: str = "local_files",
    ingestion_run_id: str | None = None,
) -> DataFrame:
    return WEB_EVENTS_DATASET.build_bronze(
        spark=spark,
        input_path=input_path,
        source_system=source_system,
        ingestion_run_id=ingestion_run_id,
    )


def run_web_events_bronze(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    WEB_EVENTS_DATASET.run_path(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
    )


def run_web_events_bronze_table(
    spark: SparkSession,
    input_path: str | Path,
    output_table: str,
) -> None:
    WEB_EVENTS_DATASET.run_table(
        spark=spark,
        input_path=input_path,
        output_table=output_table,
    )
