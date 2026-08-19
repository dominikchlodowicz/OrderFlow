from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from orderflow.bronze.dataset import BronzeDataset

MARKETING_CAMPAIGNS_DATASET = BronzeDataset(
    source_entity="marketing_campaigns",
    columns=(
        "campaign_id",
        "campaign_name",
        "source_channel",
        "start_date",
        "end_date",
        "budget_amount",
        "currency",
        "created_at",
        "updated_at",
        "is_active",
        "load_date",
        "loaded_at",
        "source_event_at",
    ),
)

MARKETING_CAMPAIGNS_COLUMNS = list(MARKETING_CAMPAIGNS_DATASET.columns)
MARKETING_CAMPAIGNS_SCHEMA = MARKETING_CAMPAIGNS_DATASET.schema


def read_marketing_campaigns_raw(
    spark: SparkSession,
    input_path: str | Path,
) -> DataFrame:
    return MARKETING_CAMPAIGNS_DATASET.read_raw(spark=spark, input_path=input_path)


def build_marketing_campaigns_bronze(
    spark: SparkSession,
    input_path: str | Path,
    *,
    source_system: str = "local_files",
    ingestion_run_id: str | None = None,
) -> DataFrame:
    return MARKETING_CAMPAIGNS_DATASET.build_bronze(
        spark=spark,
        input_path=input_path,
        source_system=source_system,
        ingestion_run_id=ingestion_run_id,
    )


def run_marketing_campaigns_bronze(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    MARKETING_CAMPAIGNS_DATASET.run_path(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
    )


def run_marketing_campaigns_bronze_table(
    spark: SparkSession,
    input_path: str | Path,
    output_table: str,
) -> None:
    MARKETING_CAMPAIGNS_DATASET.run_table(
        spark=spark,
        input_path=input_path,
        output_table=output_table,
    )
