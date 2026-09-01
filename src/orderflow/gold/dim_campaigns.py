from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from orderflow.common.delta import read_delta, read_delta_table
from orderflow.common.validation import validate_required_columns
from orderflow.gold.common import (
    NO_CAMPAIGN_ID,
    NO_CAMPAIGN_KEY,
    UNKNOWN_KEY,
    UNKNOWN_MEMBER_ID,
    surrogate_key,
    validate_required_values,
    validate_rule,
    validate_unique_key,
    with_gold_processed_at,
    write_gold,
    write_gold_table,
)

DIM_CAMPAIGNS_REQUIRED_COLUMNS = [
    "campaign_id",
    "campaign_name",
    "source_channel",
    "start_date",
    "end_date",
    "budget_amount",
    "currency",
    "is_active",
    "created_at",
    "updated_at",
]

SPECIAL_CAMPAIGN_KEYS = [UNKNOWN_KEY, NO_CAMPAIGN_KEY]

SPECIAL_CAMPAIGN_SCHEMA = T.StructType(
    [
        T.StructField("campaign_key", T.LongType(), nullable=False),
        T.StructField("campaign_id", T.StringType(), nullable=False),
        T.StructField("campaign_name", T.StringType(), nullable=False),
        T.StructField("source_channel", T.StringType(), nullable=True),
        T.StructField("start_date", T.DateType(), nullable=False),
        T.StructField("end_date", T.DateType(), nullable=True),
        T.StructField("budget_amount", T.DecimalType(18, 2), nullable=False),
        T.StructField("budget_currency_code", T.StringType(), nullable=False),
        T.StructField("is_active", T.BooleanType(), nullable=False),
        T.StructField("created_at", T.TimestampType(), nullable=False),
        T.StructField("updated_at", T.TimestampType(), nullable=True),
    ]
)

SPECIAL_CAMPAIGNS = [
    (
        UNKNOWN_KEY,
        UNKNOWN_MEMBER_ID,
        "Unknown campaign",
        None,
        date(1970, 1, 1),
        None,
        Decimal("0.00"),
        "XXX",
        False,
        datetime(1970, 1, 1),
        None,
    ),
    (
        NO_CAMPAIGN_KEY,
        NO_CAMPAIGN_ID,
        "No campaign",
        None,
        date(1970, 1, 1),
        None,
        Decimal("0.00"),
        "XXX",
        False,
        datetime(1970, 1, 1),
        None,
    ),
]


def transform_dim_campaigns(silver_campaigns_df: DataFrame) -> DataFrame:
    """Convert the current Silver marketing-campaign snapshot into Gold."""
    validate_required_columns(
        silver_campaigns_df,
        DIM_CAMPAIGNS_REQUIRED_COLUMNS,
        dataset_name="Silver marketing_campaigns",
    )

    business_campaigns_df = silver_campaigns_df.select(
        surrogate_key("campaign_id").alias("campaign_key"),
        F.col("campaign_id"),
        F.col("campaign_name"),
        F.col("source_channel"),
        F.col("start_date").cast("date").alias("start_date"),
        F.col("end_date").cast("date").alias("end_date"),
        F.col("budget_amount").cast("decimal(18,2)").alias("budget_amount"),
        F.col("currency").alias("budget_currency_code"),
        F.col("is_active").cast("boolean").alias("is_active"),
        F.col("created_at").cast("timestamp").alias("created_at"),
        F.col("updated_at").cast("timestamp").alias("updated_at"),
    )
    special_campaigns_df = silver_campaigns_df.sparkSession.createDataFrame(
        SPECIAL_CAMPAIGNS,
        schema=SPECIAL_CAMPAIGN_SCHEMA,
    )
    dim_campaigns_df = business_campaigns_df.unionByName(special_campaigns_df)

    validate_dim_campaigns(dim_campaigns_df)
    return with_gold_processed_at(dim_campaigns_df)


def validate_dim_campaigns(df: DataFrame) -> None:
    dataset_name = "dim_campaigns"
    validate_required_values(
        df,
        required_columns=[
            "campaign_key",
            "campaign_id",
            "campaign_name",
            "start_date",
            "budget_amount",
            "budget_currency_code",
            "is_active",
            "created_at",
        ],
        dataset_name=dataset_name,
    )
    regular_campaign = ~F.col("campaign_key").isin(SPECIAL_CAMPAIGN_KEYS)
    validate_rule(
        df,
        invalid_when=regular_campaign & (F.col("budget_amount") <= 0),
        dataset_name=dataset_name,
        rule_description="have non-positive campaign budgets",
    )
    validate_rule(
        df,
        invalid_when=(F.col("end_date").isNotNull() & (F.col("end_date") < F.col("start_date")))
        | (F.col("updated_at").isNotNull() & (F.col("updated_at") < F.col("created_at"))),
        dataset_name=dataset_name,
        rule_description="have invalid campaign chronology",
    )
    validate_rule(
        df,
        invalid_when=~F.col("budget_currency_code").rlike(r"^[A-Z]{3}$"),
        dataset_name=dataset_name,
        rule_description="have invalid currency codes",
    )
    validate_unique_key(
        df,
        key_columns=["campaign_id"],
        dataset_name=dataset_name,
    )
    validate_unique_key(
        df,
        key_columns=["campaign_key"],
        dataset_name=dataset_name,
    )


def write_dim_campaigns(dim_campaigns_df: DataFrame, output_path: str | Path) -> None:
    write_gold(dim_campaigns_df, output_path)


def write_dim_campaigns_table(dim_campaigns_df: DataFrame, output_table: str) -> None:
    write_gold_table(dim_campaigns_df, output_table)


def run_dim_campaigns(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    silver_campaigns_df = read_delta(spark=spark, path=input_path)
    write_dim_campaigns(
        transform_dim_campaigns(silver_campaigns_df),
        output_path,
    )


def run_dim_campaigns_tables(
    spark: SparkSession,
    input_table: str,
    output_table: str,
) -> None:
    silver_campaigns_df = read_delta_table(spark=spark, table_name=input_table)
    write_dim_campaigns_table(
        transform_dim_campaigns(silver_campaigns_df),
        output_table,
    )
