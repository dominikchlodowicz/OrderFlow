from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from orderflow.common.validation import validate_required_columns
from orderflow.silver.common import run_silver_pipeline

CUSTOMERS_REQUIRED_COLUMNS = [
    "customer_id",
    "email",
    "first_name",
    "last_name",
    "country_code",
    "city",
    "created_at",
    "updated_at",
    "customer_status",
    "marketing_consent",
    "load_date",
    "loaded_at",
    "source_event_at",
]


def normalize_blank_to_null(
    column_name: str,
) -> Column:
    return F.when(
        F.trim(F.col(column_name)) == "",
        F.lit(None),
    ).otherwise(F.trim(F.col(column_name)))


def try_cast_column(
    column_name: str,
    target_type: str,
) -> Column:
    return F.expr(f"try_cast(`{column_name}` as {target_type})")


def transform_customers_silver(
    bronze_df: DataFrame,
) -> DataFrame:
    validate_required_columns(
        bronze_df,
        CUSTOMERS_REQUIRED_COLUMNS,
        dataset_name="Bronze customers",
    )

    silver_df = bronze_df.select(
        F.lower(
            normalize_blank_to_null("customer_id"),
        ).alias("customer_id"),
        F.lower(normalize_blank_to_null("email")).alias("email"),
        normalize_blank_to_null("first_name").alias("first_name"),
        normalize_blank_to_null("last_name").alias("last_name"),
        normalize_blank_to_null("country_code").alias("country_code"),
        normalize_blank_to_null("city").alias("city"),
        try_cast_column(
            "created_at",
            "timestamp",
        ).alias("created_at"),
        try_cast_column(
            "updated_at",
            "timestamp",
        ).alias("updated_at"),
        F.lower(normalize_blank_to_null("customer_status")).alias("customer_status"),
        try_cast_column(
            "marketing_consent",
            "boolean",
        ).alias("marketing_consent"),
        try_cast_column(
            "load_date",
            "date",
        ).alias("load_date"),
        try_cast_column(
            "loaded_at",
            "timestamp",
        ).alias("loaded_at"),
        try_cast_column(
            "source_event_at",
            "timestamp",
        ).alias("source_event_at"),
    ).withColumn(
        "_silver_processed_at",
        F.current_timestamp(),
    )

    latest_row_per_date = Window.partitionBy("customer_id").orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("source_event_at").desc_nulls_last(),
        F.col("loaded_at").desc_nulls_last(),
        F.col("load_date").desc_nulls_last(),
    )

    silver_df = (
        silver_df.withColumn(
            "_row_number",
            F.row_number().over(latest_row_per_date),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

    validate_customers_silver(silver_df)

    return silver_df


def validate_customers_silver(
    df: DataFrame,
) -> None:
    invalid_required_rows = df.filter(
        F.col("customer_id").isNull()
        | F.col("email").isNull()
        | F.col("first_name").isNull()
        | F.col("last_name").isNull()
    ).count()

    if invalid_required_rows > 0:
        raise ValueError(
            "Customers Silver validation failed: "
            f"{invalid_required_rows} rows have null required fields."
        )

    duplicate_customer_ids = df.groupBy("customer_id").count().filter(F.col("count") > 1).count()

    if duplicate_customer_ids > 0:
        raise ValueError(
            "Customers Silver validation failed: "
            f"{duplicate_customer_ids} duplicate customer_id values found."
        )

    duplicate_emails = df.groupBy("email").count().filter(F.col("count") > 1).count()

    if duplicate_emails > 0:
        raise ValueError(
            f"Customers Silver validation failed: {duplicate_emails} duplicate email values found."
        )


def run_customers_silver(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    run_silver_pipeline(
        spark=spark,
        input_path=input_path,
        output_path=output_path,
        transform=transform_customers_silver,
    )
