from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from orderflow.common.delta import (
    read_delta,
    read_delta_table,
    write_delta,
    write_delta_table,
)
from orderflow.common.validation import validate_required_columns

DIM_CUSTOMERS_REQUIRED_COLUMNS = [
    "customer_id",
    "email",
    "first_name",
    "last_name",
    "country_code",
    "city",
    "customer_status",
    "marketing_consent",
    "created_at",
]

NON_NEGATIVE_BIGINT_MASK = (1 << 63) - 1


def transform_dim_customers(
    silver_customers_df: DataFrame,
) -> DataFrame:
    """Convert the current Silver customer snapshot into a Gold dimension."""
    validate_required_columns(
        silver_customers_df,
        DIM_CUSTOMERS_REQUIRED_COLUMNS,
        dataset_name="Silver customers",
    )

    email_domain = F.when(
        F.col("email").rlike(r"^[^@]+@[^@]+$"),
        F.lower(F.substring_index(F.col("email"), "@", -1)),
    )

    dim_customers_df = silver_customers_df.select(
        F.xxhash64(F.col("customer_id"))
        .bitwiseAND(F.lit(NON_NEGATIVE_BIGINT_MASK))
        .alias("customer_key"),
        F.col("customer_id"),
        F.col("email"),
        email_domain.alias("email_domain"),
        F.col("first_name"),
        F.col("last_name"),
        F.concat_ws(
            " ",
            F.col("first_name"),
            F.col("last_name"),
        ).alias("full_name"),
        F.col("country_code"),
        F.col("city"),
        F.col("customer_status"),
        F.coalesce(
            F.col("customer_status") == F.lit("active"),
            F.lit(False),
        ).alias("is_active_customer"),
        F.col("marketing_consent"),
        F.col("created_at").alias("registered_at"),
        F.to_date(F.col("created_at")).alias("registration_date"),
    )

    validate_dim_customers(dim_customers_df)

    return dim_customers_df.withColumn(
        "_gold_processed_at",
        F.current_timestamp(),
    )


def validate_dim_customers(df: DataFrame) -> None:
    null_key_count = df.filter(
        F.col("customer_key").isNull() | F.col("customer_id").isNull()
    ).count()

    if null_key_count > 0:
        raise ValueError(f"dim_customers validation failed: {null_key_count} rows have null keys.")

    null_required_attribute_count = df.filter(
        F.col("country_code").isNull()
        | F.col("is_active_customer").isNull()
        | F.col("marketing_consent").isNull()
    ).count()

    if null_required_attribute_count > 0:
        raise ValueError(
            "dim_customers validation failed: "
            f"{null_required_attribute_count} rows have null required attributes."
        )

    duplicate_customer_id_count = (
        df.groupBy("customer_id").count().filter(F.col("count") > 1).count()
    )

    if duplicate_customer_id_count > 0:
        raise ValueError(
            "dim_customers validation failed: "
            f"{duplicate_customer_id_count} duplicate customer_id values found."
        )

    duplicate_customer_key_count = (
        df.groupBy("customer_key").count().filter(F.col("count") > 1).count()
    )

    if duplicate_customer_key_count > 0:
        raise ValueError(
            "dim_customers validation failed: "
            f"{duplicate_customer_key_count} duplicate customer_key values found."
        )


def write_dim_customers(
    dim_customers_df: DataFrame,
    output_path: str | Path,
) -> None:
    write_delta(
        df=dim_customers_df,
        path=output_path,
        mode="overwrite",
        overwrite_schema=True,
    )


def write_dim_customers_table(
    dim_customers_df: DataFrame,
    output_table: str,
) -> None:
    write_delta_table(
        df=dim_customers_df,
        table_name=output_table,
        mode="overwrite",
    )


def run_dim_customers(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
) -> None:
    silver_customers_df = read_delta(
        spark=spark,
        path=input_path,
    )

    dim_customers_df = transform_dim_customers(silver_customers_df)

    write_dim_customers(
        dim_customers_df=dim_customers_df,
        output_path=output_path,
    )


def run_dim_customers_tables(
    spark: SparkSession,
    input_table: str,
    output_table: str,
) -> None:
    silver_customers_df = read_delta_table(
        spark=spark,
        table_name=input_table,
    )

    dim_customers_df = transform_dim_customers(silver_customers_df)

    write_dim_customers_table(
        dim_customers_df=dim_customers_df,
        output_table=output_table,
    )
