from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from orderflow.common.delta import (
    read_delta,
    read_delta_table,
)
from orderflow.common.validation import validate_required_columns
from orderflow.gold.common import (
    ANONYMOUS_CUSTOMER_ID,
    ANONYMOUS_CUSTOMER_KEY,
    GUEST_CUSTOMER_ID,
    GUEST_CUSTOMER_KEY,
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

SPECIAL_CUSTOMER_SCHEMA = T.StructType(
    [
        T.StructField("customer_key", T.LongType(), nullable=False),
        T.StructField("customer_id", T.StringType(), nullable=False),
        T.StructField("email", T.StringType(), nullable=True),
        T.StructField("email_domain", T.StringType(), nullable=True),
        T.StructField("first_name", T.StringType(), nullable=True),
        T.StructField("last_name", T.StringType(), nullable=True),
        T.StructField("full_name", T.StringType(), nullable=True),
        T.StructField("country_code", T.StringType(), nullable=False),
        T.StructField("city", T.StringType(), nullable=True),
        T.StructField("customer_status", T.StringType(), nullable=True),
        T.StructField("is_active_customer", T.BooleanType(), nullable=False),
        T.StructField("marketing_consent", T.BooleanType(), nullable=False),
        T.StructField("registered_at", T.TimestampType(), nullable=True),
        T.StructField("registration_date", T.DateType(), nullable=True),
    ]
)

SPECIAL_CUSTOMERS = [
    (
        UNKNOWN_KEY,
        UNKNOWN_MEMBER_ID,
        None,
        None,
        None,
        None,
        "Unknown customer",
        "ZZ",
        None,
        None,
        False,
        False,
        None,
        None,
    ),
    (
        GUEST_CUSTOMER_KEY,
        GUEST_CUSTOMER_ID,
        None,
        None,
        None,
        None,
        "Guest customer",
        "ZZ",
        None,
        None,
        False,
        False,
        None,
        None,
    ),
    (
        ANONYMOUS_CUSTOMER_KEY,
        ANONYMOUS_CUSTOMER_ID,
        None,
        None,
        None,
        None,
        "Anonymous customer",
        "ZZ",
        None,
        None,
        False,
        False,
        None,
        None,
    ),
]


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

    business_customers_df = silver_customers_df.select(
        surrogate_key("customer_id").alias("customer_key"),
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

    special_customers_df = silver_customers_df.sparkSession.createDataFrame(
        SPECIAL_CUSTOMERS,
        schema=SPECIAL_CUSTOMER_SCHEMA,
    )
    dim_customers_df = business_customers_df.unionByName(special_customers_df)

    validate_dim_customers(dim_customers_df)

    return with_gold_processed_at(dim_customers_df)


def validate_dim_customers(df: DataFrame) -> None:
    dataset_name = "dim_customers"
    validate_required_values(
        df,
        required_columns=[
            "customer_key",
            "customer_id",
            "country_code",
            "is_active_customer",
            "marketing_consent",
        ],
        dataset_name=dataset_name,
    )
    validate_rule(
        df,
        invalid_when=~F.col("country_code").rlike(r"^[A-Z]{2}$"),
        dataset_name=dataset_name,
        rule_description="have invalid country codes",
    )
    validate_unique_key(
        df,
        key_columns=["customer_id"],
        dataset_name=dataset_name,
    )
    validate_unique_key(
        df,
        key_columns=["customer_key"],
        dataset_name=dataset_name,
    )


def write_dim_customers(
    dim_customers_df: DataFrame,
    output_path: str | Path,
) -> None:
    write_gold(dim_customers_df, output_path)


def write_dim_customers_table(
    dim_customers_df: DataFrame,
    output_table: str,
) -> None:
    write_gold_table(dim_customers_df, output_table)


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
