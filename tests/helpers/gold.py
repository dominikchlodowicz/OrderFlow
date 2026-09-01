from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from orderflow.gold.common import (
    ANONYMOUS_CUSTOMER_ID,
    ANONYMOUS_CUSTOMER_KEY,
    GUEST_CUSTOMER_ID,
    GUEST_CUSTOMER_KEY,
    NO_CAMPAIGN_ID,
    NO_CAMPAIGN_KEY,
    NOT_APPLICABLE_PRODUCT_ID,
    NOT_APPLICABLE_PRODUCT_KEY,
    UNKNOWN_KEY,
    UNKNOWN_MEMBER_ID,
)


def customer_dimension_df(spark: SparkSession) -> DataFrame:
    schema = T.StructType(
        [
            T.StructField("customer_id", T.StringType(), nullable=False),
            T.StructField("customer_key", T.LongType(), nullable=False),
        ]
    )
    return spark.createDataFrame(
        [
            ("cust_001", 101),
            (UNKNOWN_MEMBER_ID, UNKNOWN_KEY),
            (GUEST_CUSTOMER_ID, GUEST_CUSTOMER_KEY),
            (ANONYMOUS_CUSTOMER_ID, ANONYMOUS_CUSTOMER_KEY),
        ],
        schema=schema,
    )


def campaign_dimension_df(spark: SparkSession) -> DataFrame:
    schema = T.StructType(
        [
            T.StructField("campaign_id", T.StringType(), nullable=False),
            T.StructField("campaign_key", T.LongType(), nullable=False),
        ]
    )
    return spark.createDataFrame(
        [
            ("cmp_001", 201),
            (UNKNOWN_MEMBER_ID, UNKNOWN_KEY),
            (NO_CAMPAIGN_ID, NO_CAMPAIGN_KEY),
        ],
        schema=schema,
    )


def product_dimension_df(spark: SparkSession) -> DataFrame:
    schema = T.StructType(
        [
            T.StructField("product_id", T.StringType(), nullable=False),
            T.StructField("product_key", T.LongType(), nullable=False),
        ]
    )
    return spark.createDataFrame(
        [
            ("prod_001", 301),
            (UNKNOWN_MEMBER_ID, UNKNOWN_KEY),
            (NOT_APPLICABLE_PRODUCT_ID, NOT_APPLICABLE_PRODUCT_KEY),
        ],
        schema=schema,
    )


def currency_dimension_df(spark: SparkSession) -> DataFrame:
    schema = T.StructType(
        [
            T.StructField("currency_code", T.StringType(), nullable=False),
            T.StructField("currency_key", T.LongType(), nullable=False),
        ]
    )
    return spark.createDataFrame(
        [("PLN", 401), ("EUR", 402)],
        schema=schema,
    )


def calendar_dimension_df(spark: SparkSession) -> DataFrame:
    schema = T.StructType(
        [
            T.StructField("date_day", T.DateType(), nullable=False),
            T.StructField("date_key", T.IntegerType(), nullable=False),
        ]
    )
    dates = [date(2026, 6, day) for day in range(1, 16)]
    return spark.createDataFrame(
        [(date_day, int(date_day.strftime("%Y%m%d"))) for date_day in dates],
        schema=schema,
    )
