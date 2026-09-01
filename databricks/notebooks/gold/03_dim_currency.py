# Databricks notebook source

# COMMAND ----------

from typing import Any

dbutils: Any
spark: Any

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

bootstrap_databricks_notebook(dbutils=dbutils, spark=spark)

# COMMAND ----------

from orderflow.config.constants import (
    CURRENCY_GOLD_TABLE,
    EXCHANGE_RATES_SILVER_TABLE,
    MARKETING_CAMPAIGNS_SILVER_TABLE,
    ORDERS_SILVER_TABLE,
    PAYMENTS_SILVER_TABLE,
    PRODUCTS_SILVER_TABLE,
    REFUNDS_SILVER_TABLE,
)
from orderflow.gold.dim_currency import run_dim_currency_tables

# COMMAND ----------

run_databricks_multi_table_to_table_step(
    step_name="Gold dim_currency transformation",
    runner=run_dim_currency_tables,
    spark=spark,
    input_tables={
        "products_input_table": PRODUCTS_SILVER_TABLE,
        "marketing_campaigns_input_table": MARKETING_CAMPAIGNS_SILVER_TABLE,
        "orders_input_table": ORDERS_SILVER_TABLE,
        "payments_input_table": PAYMENTS_SILVER_TABLE,
        "refunds_input_table": REFUNDS_SILVER_TABLE,
        "exchange_rates_input_table": EXCHANGE_RATES_SILVER_TABLE,
    },
    output_table=CURRENCY_GOLD_TABLE,
)
