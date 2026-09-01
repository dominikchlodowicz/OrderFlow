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
    CALENDAR_GOLD_TABLE,
    CAMPAIGNS_GOLD_TABLE,
    CURRENCY_GOLD_TABLE,
    CUSTOMERS_GOLD_TABLE,
    ORDERS_SILVER_TABLE,
    PAYMENTS_SILVER_TABLE,
    REFUNDS_GOLD_TABLE,
    REFUNDS_SILVER_TABLE,
)
from orderflow.gold.fct_refunds import run_fct_refunds_tables

# COMMAND ----------

run_databricks_multi_table_to_table_step(
    step_name="Gold fct_refunds transformation",
    runner=run_fct_refunds_tables,
    spark=spark,
    input_tables={
        "refunds_input_table": REFUNDS_SILVER_TABLE,
        "payments_input_table": PAYMENTS_SILVER_TABLE,
        "orders_input_table": ORDERS_SILVER_TABLE,
        "customers_input_table": CUSTOMERS_GOLD_TABLE,
        "campaigns_input_table": CAMPAIGNS_GOLD_TABLE,
        "currency_input_table": CURRENCY_GOLD_TABLE,
        "calendar_input_table": CALENDAR_GOLD_TABLE,
    },
    output_table=REFUNDS_GOLD_TABLE,
)
