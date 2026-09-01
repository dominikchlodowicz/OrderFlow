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
    ORDER_ITEMS_GOLD_TABLE,
    ORDER_ITEMS_SILVER_TABLE,
    ORDERS_SILVER_TABLE,
    PRODUCTS_GOLD_TABLE,
)
from orderflow.gold.fct_order_items import run_fct_order_items_tables

# COMMAND ----------

run_databricks_multi_table_to_table_step(
    step_name="Gold fct_order_items transformation",
    runner=run_fct_order_items_tables,
    spark=spark,
    input_tables={
        "order_items_input_table": ORDER_ITEMS_SILVER_TABLE,
        "orders_input_table": ORDERS_SILVER_TABLE,
        "customers_input_table": CUSTOMERS_GOLD_TABLE,
        "products_input_table": PRODUCTS_GOLD_TABLE,
        "campaigns_input_table": CAMPAIGNS_GOLD_TABLE,
        "currency_input_table": CURRENCY_GOLD_TABLE,
        "calendar_input_table": CALENDAR_GOLD_TABLE,
    },
    output_table=ORDER_ITEMS_GOLD_TABLE,
)
