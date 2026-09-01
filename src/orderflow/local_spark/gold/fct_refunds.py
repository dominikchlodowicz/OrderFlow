from orderflow.gold.fct_refunds import run_fct_refunds
from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("gold-fct-refunds")

run_fct_refunds(
    spark=spark,
    refunds_input_path=data_path("silver", "refunds"),
    payments_input_path=data_path("silver", "payments"),
    orders_input_path=data_path("silver", "orders"),
    customers_input_path=data_path("gold", "dim_customers"),
    campaigns_input_path=data_path("gold", "dim_campaigns"),
    currency_input_path=data_path("gold", "dim_currency"),
    calendar_input_path=data_path("gold", "dim_calendar"),
    output_path=data_path("gold", "fct_refunds"),
)

spark.stop()
