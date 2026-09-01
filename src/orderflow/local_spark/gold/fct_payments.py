from orderflow.gold.fct_payments import run_fct_payments
from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("gold-fct-payments")

run_fct_payments(
    spark=spark,
    payments_input_path=data_path("silver", "payments"),
    orders_input_path=data_path("silver", "orders"),
    customers_input_path=data_path("gold", "dim_customers"),
    campaigns_input_path=data_path("gold", "dim_campaigns"),
    currency_input_path=data_path("gold", "dim_currency"),
    calendar_input_path=data_path("gold", "dim_calendar"),
    output_path=data_path("gold", "fct_payments"),
)

spark.stop()
