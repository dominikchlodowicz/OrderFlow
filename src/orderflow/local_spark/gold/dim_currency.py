from orderflow.gold.dim_currency import run_dim_currency
from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("gold-dim-currency")

run_dim_currency(
    spark=spark,
    products_input_path=data_path("silver", "products"),
    marketing_campaigns_input_path=data_path("silver", "marketing_campaigns"),
    orders_input_path=data_path("silver", "orders"),
    payments_input_path=data_path("silver", "payments"),
    refunds_input_path=data_path("silver", "refunds"),
    exchange_rates_input_path=data_path("silver", "exchange_rates"),
    output_path=data_path("gold", "dim_currency"),
)

spark.stop()
