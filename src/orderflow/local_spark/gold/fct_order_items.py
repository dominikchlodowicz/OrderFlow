from orderflow.gold.fct_order_items import run_fct_order_items
from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("gold-fct-order-items")

run_fct_order_items(
    spark=spark,
    order_items_input_path=data_path("silver", "order_items"),
    orders_input_path=data_path("silver", "orders"),
    customers_input_path=data_path("gold", "dim_customers"),
    products_input_path=data_path("gold", "dim_products"),
    campaigns_input_path=data_path("gold", "dim_campaigns"),
    currency_input_path=data_path("gold", "dim_currency"),
    calendar_input_path=data_path("gold", "dim_calendar"),
    output_path=data_path("gold", "fct_order_items"),
)

spark.stop()
