from orderflow.gold.fct_shipments import run_fct_shipments
from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("gold-fct-shipments")

run_fct_shipments(
    spark=spark,
    shipments_input_path=data_path("silver", "shipments"),
    orders_input_path=data_path("silver", "orders"),
    customers_input_path=data_path("gold", "dim_customers"),
    campaigns_input_path=data_path("gold", "dim_campaigns"),
    calendar_input_path=data_path("gold", "dim_calendar"),
    output_path=data_path("gold", "fct_shipments"),
)

spark.stop()
