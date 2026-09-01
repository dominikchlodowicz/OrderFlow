from orderflow.gold.fct_web_events import run_fct_web_events
from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("gold-fct-web-events")

run_fct_web_events(
    spark=spark,
    web_events_input_path=data_path("silver", "web_events"),
    customers_input_path=data_path("gold", "dim_customers"),
    products_input_path=data_path("gold", "dim_products"),
    campaigns_input_path=data_path("gold", "dim_campaigns"),
    calendar_input_path=data_path("gold", "dim_calendar"),
    output_path=data_path("gold", "fct_web_events"),
)

spark.stop()
