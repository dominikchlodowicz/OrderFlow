from orderflow.bronze.order_items import run_order_items_bronze
from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("bronze-order-items")

run_order_items_bronze(
    spark=spark,
    input_path=data_path("raw", "order_items"),
    output_path=data_path("bronze", "order_items"),
)

spark.stop()
