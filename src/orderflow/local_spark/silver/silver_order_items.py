from orderflow.local_spark.session import build_local_spark
from orderflow.silver.order_items import run_order_items_silver

spark = build_local_spark("silver-order-items")

run_order_items_silver(
    spark=spark,
    input_path="data/bronze/order_items",
    output_path="data/silver/order_items",
)

spark.stop()
