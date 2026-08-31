from orderflow.local_spark.session import build_local_spark
from orderflow.silver.orders import run_orders_silver

spark = build_local_spark("silver-orders")

run_orders_silver(
    spark=spark,
    input_path="data/bronze/orders",
    output_path="data/silver/orders",
)

spark.stop()
