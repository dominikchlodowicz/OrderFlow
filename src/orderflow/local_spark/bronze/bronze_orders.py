from orderflow.bronze.orders import run_orders_bronze
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("bronze-orders")

run_orders_bronze(
    spark=spark,
    input_path="data/raw/orders",
    output_path="data/bronze/orders",
)

spark.stop()
