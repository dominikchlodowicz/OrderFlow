from orderflow.bronze.shipments import run_shipments_bronze
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("bronze-shipments")

run_shipments_bronze(
    spark=spark,
    input_path="data/raw/shipments",
    output_path="data/bronze/shipments",
)

spark.stop()
