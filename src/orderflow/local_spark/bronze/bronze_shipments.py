from orderflow.bronze.shipments import run_shipments_bronze
from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("bronze-shipments")

run_shipments_bronze(
    spark=spark,
    input_path=data_path("raw", "shipments"),
    output_path=data_path("bronze", "shipments"),
)

spark.stop()
