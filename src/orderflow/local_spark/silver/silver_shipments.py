from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark
from orderflow.silver.shipments import run_shipments_silver

spark = build_local_spark("silver-shipments")

run_shipments_silver(
    spark=spark,
    input_path=data_path("bronze", "shipments"),
    output_path=data_path("silver", "shipments"),
)

spark.stop()
