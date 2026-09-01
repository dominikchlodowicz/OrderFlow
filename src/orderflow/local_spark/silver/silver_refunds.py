from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark
from orderflow.silver.refunds import run_refunds_silver

spark = build_local_spark("silver-refunds")

run_refunds_silver(
    spark=spark,
    input_path=data_path("bronze", "refunds"),
    output_path=data_path("silver", "refunds"),
)

spark.stop()
