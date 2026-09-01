from orderflow.bronze.refunds import run_refunds_bronze
from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("bronze-refunds")

run_refunds_bronze(
    spark=spark,
    input_path=data_path("raw", "refunds"),
    output_path=data_path("bronze", "refunds"),
)

spark.stop()
