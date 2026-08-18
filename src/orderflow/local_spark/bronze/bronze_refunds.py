from orderflow.bronze.refunds import run_refunds_bronze
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("bronze-refunds")

run_refunds_bronze(
    spark=spark,
    input_path="data/raw/refunds",
    output_path="data/bronze/refunds",
)

spark.stop()
