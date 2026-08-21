from orderflow.local_spark.session import build_local_spark
from orderflow.silver.refunds import run_refunds_silver

spark = build_local_spark("silver-refunds")

run_refunds_silver(
    spark=spark,
    input_path="data/bronze/refunds",
    output_path="data/silver/refunds",
)

spark.stop()
