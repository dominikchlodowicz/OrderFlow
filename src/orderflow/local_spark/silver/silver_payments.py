from orderflow.local_spark.session import build_local_spark
from orderflow.silver.payments import run_payments_silver

spark = build_local_spark("silver-payments")

run_payments_silver(
    spark=spark,
    input_path="data/bronze/payments",
    output_path="data/silver/payments",
)

spark.stop()
