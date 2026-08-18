from orderflow.bronze.payments import run_payments_bronze
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("bronze-payments")

run_payments_bronze(
    spark=spark,
    input_path="data/raw/payments",
    output_path="data/bronze/payments",
)

spark.stop()
