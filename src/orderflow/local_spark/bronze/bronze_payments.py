from orderflow.bronze.payments import run_payments_bronze
from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("bronze-payments")

run_payments_bronze(
    spark=spark,
    input_path=data_path("raw", "payments"),
    output_path=data_path("bronze", "payments"),
)

spark.stop()
