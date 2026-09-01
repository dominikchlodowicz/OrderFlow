from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark
from orderflow.silver.payments import run_payments_silver

spark = build_local_spark("silver-payments")

run_payments_silver(
    spark=spark,
    input_path=data_path("bronze", "payments"),
    output_path=data_path("silver", "payments"),
)

spark.stop()
