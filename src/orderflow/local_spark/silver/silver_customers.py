from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark
from orderflow.silver.customers import run_customers_silver

spark = build_local_spark("silver-customers")

run_customers_silver(
    spark=spark,
    input_path=data_path("bronze", "customers"),
    output_path=data_path("silver", "customers"),
)

spark.stop()
