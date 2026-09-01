from orderflow.bronze.customers import run_customers_bronze
from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("bronze-calendar")

run_customers_bronze(
    spark=spark,
    input_path=data_path("raw", "customers"),
    output_path=data_path("bronze", "customers"),
)

spark.stop()
