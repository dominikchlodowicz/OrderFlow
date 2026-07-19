from orderflow.silver.customers import run_customers_silver
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("silver-customers")

run_customers_silver(
    spark=spark,
    input_path="data/bronze/customers",
    output_path="data/silver/customers"
)

spark.stop()