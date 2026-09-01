from orderflow.gold.dim_customers import run_dim_customers
from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("gold-customers")

run_dim_customers(
    spark=spark,
    input_path=data_path("silver", "customers"),
    output_path=data_path("gold", "dim_customers"),
)

spark.stop()
