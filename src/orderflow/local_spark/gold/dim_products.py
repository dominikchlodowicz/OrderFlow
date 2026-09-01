from orderflow.gold.dim_products import run_dim_products
from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("gold-dim-products")

run_dim_products(
    spark=spark,
    input_path=data_path("silver", "products"),
    output_path=data_path("gold", "dim_products"),
)

spark.stop()
