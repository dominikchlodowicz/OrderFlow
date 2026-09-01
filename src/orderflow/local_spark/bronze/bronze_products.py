from orderflow.bronze.products import run_products_bronze
from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("bronze-products")

run_products_bronze(
    spark=spark,
    input_path=data_path("raw", "products"),
    output_path=data_path("bronze", "products"),
)

spark.stop()
