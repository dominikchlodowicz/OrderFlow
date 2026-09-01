from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark
from orderflow.silver.products import run_products_silver

spark = build_local_spark("silver-products")

run_products_silver(
    spark=spark,
    input_path=data_path("bronze", "products"),
    output_path=data_path("silver", "products"),
)

spark.stop()
