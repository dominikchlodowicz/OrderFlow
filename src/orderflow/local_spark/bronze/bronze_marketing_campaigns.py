from orderflow.bronze.marketing_campaigns import run_marketing_campaigns_bronze
from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("bronze-marketing-campaigns")

run_marketing_campaigns_bronze(
    spark=spark,
    input_path=data_path("raw", "marketing_campaigns"),
    output_path=data_path("bronze", "marketing_campaigns"),
)

spark.stop()
