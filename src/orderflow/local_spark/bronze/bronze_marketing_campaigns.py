from orderflow.bronze.marketing_campaigns import run_marketing_campaigns_bronze
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("bronze-marketing-campaigns")

run_marketing_campaigns_bronze(
    spark=spark,
    input_path="data/raw/marketing_campaigns",
    output_path="data/bronze/marketing_campaigns",
)

spark.stop()
