from orderflow.local_spark.session import build_local_spark
from orderflow.silver.marketing_campaigns import run_marketing_campaigns_silver

spark = build_local_spark("silver-marketing-campaigns")

run_marketing_campaigns_silver(
    spark=spark,
    input_path="data/bronze/marketing_campaigns",
    output_path="data/silver/marketing_campaigns",
)

spark.stop()
