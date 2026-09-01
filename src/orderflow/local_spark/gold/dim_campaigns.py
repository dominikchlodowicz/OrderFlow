from orderflow.gold.dim_campaigns import run_dim_campaigns
from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("gold-dim-campaigns")

run_dim_campaigns(
    spark=spark,
    input_path=data_path("silver", "marketing_campaigns"),
    output_path=data_path("gold", "dim_campaigns"),
)

spark.stop()
