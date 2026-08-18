from orderflow.bronze.web_events import run_web_events_bronze
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("bronze-web-events")

run_web_events_bronze(
    spark=spark,
    input_path="data/raw/web_events",
    output_path="data/bronze/web_events",
)

spark.stop()
