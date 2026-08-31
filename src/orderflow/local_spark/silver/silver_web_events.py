from orderflow.local_spark.session import build_local_spark
from orderflow.silver.web_events import run_web_events_silver

spark = build_local_spark("silver-web-events")

run_web_events_silver(
    spark=spark,
    input_path="data/bronze/web_events",
    output_path="data/silver/web_events",
)

spark.stop()
