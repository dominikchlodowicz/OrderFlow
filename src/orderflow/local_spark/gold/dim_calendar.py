from orderflow.gold.dim_calendar import run_dim_calendar
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("gold-calendar")

run_dim_calendar(
    spark=spark,
    input_path="data/silver/calendar",
    output_path="data/gold/dim_calendar",
)

spark.stop()