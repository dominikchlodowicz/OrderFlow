from orderflow.bronze.calendar import run_calendar_bronze
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("bronze-calendar")

run_calendar_bronze(
    spark=spark,
    input_path="data/raw/calendar",
    output_path="data/bronze/calendar",
)

spark.stop()