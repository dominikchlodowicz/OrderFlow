from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark
from orderflow.silver.calendar import run_calendar_silver

spark = build_local_spark("silver-calendar")

run_calendar_silver(
    spark=spark,
    input_path=data_path("bronze", "calendar"),
    output_path=data_path("silver", "calendar"),
)

spark.stop()
