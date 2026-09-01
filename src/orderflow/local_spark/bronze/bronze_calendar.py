from orderflow.bronze.calendar import run_calendar_bronze
from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("bronze-calendar")

run_calendar_bronze(
    spark=spark,
    input_path=data_path("raw", "calendar"),
    output_path=data_path("bronze", "calendar"),
)

spark.stop()
