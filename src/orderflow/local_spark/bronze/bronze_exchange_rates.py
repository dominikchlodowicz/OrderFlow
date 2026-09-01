from orderflow.bronze.exchange_rates import run_exchange_rates_bronze
from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("bronze-exchange-rates")

run_exchange_rates_bronze(
    spark=spark,
    input_path=data_path("raw", "exchange_rates"),
    output_path=data_path("bronze", "exchange_rates"),
)

spark.stop()
