from orderflow.bronze.exchange_rates import run_exchange_rates_bronze
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("bronze-exchange-rates")

run_exchange_rates_bronze(
    spark=spark,
    input_path="data/raw/exchange_rates",
    output_path="data/bronze/exchange_rates",
)

spark.stop()
