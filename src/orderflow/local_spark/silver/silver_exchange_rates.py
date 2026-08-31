from orderflow.local_spark.session import build_local_spark
from orderflow.silver.exchange_rates import run_exchange_rates_silver

spark = build_local_spark("silver-exchange-rates")

run_exchange_rates_silver(
    spark=spark,
    input_path="data/bronze/exchange_rates",
    output_path="data/silver/exchange_rates",
)

spark.stop()
