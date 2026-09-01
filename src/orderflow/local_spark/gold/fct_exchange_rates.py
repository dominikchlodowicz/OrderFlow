from orderflow.gold.fct_exchange_rates import run_fct_exchange_rates
from orderflow.local_spark.paths import data_path
from orderflow.local_spark.session import build_local_spark

spark = build_local_spark("gold-fct-exchange-rates")

run_fct_exchange_rates(
    spark=spark,
    exchange_rates_input_path=data_path("silver", "exchange_rates"),
    currency_input_path=data_path("gold", "dim_currency"),
    calendar_input_path=data_path("gold", "dim_calendar"),
    output_path=data_path("gold", "fct_exchange_rates"),
)

spark.stop()
