from pyspark.sql import SparkSession

from orderflow.spark.hello_orders import run_hello_orders


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("orderflow-local-hello")
        .master("local[2]")
        .getOrCreate()
    )

    print("Hello from local PySpark")
    print(f"Spark version: {spark.version}")

    orders_df, daily_orders_df = run_hello_orders(spark)

    print("Orders:")
    orders_df.show()

    print("Schema:")
    orders_df.printSchema()

    print("Daily totals:")
    daily_orders_df.show()

    spark.stop()


if __name__ == "__main__":
    main()