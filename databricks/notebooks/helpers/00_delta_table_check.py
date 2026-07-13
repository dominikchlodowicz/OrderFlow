from typing import Any

dbutils: Any
spark: Any

adls2_path_to_check = "/silver/delta/calendar"

spark.conf.set(
    "fs.azure.account.key.storderflowdevfrc1.dfs.core.windows.net",
    dbutils.secrets.get(
        scope="orderflow",
        key="storderflowdevfrc1-key",
    ),
)

path = (
    "abfss://lakehouse"
    "@storderflowdevfrc1.dfs.core.windows.net" +
    adls2_path_to_check
)

df = spark.read.format("delta").load(path)

print("Row count:", df.count())
df.printSchema()
display(df.orderBy("date_day"))