%sql

CREATE TABLE IF NOT EXISTS orderflow_dev.bronze.calendar
USING DELTA
LOCATION 'abfss://lakehouse@storderflowdevfrc1.dfs.core.windows.net/bronze/delta/calendar';

CREATE TABLE IF NOT EXISTS orderflow_dev.bronze.customers
USING DELTA
LOCATION 'abfss://lakehouse@storderflowdevfrc1.dfs.core.windows.net/bronze/delta/customers';

CREATE TABLE IF NOT EXISTS orderflow_dev.silver.calendar
USING DELTA
LOCATION 'abfss://lakehouse@storderflowdevfrc1.dfs.core.windows.net/silver/delta/calendar';

CREATE TABLE IF NOT EXISTS orderflow_dev.silver.customers
USING DELTA
LOCATION 'abfss://lakehouse@storderflowdevfrc1.dfs.core.windows.net/silver/delta/customers';

CREATE TABLE IF NOT EXISTS orderflow_dev.gold.calendar
USING DELTA
LOCATION 'abfss://lakehouse@storderflowdevfrc1.dfs.core.windows.net/gold/delta/dim_calendar';

CREATE TABLE IF NOT EXISTS orderflow_dev.gold.customers
USING DELTA
LOCATION 'abfss://lakehouse@storderflowdevfrc1.dfs.core.windows.net/gold/delta/dim_customers';