# Azure constants
STORAGE_ACCOUNT_NAME = "storderflowdevfrc1"
LAKEHOUSE_CONTAINER = "lakehouse"

SECRET_SCOPE = "orderflow"
STORAGE_ACCOUNT_KEY_SECRET_NAME = "storderflowdevfrc1-key"

ADLS_SOURCE_SYSTEM = "adls_raw"

ADLS_BASE_PATH = f"abfss://{LAKEHOUSE_CONTAINER}" f"@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"

# Unity Catalog objects created by the Databricks setup notebooks.
ORDERFLOW_CATALOG = "orderflow_dev"
LANDING_VOLUME_PATH = f"/Volumes/{ORDERFLOW_CATALOG}/bronze/landing"

CALENDAR_BRONZE_INPUT_PATH = f"{LANDING_VOLUME_PATH}/calendar"
CUSTOMERS_BRONZE_INPUT_PATH = f"{LANDING_VOLUME_PATH}/customers"

CALENDAR_BRONZE_TABLE = f"{ORDERFLOW_CATALOG}.bronze.calendar"
CUSTOMERS_BRONZE_TABLE = f"{ORDERFLOW_CATALOG}.bronze.customers"

CALENDAR_SILVER_TABLE = f"{ORDERFLOW_CATALOG}.silver.calendar"
CUSTOMERS_SILVER_TABLE = f"{ORDERFLOW_CATALOG}.silver.customers"

CALENDAR_GOLD_TABLE = f"{ORDERFLOW_CATALOG}.gold.dim_calendar"
CUSTOMERS_GOLD_TABLE = f"{ORDERFLOW_CATALOG}.gold.dim_customers"
