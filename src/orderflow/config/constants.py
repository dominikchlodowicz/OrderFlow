# Azure constants
STORAGE_ACCOUNT_NAME = "storderflowdevfrc1"
LAKEHOUSE_CONTAINER = "lakehouse"

SECRET_SCOPE = "orderflow"
STORAGE_ACCOUNT_KEY_SECRET_NAME = "storderflowdevfrc1-key"

ADLS_SOURCE_SYSTEM = "adls_raw"

ADLS_BASE_PATH = (
    f"abfss://{LAKEHOUSE_CONTAINER}"
    f"@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
)

CALENDAR_BRONZE_INPUT_PATH = f"{ADLS_BASE_PATH}/bronze/landing/calendar"
CALENDAR_BRONZE_OUTPUT_PATH = f"{ADLS_BASE_PATH}/bronze/delta/calendar"

CUSTOMERS_BRONZE_INPUT_PATH = f"{ADLS_BASE_PATH}/bronze/landing/customers"
CUSTOMERS_BRONZE_OUTPUT_PATH = f"{ADLS_BASE_PATH}/bronze/delta/customers"

CALENDAR_SILVER_INPUT_PATH = f"{ADLS_BASE_PATH}/bronze/delta/calendar"
CALENDAR_SILVER_OUTPUT_PATH = f"{ADLS_BASE_PATH}/silver/delta/calendar"

DIM_CALENDAR_INPUT_PATH = CALENDAR_SILVER_OUTPUT_PATH
DIM_CALENDAR_OUTPUT_PATH = (
    f"{ADLS_BASE_PATH}/gold/delta/dim_calendar"
)


