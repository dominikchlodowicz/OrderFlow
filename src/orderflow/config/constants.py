# Azure constants
STORAGE_ACCOUNT_NAME = "storderflowdevfrc1"
LAKEHOUSE_CONTAINER = "lakehouse"

ADLS_BASE_PATH = (
    f"abfss://{LAKEHOUSE_CONTAINER}"
    f"@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
)

CALENDAR_BRONZE_INPUT_PATH = f"{ADLS_BASE_PATH}/bronze/landing/calendar"
CALENDAR_BRONZE_OUTPUT_PATH = f"{ADLS_BASE_PATH}/bronze/delta/calendar"

ADLS_SOURCE_SYSTEM = "adls_raw"