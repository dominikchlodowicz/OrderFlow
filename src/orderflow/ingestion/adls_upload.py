from dataclasses import dataclass
import os
from pathlib import Path

from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv


@dataclass(frozen=True)
class AdlsUploadConfig:
    local_path: Path
    container_name: str
    connection_string: str
    target_prefix: str = "bronze/landing"


def upload_directory(config: AdlsUploadConfig) -> int:
    blob_service_client = BlobServiceClient.from_connection_string(config.connection_string)
    container_client = blob_service_client.get_container_client(config.container_name)

    uploaded_count = 0

    for file_path in config.local_path.rglob("*.csv"):
        relative_path = file_path.relative_to(config.local_path).as_posix()
        blob_path = f"{config.target_prefix.rstrip('/')}/{relative_path}"
        blob_client = container_client.get_blob_client(blob_path)

        with file_path.open("rb") as file_data:
            blob_client.upload_blob(
                file_data,
                overwrite=True,
                content_settings=ContentSettings(content_type="text/csv"),
            )

        uploaded_count += 1
        print(f"Uploaded: {file_path} -> {config.container_name}/{blob_path}")

    print(f"Done. Uploaded {uploaded_count} files.")
    return uploaded_count


def config_from_env() -> AdlsUploadConfig:
    load_dotenv()

    local_path = Path(os.environ.get("LOCAL_RAW_DATA_DIR", "data/raw"))

    if not local_path.exists():
        raise FileNotFoundError(f"Local directory does not exist: {local_path}")

    return AdlsUploadConfig(
        local_path=local_path,
        container_name=os.environ["AZURE_STORAGE_CONTAINER_NAME"],
        connection_string=os.environ["AZURE_STORAGE_CONNECTION_STRING"],
        target_prefix=os.environ.get("ADLS_BRONZE_LANDING_PREFIX", "bronze/landing"),
    )


def main() -> None:
    upload_directory(config_from_env())
