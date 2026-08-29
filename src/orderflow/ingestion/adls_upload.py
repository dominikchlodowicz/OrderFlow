import argparse
from dataclasses import dataclass
import os
from pathlib import Path

from azure.storage.blob import BlobServiceClient, ContentSettings
from dotenv import load_dotenv


DATASET_NAMES = (
    "calendar",
    "customers",
    "exchange_rates",
    "marketing_campaigns",
    "order_items",
    "orders",
    "payments",
    "products",
    "refunds",
    "shipments",
    "web_events",
)


@dataclass(frozen=True)
class AdlsUploadConfig:
    local_path: Path
    container_name: str
    connection_string: str
    target_prefix: str = "landing"


def upload_directory(
    config: AdlsUploadConfig,
    dataset_name: str | None = None,
) -> int:
    blob_service_client = BlobServiceClient.from_connection_string(
        config.connection_string
    )
    container_client = blob_service_client.get_container_client(
        config.container_name
    )

    upload_root = (
        config.local_path / dataset_name
        if dataset_name
        else config.local_path
    )

    if not upload_root.is_dir():
        raise FileNotFoundError(
            f"Upload directory does not exist: {upload_root}"
        )

    uploaded_count = 0

    for file_path in upload_root.rglob("*.csv"):
        relative_path = file_path.relative_to(
            config.local_path
        ).as_posix()

        blob_path = (
            f"{config.target_prefix.strip('/')}/{relative_path}"
        )

        blob_client = container_client.get_blob_client(blob_path)

        with file_path.open("rb") as file_data:
            blob_client.upload_blob(
                file_data,
                overwrite=True,
                content_settings=ContentSettings(
                    content_type="text/csv"
                ),
            )

        uploaded_count += 1
        print(
            f"Uploaded: {file_path} "
            f"-> {config.container_name}/{blob_path}"
        )

    print(f"Done. Uploaded {uploaded_count} files.")
    return uploaded_count


def config_from_env() -> AdlsUploadConfig:
    load_dotenv()

    local_path = Path(
        os.environ.get("LOCAL_RAW_DATA_DIR", "data/raw")
    )

    if not local_path.is_dir():
        raise FileNotFoundError(
            f"Local directory does not exist: {local_path}"
        )

    return AdlsUploadConfig(
        local_path=local_path,
        container_name=os.environ[
            "AZURE_STORAGE_CONTAINER_NAME"
        ],
        connection_string=os.environ[
            "AZURE_STORAGE_CONNECTION_STRING"
        ],
        target_prefix=os.environ.get(
            "ADLS_LANDING_PREFIX",
            "landing",
        ),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload generated CSV datasets to ADLS landing."
    )
    parser.add_argument(
        "--dataset",
        choices=DATASET_NAMES,
        help="Upload only this dataset. Omit to upload all datasets.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    upload_directory(
        config=config_from_env(),
        dataset_name=args.dataset,
    )


if __name__ == "__main__":
    main()