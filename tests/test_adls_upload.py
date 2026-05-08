from pathlib import Path
from unittest.mock import Mock

import pytest

from orderflow.ingestion import adls_upload
from orderflow.ingestion.adls_upload import AdlsUploadConfig


@pytest.fixture
def mocked_blob_service(monkeypatch):
    blob_service_client = Mock()
    container_client = Mock()
    blob_clients = {}

    def get_blob_client(blob_path: str):
        blob_client = Mock()
        blob_clients[blob_path] = blob_client
        return blob_client

    container_client.get_blob_client.side_effect = get_blob_client
    blob_service_client.get_container_client.return_value = container_client

    from_connection_string = Mock(return_value=blob_service_client)
    monkeypatch.setattr(
        adls_upload.BlobServiceClient,
        "from_connection_string",
        from_connection_string,
    )

    return from_connection_string, blob_service_client, container_client, blob_clients


def test_upload_directory_uploads_csv_files_to_expected_blob_paths(
    tmp_path,
    mocked_blob_service,
) -> None:
    local_path = tmp_path / "raw"
    nested_path = local_path / "orders" / "2026"
    nested_path.mkdir(parents=True)

    (local_path / "customers.csv").write_text("customer_id\n1\n")
    (nested_path / "orders.csv").write_text("order_id\n100\n")
    (local_path / "ignore.txt").write_text("not,csv\n")

    from_connection_string, blob_service_client, container_client, blob_clients = (
        mocked_blob_service
    )
    config = AdlsUploadConfig(
        local_path=local_path,
        container_name="landing",
        connection_string="UseDevelopmentStorage=true",
        target_prefix="bronze/landing/",
    )

    uploaded_count = adls_upload.upload_directory(config)

    assert uploaded_count == 2
    from_connection_string.assert_called_once_with("UseDevelopmentStorage=true")
    blob_service_client.get_container_client.assert_called_once_with("landing")

    expected_blob_paths = {
        "bronze/landing/customers.csv",
        "bronze/landing/orders/2026/orders.csv",
    }
    actual_blob_paths = {call.args[0] for call in container_client.get_blob_client.call_args_list}
    assert actual_blob_paths == expected_blob_paths

    for blob_path, blob_client in blob_clients.items():
        blob_client.upload_blob.assert_called_once()
        args, kwargs = blob_client.upload_blob.call_args
        assert len(args) == 1
        assert Path(args[0].name).suffix == ".csv"
        assert blob_path in expected_blob_paths
        assert kwargs["overwrite"] is True
        assert kwargs["content_settings"].content_type == "text/csv"


def test_upload_directory_with_no_csv_files_returns_zero(
    tmp_path,
    mocked_blob_service,
) -> None:
    local_path = tmp_path / "raw"
    local_path.mkdir()
    (local_path / "readme.txt").write_text("skip me")

    _, _, container_client, _ = mocked_blob_service
    config = AdlsUploadConfig(
        local_path=local_path,
        container_name="landing",
        connection_string="UseDevelopmentStorage=true",
    )

    uploaded_count = adls_upload.upload_directory(config)

    assert uploaded_count == 0
    container_client.get_blob_client.assert_not_called()


def test_config_from_env_builds_config_from_environment(tmp_path, monkeypatch) -> None:
    load_dotenv = Mock()
    monkeypatch.setattr(adls_upload, "load_dotenv", load_dotenv)
    monkeypatch.setenv("LOCAL_RAW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER_NAME", "landing")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "connection-string")
    monkeypatch.setenv("ADLS_BRONZE_LANDING_PREFIX", "bronze/custom")

    config = adls_upload.config_from_env()

    load_dotenv.assert_called_once_with()
    assert config == AdlsUploadConfig(
        local_path=tmp_path,
        container_name="landing",
        connection_string="connection-string",
        target_prefix="bronze/custom",
    )


def test_config_from_env_uses_optional_defaults(tmp_path, monkeypatch) -> None:
    load_dotenv = Mock()
    monkeypatch.setattr(adls_upload, "load_dotenv", load_dotenv)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data" / "raw").mkdir(parents=True)
    monkeypatch.delenv("LOCAL_RAW_DATA_DIR", raising=False)
    monkeypatch.delenv("ADLS_BRONZE_LANDING_PREFIX", raising=False)
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER_NAME", "landing")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "connection-string")

    config = adls_upload.config_from_env()

    assert config == AdlsUploadConfig(
        local_path=Path("data/raw"),
        container_name="landing",
        connection_string="connection-string",
        target_prefix="bronze/landing",
    )


def test_config_from_env_raises_when_local_directory_is_missing(tmp_path, monkeypatch) -> None:
    missing_path = tmp_path / "missing"
    monkeypatch.setattr(adls_upload, "load_dotenv", Mock())
    monkeypatch.setenv("LOCAL_RAW_DATA_DIR", str(missing_path))

    with pytest.raises(FileNotFoundError, match=f"Local directory does not exist: {missing_path}"):
        adls_upload.config_from_env()
