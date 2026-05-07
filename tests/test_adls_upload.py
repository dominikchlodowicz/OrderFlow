import pytest

from orderflow.ingestion.adls_upload import upload_directory


def test_adls_upload_placeholder_is_explicit() -> None:
    with pytest.raises(NotImplementedError):
        upload_directory("data/raw", "bronze/raw")
