from datetime import date, datetime

from pyspark.sql import Row

SILVER_LINEAGE_COLUMNS = [
    "_source_file_name",
    "_source_file_path",
    "_ingestion_run_id",
    "_bronze_ingested_at",
    "_raw_record_hash",
    "_silver_processed_at",
]


def with_bronze_lineage(
    values: dict[str, object],
    *,
    source_entity: str,
    load_date: str = "2026-06-15",
    raw_record_hash: str = "raw-hash",
    ingested_at: datetime = datetime(2026, 6, 15, 12, 0, 0),
) -> dict[str, object]:
    return {
        **values,
        "_source_system": "local_files",
        "_source_entity": source_entity,
        "_source_file_name": f"{source_entity}.csv",
        "_source_load_date": date.fromisoformat(load_date),
        "_source_file_path": (
            f"/Volumes/orderflow_dev/bronze/landing/{source_entity}/"
            f"load_date={load_date}/{source_entity}.csv"
        ),
        "_ingestion_run_id": "run-001",
        "_ingested_at": ingested_at,
        "_raw_record_hash": raw_record_hash,
    }


def assert_silver_lineage(row: Row, *, raw_record_hash: str = "raw-hash") -> None:
    assert row["_source_file_name"]
    assert row["_source_file_path"]
    assert row["_ingestion_run_id"] == "run-001"
    assert row["_bronze_ingested_at"] == datetime(2026, 6, 15, 12, 0, 0)
    assert row["_raw_record_hash"] == raw_record_hash
    assert row["_silver_processed_at"] is not None
