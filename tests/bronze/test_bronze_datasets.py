import csv
import hashlib
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pytest
from delta.tables import DeltaTable
from helpers.constants import STANDARD_BRONZE_METADATA_COLUMNS
from pyspark.sql import types as T

from orderflow.bronze.dataset import BronzeDataset
from orderflow.bronze.exchange_rates import (
    EXCHANGE_RATES_DATASET,
    run_exchange_rates_bronze,
)
from orderflow.bronze.marketing_campaigns import (
    MARKETING_CAMPAIGNS_DATASET,
    run_marketing_campaigns_bronze,
)
from orderflow.bronze.order_items import ORDER_ITEMS_DATASET, run_order_items_bronze
from orderflow.bronze.orders import ORDERS_DATASET, run_orders_bronze
from orderflow.bronze.payments import PAYMENTS_DATASET, run_payments_bronze
from orderflow.bronze.products import PRODUCTS_DATASET, run_products_bronze
from orderflow.bronze.refunds import REFUNDS_DATASET, run_refunds_bronze
from orderflow.bronze.shipments import SHIPMENTS_DATASET, run_shipments_bronze
from orderflow.bronze.web_events import WEB_EVENTS_DATASET, run_web_events_bronze

LOAD_DATE = "2026-06-15"


@dataclass(frozen=True)
class BronzeDatasetCase:
    source_entity: str
    dataset: BronzeDataset
    run_bronze: Callable[..., None]
    columns: tuple[str, ...]
    values: tuple[str, ...]

    @property
    def raw_row(self) -> dict[str, str]:
        return dict(zip(self.columns, self.values, strict=True))


BRONZE_DATASET_CASES = [
    BronzeDatasetCase(
        source_entity="marketing_campaigns",
        dataset=MARKETING_CAMPAIGNS_DATASET,
        run_bronze=run_marketing_campaigns_bronze,
        columns=(
            "campaign_id",
            "campaign_name",
            "source_channel",
            "start_date",
            "end_date",
            "budget_amount",
            "currency",
            "created_at",
            "updated_at",
            "is_active",
            "load_date",
            "loaded_at",
            "source_event_at",
        ),
        values=(
            "cmp_summer",
            "Summer Sale",
            "email",
            "2026-06-01",
            "2026-06-30",
            "5000.00",
            "PLN",
            "2026-05-20 09:00:00",
            "2026-05-21 10:00:00",
            "True",
            LOAD_DATE,
            "2026-06-15 01:00:00",
            "2026-05-21 10:00:00",
        ),
    ),
    BronzeDatasetCase(
        source_entity="order_items",
        dataset=ORDER_ITEMS_DATASET,
        run_bronze=run_order_items_bronze,
        columns=(
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "discount_amount",
            "line_total",
            "created_at",
            "load_date",
            "loaded_at",
            "source_event_at",
        ),
        values=(
            "item_001",
            "ord_001",
            "prod_001",
            "2",
            "120.00",
            "20.00",
            "220.00",
            "2026-06-15 10:00:00",
            LOAD_DATE,
            "2026-06-15 11:00:00",
            "2026-06-15 10:00:00",
        ),
    ),
    BronzeDatasetCase(
        source_entity="orders",
        dataset=ORDERS_DATASET,
        run_bronze=run_orders_bronze,
        columns=(
            "order_id",
            "customer_id",
            "order_status",
            "order_created_at",
            "order_updated_at",
            "country_code",
            "currency",
            "gross_amount",
            "discount_amount",
            "net_amount",
            "source_channel",
            "campaign_id",
            "load_date",
            "loaded_at",
            "source_event_at",
        ),
        values=(
            "ord_001",
            "cust_001",
            "paid",
            "2026-06-15 10:00:00",
            "2026-06-15 10:05:00",
            "PL",
            "PLN",
            "240.00",
            "20.00",
            "220.00",
            "email",
            "cmp_summer",
            LOAD_DATE,
            "2026-06-15 11:00:00",
            "2026-06-15 10:05:00",
        ),
    ),
    BronzeDatasetCase(
        source_entity="payments",
        dataset=PAYMENTS_DATASET,
        run_bronze=run_payments_bronze,
        columns=(
            "payment_id",
            "order_id",
            "payment_attempt_number",
            "payment_method",
            "payment_status",
            "amount",
            "currency",
            "created_at",
            "processed_at",
            "failure_reason",
            "load_date",
            "loaded_at",
            "source_event_at",
        ),
        values=(
            "pay_001",
            "ord_001",
            "1",
            "card",
            "failed",
            "220.00",
            "PLN",
            "2026-06-15 10:01:00",
            "2026-06-15 10:02:00",
            "insufficient_funds",
            LOAD_DATE,
            "2026-06-15 11:00:00",
            "2026-06-15 10:02:00",
        ),
    ),
    BronzeDatasetCase(
        source_entity="products",
        dataset=PRODUCTS_DATASET,
        run_bronze=run_products_bronze,
        columns=(
            "product_id",
            "sku",
            "product_name",
            "category",
            "brand",
            "unit_price",
            "currency",
            "is_active",
            "created_at",
            "updated_at",
            "load_date",
            "loaded_at",
            "source_event_at",
        ),
        values=(
            "prod_001",
            "SKU-00001",
            "VistulaTech Phone",
            "electronics",
            "VistulaTech",
            "120.00",
            "PLN",
            "True",
            "2025-01-10 09:00:00",
            "2026-06-15 08:00:00",
            LOAD_DATE,
            "2026-06-15 09:00:00",
            "2026-06-15 08:00:00",
        ),
    ),
    BronzeDatasetCase(
        source_entity="refunds",
        dataset=REFUNDS_DATASET,
        run_bronze=run_refunds_bronze,
        columns=(
            "refund_id",
            "order_id",
            "payment_id",
            "refund_reason",
            "refund_amount",
            "currency",
            "created_at",
            "processed_at",
            "refund_status",
            "load_date",
            "loaded_at",
            "source_event_at",
        ),
        values=(
            "ref_001",
            "ord_001",
            "pay_001",
            "customer_request",
            "50.00",
            "PLN",
            "2026-06-15 12:00:00",
            "2026-06-15 12:05:00",
            "processed",
            LOAD_DATE,
            "2026-06-15 13:00:00",
            "2026-06-15 12:05:00",
        ),
    ),
    BronzeDatasetCase(
        source_entity="shipments",
        dataset=SHIPMENTS_DATASET,
        run_bronze=run_shipments_bronze,
        columns=(
            "shipment_id",
            "order_id",
            "carrier",
            "shipment_status",
            "shipped_at",
            "estimated_delivery_at",
            "delivered_at",
            "delivery_country",
            "delivery_city",
            "shipping_cost",
            "load_date",
            "loaded_at",
            "source_event_at",
        ),
        values=(
            "ship_001",
            "ord_001",
            "InPost",
            "delivered",
            "2026-06-15 08:00:00",
            "2026-06-17 18:00:00",
            "2026-06-17 12:00:00",
            "PL",
            "Warszawa",
            "15.00",
            LOAD_DATE,
            "2026-06-15 09:00:00",
            "2026-06-17 12:00:00",
        ),
    ),
    BronzeDatasetCase(
        source_entity="web_events",
        dataset=WEB_EVENTS_DATASET,
        run_bronze=run_web_events_bronze,
        columns=(
            "event_id",
            "session_id",
            "customer_id",
            "anonymous_id",
            "event_type",
            "event_timestamp",
            "product_id",
            "campaign_id",
            "device_type",
            "country_code",
            "page_url",
            "load_date",
            "loaded_at",
            "source_event_at",
        ),
        values=(
            "evt_001",
            "sess_001",
            "cust_001",
            "anon_001",
            "product_view",
            "2026-06-15 09:30:00",
            "prod_001",
            "cmp_summer",
            "mobile",
            "PL",
            "https://example.com/products/prod_001",
            LOAD_DATE,
            "2026-06-15 10:00:00",
            "2026-06-15 09:30:00",
        ),
    ),
    BronzeDatasetCase(
        source_entity="exchange_rates",
        dataset=EXCHANGE_RATES_DATASET,
        run_bronze=run_exchange_rates_bronze,
        columns=(
            "rate_date",
            "currency",
            "rate_to_pln",
            "source",
            "load_date",
            "loaded_at",
        ),
        values=(
            LOAD_DATE,
            "EUR",
            "4.3210",
            "synthetic_nbp_like",
            LOAD_DATE,
            "2026-06-15 02:00:00",
        ),
    ),
]


def _write_csv_rows(
    csv_path: Path,
    case: BronzeDatasetCase,
    rows: Iterable[dict[str, str | None]],
) -> None:
    with csv_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=case.columns)
        writer.writeheader()
        writer.writerows(rows)


def _write_csv(csv_path: Path, case: BronzeDatasetCase) -> None:
    _write_csv_rows(csv_path, case, [case.raw_row])


def _expected_raw_record_hash(
    case: BronzeDatasetCase,
    raw_row: dict[str, str | None] | None = None,
) -> str:
    raw_row = raw_row or case.raw_row
    payload = "||".join(
        "<NULL>" if raw_row[column] is None else raw_row[column] for column in case.columns
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _case_for(source_entity: str) -> BronzeDatasetCase:
    return next(case for case in BRONZE_DATASET_CASES if case.source_entity == source_entity)


@pytest.mark.parametrize(
    "case",
    BRONZE_DATASET_CASES,
    ids=lambda case: case.source_entity,
)
def test_bronze_dataset_schema_matches_table_contract(case: BronzeDatasetCase) -> None:
    assert case.dataset.source_entity == case.source_entity
    assert case.dataset.columns == case.columns
    assert [field.name for field in case.dataset.schema] == list(case.columns)
    assert all(isinstance(field.dataType, T.StringType) for field in case.dataset.schema)
    assert all(field.nullable for field in case.dataset.schema)


@pytest.mark.parametrize(
    "case",
    BRONZE_DATASET_CASES,
    ids=lambda case: case.source_entity,
)
def test_run_bronze_dataset_writes_contract_row(
    case: BronzeDatasetCase,
    spark,
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "landing" / case.source_entity / f"load_date={LOAD_DATE}"
    output_path = tmp_path / "bronze" / case.source_entity
    input_path.mkdir(parents=True)

    csv_path = input_path / f"{case.source_entity}.csv"
    _write_csv(csv_path, case)

    case.run_bronze(
        spark=spark,
        input_path=input_path.parent,
        output_path=output_path,
    )

    result_df = spark.read.format("delta").load(str(output_path))

    assert result_df.count() == 1
    assert result_df.columns == list(case.columns) + STANDARD_BRONZE_METADATA_COLUMNS
    assert all(
        isinstance(result_df.schema[column].dataType, T.StringType) for column in case.columns
    )
    assert isinstance(result_df.schema["_source_load_date"].dataType, T.DateType)
    assert isinstance(result_df.schema["_ingested_at"].dataType, T.TimestampType)

    row = result_df.first().asDict()

    for column_name, expected_value in case.raw_row.items():
        assert row[column_name] == expected_value

    assert row["_source_system"] == "local_files"
    assert row["_source_entity"] == case.source_entity
    assert row["_source_file_name"] == f"{case.source_entity}.csv"
    assert row["_source_load_date"] == date.fromisoformat(LOAD_DATE)
    assert row["_source_file_path"].endswith(f"load_date={LOAD_DATE}/{case.source_entity}.csv")
    assert row["_ingestion_run_id"]
    assert row["_ingested_at"] is not None
    assert row["_raw_record_hash"] == _expected_raw_record_hash(case)


def test_bronze_dataset_replay_replaces_only_target_load_date(spark, tmp_path: Path) -> None:
    case = _case_for("orders")
    first_load_date = "2026-06-15"
    second_load_date = "2026-06-16"
    base_input_path = tmp_path / "landing" / case.source_entity
    first_day = base_input_path / f"load_date={first_load_date}"
    second_day = base_input_path / f"load_date={second_load_date}"
    output_path = tmp_path / "bronze" / case.source_entity

    first_day.mkdir(parents=True)
    second_day.mkdir(parents=True)

    first_row = {
        **case.raw_row,
        "order_id": "ord_first",
        "order_status": "paid",
        "load_date": first_load_date,
    }
    second_row = {
        **case.raw_row,
        "order_id": "ord_second",
        "order_status": "pending",
        "load_date": second_load_date,
    }

    _write_csv_rows(first_day / "orders.csv", case, [first_row])
    _write_csv_rows(second_day / "orders.csv", case, [second_row])

    case.run_bronze(spark=spark, input_path=base_input_path, output_path=output_path)

    replayed_second_row = {**second_row, "order_status": "cancelled"}
    _write_csv_rows(second_day / "orders.csv", case, [replayed_second_row])

    case.run_bronze(spark=spark, input_path=second_day, output_path=output_path)

    result = [
        row.asDict()
        for row in spark.read.format("delta")
        .load(str(output_path))
        .select("order_id", "order_status", "_source_load_date")
        .orderBy("_source_load_date")
        .collect()
    ]

    assert result == [
        {
            "order_id": "ord_first",
            "order_status": "paid",
            "_source_load_date": date.fromisoformat(first_load_date),
        },
        {
            "order_id": "ord_second",
            "order_status": "cancelled",
            "_source_load_date": date.fromisoformat(second_load_date),
        },
    ]
    assert DeltaTable.forPath(spark, str(output_path)).detail().first()["partitionColumns"] == [
        "_source_load_date"
    ]


def test_bronze_dataset_rejects_empty_batch(spark, tmp_path: Path) -> None:
    case = _case_for("orders")
    input_path = tmp_path / "landing" / case.source_entity / f"load_date={LOAD_DATE}"
    input_path.mkdir(parents=True)
    _write_csv_rows(input_path / "orders.csv", case, [])

    with pytest.raises(ValueError, match="Bronze batch for 'orders' is empty"):
        case.dataset.build_bronze(spark=spark, input_path=input_path)


def test_bronze_dataset_rejects_path_without_load_date(spark, tmp_path: Path) -> None:
    case = _case_for("orders")
    input_path = tmp_path / "landing" / case.source_entity / "incoming"
    input_path.mkdir(parents=True)
    _write_csv(input_path / "orders.csv", case)

    with pytest.raises(ValueError, match="Expected folder pattern: load_date=YYYY-MM-DD"):
        case.dataset.build_bronze(spark=spark, input_path=input_path)


def test_bronze_dataset_preserves_nullable_raw_values_and_batch_metadata(
    spark,
    tmp_path: Path,
) -> None:
    case = _case_for("payments")
    input_path = tmp_path / "landing" / case.source_entity / f"load_date={LOAD_DATE}"
    input_path.mkdir(parents=True)

    null_failure_row = {
        **case.raw_row,
        "payment_id": "pay_null",
        "failure_reason": None,
    }
    special_failure_row = {
        **case.raw_row,
        "payment_id": "pay_special",
        "failure_reason": "bank, timeout – Żółć",
    }
    _write_csv_rows(
        input_path / "payments.csv",
        case,
        [null_failure_row, special_failure_row],
    )

    result = {
        row["payment_id"]: row.asDict()
        for row in case.dataset.build_bronze(
            spark=spark,
            input_path=input_path,
            source_system="test_source",
            ingestion_run_id="test_batch",
        ).collect()
    }

    assert result["pay_null"]["failure_reason"] is None
    assert result["pay_special"]["failure_reason"] == "bank, timeout – Żółć"
    assert {row["_source_system"] for row in result.values()} == {"test_source"}
    assert {row["_ingestion_run_id"] for row in result.values()} == {"test_batch"}
    assert len({row["_ingested_at"] for row in result.values()}) == 1
    assert result["pay_null"]["_raw_record_hash"] == _expected_raw_record_hash(
        case,
        null_failure_row,
    )
    assert result["pay_special"]["_raw_record_hash"] == _expected_raw_record_hash(
        case,
        special_failure_row,
    )
