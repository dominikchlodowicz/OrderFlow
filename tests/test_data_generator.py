from pathlib import Path

import pandas as pd
import pytest

from orderflow.generator.data_generator import GeneratorConfig, OrderFlowGenerator

ENTITY_TABLES = [
    "customers",
    "products",
    "marketing_campaigns",
    "calendar",
    "exchange_rates",
    "orders",
    "order_items",
    "payments",
    "shipments",
    "refunds",
    "web_events",
]

LOAD_DATES = ["2026-01-02", "2026-01-03"]

REQUIRED_COLUMNS_BY_TABLE = {
    "customers": {
        "customer_id",
        "email",
        "country_code",
        "customer_status",
        "load_date",
        "loaded_at",
    },
    "products": {
        "product_id",
        "sku",
        "category",
        "unit_price",
        "currency",
        "load_date",
        "loaded_at",
    },
    "orders": {
        "order_id",
        "customer_id",
        "order_status",
        "order_created_at",
        "country_code",
        "currency",
        "net_amount",
        "load_date",
        "loaded_at",
    },
    "order_items": {
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "line_total",
        "load_date",
        "loaded_at",
    },
    "payments": {
        "payment_id",
        "order_id",
        "payment_attempt_number",
        "payment_status",
        "amount",
        "currency",
        "load_date",
        "loaded_at",
    },
}


def small_config(output_dir: Path, seed: int = 123) -> GeneratorConfig:
    return GeneratorConfig(
        start_date=LOAD_DATES[0],
        end_date=LOAD_DATES[-1],
        output_dir=str(output_dir),
        seed=seed,
        initial_customers=20,
        initial_products=12,
        avg_orders_per_day=50,
        avg_web_sessions_per_day=15,
        payment_failure_rate=0.0,
        refund_rate=0.0,
        shipment_lost_rate=0.0,
        shipment_returned_rate=0.0,
        late_arrival_rate=0.0,
        invalid_record_rate=0.0,
        duplicate_rate=0.0,
        include_empty_files=True,
        clean_output=True,
    )


@pytest.fixture(scope="module")
def generated_output(tmp_path_factory) -> Path:
    output_dir = tmp_path_factory.mktemp("data_generator") / "raw"
    OrderFlowGenerator(small_config(output_dir)).generate()
    return output_dir


def read_partition(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def read_table(output_dir: Path, table_name: str) -> pd.DataFrame:
    frames = [
        read_partition(path)
        for path in sorted((output_dir / table_name).glob(f"load_date=*/{table_name}.csv"))
    ]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def output_fingerprint(output_dir: Path) -> dict[str, bytes]:
    return {
        path.relative_to(output_dir).as_posix(): path.read_bytes()
        for path in sorted(output_dir.rglob("*.csv"))
    }


def test_generator_runs_without_crashing_for_small_date_range(tmp_path) -> None:
    output_dir = tmp_path / "raw"

    OrderFlowGenerator(small_config(output_dir)).generate()

    assert output_dir.exists()


@pytest.mark.parametrize("table_name", ENTITY_TABLES)
def test_expected_entity_partition_files_are_created(
    generated_output,
    table_name: str,
) -> None:
    for load_date in LOAD_DATES:
        file_path = generated_output / table_name / f"load_date={load_date}" / f"{table_name}.csv"
        assert file_path.is_file()


@pytest.mark.parametrize("table_name,required_columns", REQUIRED_COLUMNS_BY_TABLE.items())
def test_required_columns_exist_in_key_files(
    generated_output,
    table_name: str,
    required_columns: set[str],
) -> None:
    table = read_table(generated_output, table_name)

    assert not table.empty
    assert required_columns <= set(table.columns)


def test_order_items_reference_existing_orders(generated_output) -> None:
    orders = read_table(generated_output, "orders")
    order_items = read_table(generated_output, "order_items")

    assert not orders.empty
    assert not order_items.empty
    assert set(order_items["order_id"]) <= set(orders["order_id"])


@pytest.mark.xfail(
    strict=True,
    reason="Generator currently uses uuid4(), so seed alone does not make IDs reproducible.",
)
def test_output_is_reproducible_with_same_seed(tmp_path) -> None:
    first_output = tmp_path / "first"
    second_output = tmp_path / "second"

    OrderFlowGenerator(small_config(first_output, seed=987)).generate()
    OrderFlowGenerator(small_config(second_output, seed=987)).generate()

    assert output_fingerprint(first_output) == output_fingerprint(second_output)
