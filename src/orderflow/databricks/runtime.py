"""Databricks runtime helpers for OrderFlow notebook tasks."""

from collections.abc import Mapping
from typing import Any, Protocol


class VolumeToTableRunner(Protocol):
    """Contract for a landing-volume-to-table transformation."""

    def __call__(
        self,
        *,
        spark: Any,
        input_path: str,
        output_table: str,
    ) -> None: ...


class TableToTableRunner(Protocol):
    """Contract for a single-input table-to-table transformation."""

    def __call__(
        self,
        *,
        spark: Any,
        input_table: str,
        output_table: str,
    ) -> None: ...


class MultiTableToTableRunner(Protocol):
    """Contract for a multi-input table-to-table transformation."""

    def __call__(
        self,
        *,
        spark: Any,
        output_table: str,
        **input_tables: str,
    ) -> None: ...


def run_databricks_volume_to_table_step(
    *,
    step_name: str,
    runner: VolumeToTableRunner,
    spark: Any,
    input_path: str,
    output_table: str,
) -> None:
    """Run a Databricks landing-volume-to-catalog-table pipeline step."""

    print(f"Running {step_name}")
    print(f"Input path:   {input_path}")
    print(f"Output table: {output_table}")

    runner(
        spark=spark,
        input_path=input_path,
        output_table=output_table,
    )

    print(f"{step_name} completed successfully.")


def run_databricks_table_to_table_step(
    *,
    step_name: str,
    runner: TableToTableRunner,
    spark: Any,
    input_table: str,
    output_table: str,
) -> None:
    """Run a Databricks catalog-table-to-catalog-table pipeline step."""

    print(f"Running {step_name}")
    print(f"Input table:  {input_table}")
    print(f"Output table: {output_table}")

    runner(
        spark=spark,
        input_table=input_table,
        output_table=output_table,
    )

    print(f"{step_name} completed successfully.")


def run_databricks_multi_table_to_table_step(
    *,
    step_name: str,
    runner: MultiTableToTableRunner,
    spark: Any,
    input_tables: Mapping[str, str],
    output_table: str,
) -> None:
    """Run a catalog transformation with multiple named input tables."""

    if not input_tables:
        raise ValueError(f"{step_name} requires at least one input table")

    print(f"Running {step_name}")

    for input_name, input_table in input_tables.items():
        print(f"Input {input_name}: {input_table}")

    print(f"Output table: {output_table}")

    runner(
        spark=spark,
        output_table=output_table,
        **dict(input_tables),
    )

    print(f"{step_name} completed successfully.")


def build_adls2_path(
    path_inside_container: str,
    *,
    account_name: str | None = None,
    container_name: str | None = None,
) -> str:
    """
    Build a complete ADLS Gen2 ABFSS path.

    Example:
        build_adls2_path("/silver/delta/calendar")

    Returns:
        abfss://<container>@<account>.dfs.core.windows.net/silver/delta/calendar
    """

    from orderflow.config.constants import (
        LAKEHOUSE_CONTAINER,
        STORAGE_ACCOUNT_NAME,
    )

    resolved_account_name = account_name or STORAGE_ACCOUNT_NAME
    resolved_container_name = container_name or LAKEHOUSE_CONTAINER
    normalized_path = path_inside_container.lstrip("/")

    return (
        f"abfss://{resolved_container_name}"
        f"@{resolved_account_name}.dfs.core.windows.net"
        f"/{normalized_path}"
    )


def verify_catalog_table(
    *,
    spark: Any,
    table_name: str,
    order_by: str | None = None,
    sample_size: int = 20,
) -> int:
    """
    Verify a Unity Catalog table and print a sample.

    Returns:
        Number of rows in the table.
    """

    if sample_size < 1:
        raise ValueError("sample_size must be greater than zero")

    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(f"Expected table does not exist: {table_name}")

    dataframe = spark.table(table_name)
    row_count = dataframe.count()

    print(f"Table: {table_name}")
    print(f"Row count: {row_count}")

    dataframe.printSchema()

    if order_by is not None:
        if order_by not in dataframe.columns:
            raise ValueError(
                f"Cannot order {table_name} by missing column: {order_by}"
            )

        dataframe = dataframe.orderBy(order_by)

    dataframe.show(sample_size, truncate=False)

    return row_count
