"""Small, aggregation-only PySpark data-quality framework."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from functools import reduce
from operator import and_, or_
from typing import Literal

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from orderflow.common.validation import find_missing_required_columns

CheckStatus = Literal["PASS", "FAIL"]


@dataclass(frozen=True)
class CheckResult:
    """Structured result of one data-quality assertion."""

    table_name: str
    check_name: str
    status: CheckStatus
    violation_count: int
    details: str
    critical: bool = True

    @property
    def passed(self) -> bool:
        return self.status == "PASS"


@dataclass(frozen=True)
class DataQualityReport:
    """Complete set of results produced by a quality suite."""

    results: tuple[CheckResult, ...]

    @property
    def critical_failures(self) -> tuple[CheckResult, ...]:
        return tuple(result for result in self.results if not result.passed and result.critical)

    @property
    def warnings(self) -> tuple[CheckResult, ...]:
        return tuple(result for result in self.results if not result.passed and not result.critical)

    @property
    def passed(self) -> bool:
        return not self.critical_failures

    def format_summary(self) -> str:
        """Render every result without collecting source rows."""
        passed_count = sum(result.passed for result in self.results)
        lines = [
            "OrderFlow data-quality report",
            (
                f"Checks: {len(self.results)} | Passed: {passed_count} | "
                f"Critical failures: {len(self.critical_failures)} | "
                f"Warnings: {len(self.warnings)}"
            ),
            "STATUS | SEVERITY | TABLE | CHECK | VIOLATIONS | DETAILS",
        ]
        for result in self.results:
            severity = "CRITICAL" if result.critical else "WARNING"
            lines.append(
                " | ".join(
                    [
                        result.status,
                        severity,
                        result.table_name,
                        result.check_name,
                        str(result.violation_count),
                        result.details,
                    ]
                )
            )
        return "\n".join(lines)

    def raise_for_failures(self) -> None:
        """Fail the caller after the complete report has been produced."""
        if self.critical_failures:
            raise DataQualityFailure(self)


class DataQualityFailure(RuntimeError):
    """Raised when a completed report contains critical failures."""

    def __init__(self, report: DataQualityReport) -> None:
        self.report = report
        super().__init__(
            f"Data quality failed with {len(report.critical_failures)} critical check failure(s)."
        )


@dataclass(frozen=True)
class _SchemaCheck:
    table_name: str
    check_name: str
    required_columns: tuple[str, ...]
    expected_types: Mapping[str, str] | None
    critical: bool


@dataclass(frozen=True)
class _AggregateCheck:
    table_name: str
    check_name: str
    required_columns: tuple[str, ...]
    expression: Column
    expectation: str
    critical: bool


@dataclass(frozen=True)
class _ReferenceCheck:
    table_name: str
    check_name: str
    source_columns: tuple[str, ...]
    reference_table: str
    reference_columns: tuple[str, ...]
    ignore_nulls: bool
    critical: bool


_RegisteredCheck = _SchemaCheck | _AggregateCheck | _ReferenceCheck


class DataQualitySuite:
    """Register checks and execute only small aggregated Spark actions."""

    def __init__(self, tables: Mapping[str, DataFrame] | None = None) -> None:
        self._tables = dict(tables or {})
        self._schema_checks: list[_SchemaCheck] = []
        self._aggregate_checks: list[_AggregateCheck] = []
        self._reference_checks: list[_ReferenceCheck] = []
        self._recorded_results: list[CheckResult] = []
        self._registered_names: set[tuple[str, str]] = set()

    def add_table(self, table_name: str, df: DataFrame) -> None:
        if table_name in self._tables:
            raise ValueError(f"Data-quality table is already registered: {table_name}")
        self._tables[table_name] = df

    def record_result(self, result: CheckResult) -> None:
        self._register_name(result.table_name, result.check_name)
        self._recorded_results.append(result)

    def check_required_columns(
        self,
        table_name: str,
        required_columns: Sequence[str],
        *,
        check_name: str = "required_columns",
        critical: bool = True,
    ) -> None:
        self._require_table(table_name)
        self._register_name(table_name, check_name)
        self._schema_checks.append(
            _SchemaCheck(
                table_name,
                check_name,
                tuple(required_columns),
                None,
                critical,
            )
        )

    def check_schema(
        self,
        table_name: str,
        expected_types: Mapping[str, str],
        *,
        check_name: str = "schema_types",
        critical: bool = True,
    ) -> None:
        self._require_table(table_name)
        self._register_name(table_name, check_name)
        self._schema_checks.append(
            _SchemaCheck(
                table_name,
                check_name,
                tuple(expected_types),
                dict(expected_types),
                critical,
            )
        )

    def check_non_empty(
        self,
        table_name: str,
        *,
        check_name: str = "non_empty",
        critical: bool = True,
    ) -> None:
        expression = F.when(F.count(F.lit(1)) == 0, F.lit(1)).otherwise(F.lit(0))
        self._add_aggregate(
            table_name,
            check_name,
            (),
            expression,
            "the table contains at least one row",
            critical,
        )

    def check_non_null(
        self,
        table_name: str,
        columns: Sequence[str],
        *,
        check_name: str = "required_values_non_null",
        where: Column | None = None,
        critical: bool = True,
    ) -> None:
        required_columns = tuple(columns)
        if not required_columns:
            raise ValueError("A non-null check requires at least one column")
        invalid_when = reduce(or_, (F.col(column).isNull() for column in required_columns))
        if where is not None:
            invalid_when = where & invalid_when
        self._add_predicate(
            table_name,
            check_name,
            required_columns,
            invalid_when,
            f"columns {list(required_columns)} are non-null",
            critical,
        )

    def check_unique(
        self,
        table_name: str,
        columns: Sequence[str],
        *,
        check_name: str | None = None,
        critical: bool = True,
    ) -> None:
        key_columns = tuple(columns)
        if not key_columns:
            raise ValueError("A uniqueness check requires at least one key column")
        resolved_name = check_name or f"unique_{'_'.join(key_columns)}"
        distinct_keys = F.countDistinct(F.struct(*[F.col(column) for column in key_columns]))
        duplicate_surplus = F.count(F.lit(1)) - distinct_keys
        self._add_aggregate(
            table_name,
            resolved_name,
            key_columns,
            duplicate_surplus,
            f"grain {list(key_columns)} is unique",
            critical,
        )

    def check_accepted_values(
        self,
        table_name: str,
        column: str,
        accepted_values: Sequence[object],
        *,
        check_name: str | None = None,
        where: Column | None = None,
        critical: bool = True,
    ) -> None:
        accepted = tuple(accepted_values)
        invalid_when = F.col(column).isNotNull() & ~F.col(column).isin(*accepted)
        if where is not None:
            invalid_when = where & invalid_when
        self._add_predicate(
            table_name,
            check_name or f"accepted_{column}",
            (column,),
            invalid_when,
            f"{column} is one of {list(accepted)}",
            critical,
        )

    def check_numeric_range(
        self,
        table_name: str,
        column: str,
        *,
        minimum: int | float | Decimal | None = None,
        maximum: int | float | Decimal | None = None,
        include_minimum: bool = True,
        include_maximum: bool = True,
        check_name: str | None = None,
        where: Column | None = None,
        critical: bool = True,
    ) -> None:
        if minimum is None and maximum is None:
            raise ValueError("A numeric range check requires a minimum or maximum")

        predicates: list[Column] = []
        if minimum is not None:
            predicates.append(
                F.col(column) < minimum if include_minimum else F.col(column) <= minimum
            )
        if maximum is not None:
            predicates.append(
                F.col(column) > maximum if include_maximum else F.col(column) >= maximum
            )
        invalid_when = reduce(or_, predicates)
        invalid_when = F.col(column).isNotNull() & invalid_when
        if where is not None:
            invalid_when = where & invalid_when

        if minimum is not None and maximum is None:
            operator = ">=" if include_minimum else ">"
            expectation = f"{column} is {operator} {minimum}"
        elif minimum is None and maximum is not None:
            operator = "<=" if include_maximum else "<"
            expectation = f"{column} is {operator} {maximum}"
        else:
            left = "[" if include_minimum else "("
            right = "]" if include_maximum else ")"
            expectation = f"{column} is in {left}{minimum}, {maximum}{right}"
        self._add_predicate(
            table_name,
            check_name or f"range_{column}",
            (column,),
            invalid_when,
            expectation,
            critical,
        )

    def check_date_order(
        self,
        table_name: str,
        earlier_column: str,
        later_column: str,
        *,
        check_name: str | None = None,
        critical: bool = True,
    ) -> None:
        invalid_when = (
            F.col(earlier_column).isNotNull()
            & F.col(later_column).isNotNull()
            & (F.col(later_column) < F.col(earlier_column))
        )
        self._add_predicate(
            table_name,
            check_name or f"{later_column}_not_before_{earlier_column}",
            (earlier_column, later_column),
            invalid_when,
            f"{later_column} does not precede {earlier_column}",
            critical,
        )

    def check_condition(
        self,
        table_name: str,
        *,
        check_name: str,
        invalid_when: Column,
        required_columns: Sequence[str],
        expectation: str,
        critical: bool = True,
    ) -> None:
        self._add_predicate(
            table_name,
            check_name,
            tuple(required_columns),
            invalid_when,
            expectation,
            critical,
        )

    def check_values_present(
        self,
        table_name: str,
        column: str,
        required_values: Sequence[object],
        *,
        check_name: str | None = None,
        critical: bool = True,
    ) -> None:
        values = tuple(required_values)
        present_count = F.countDistinct(F.when(F.col(column).isin(*values), F.col(column)))
        expression = F.lit(len(values)) - present_count
        self._add_aggregate(
            table_name,
            check_name or f"required_{column}_values_present",
            (column,),
            expression,
            f"{column} contains required values {list(values)}",
            critical,
        )

    def check_reference(
        self,
        table_name: str,
        source_columns: Sequence[str],
        reference_table: str,
        reference_columns: Sequence[str],
        *,
        check_name: str | None = None,
        ignore_nulls: bool = True,
        critical: bool = True,
    ) -> None:
        self._require_table(table_name)
        self._require_table(reference_table)
        source_key = tuple(source_columns)
        reference_key = tuple(reference_columns)
        if not source_key or len(source_key) != len(reference_key):
            raise ValueError("Foreign-key checks require equally sized, non-empty key lists")
        resolved_name = check_name or (f"reference_{'_'.join(source_key)}_to_{reference_table}")
        self._register_name(table_name, resolved_name)
        self._reference_checks.append(
            _ReferenceCheck(
                table_name,
                resolved_name,
                source_key,
                reference_table,
                reference_key,
                ignore_nulls,
                critical,
            )
        )

    def run(self) -> DataQualityReport:
        """Execute all registered checks and return every result together."""
        results = list(self._recorded_results)
        results.extend(self._run_schema_checks())
        results.extend(self._run_aggregate_checks())
        results.extend(self._run_reference_checks())
        return DataQualityReport(
            tuple(sorted(results, key=lambda item: (item.table_name, item.check_name)))
        )

    def _run_schema_checks(self) -> list[CheckResult]:
        results: list[CheckResult] = []
        for check in self._schema_checks:
            df = self._tables[check.table_name]
            missing = find_missing_required_columns(df, check.required_columns)
            mismatches: list[str] = []
            if check.expected_types is not None:
                actual_types = {
                    field.name: field.dataType.simpleString() for field in df.schema.fields
                }
                mismatches = [
                    f"{column}: expected {expected}, found {actual_types[column]}"
                    for column, expected in check.expected_types.items()
                    if column in actual_types and actual_types[column] != expected
                ]
            violations = len(missing) + len(mismatches)
            if violations:
                parts = []
                if missing:
                    parts.append(f"missing columns: {missing}")
                if mismatches:
                    parts.append(f"type mismatches: {mismatches}")
                results.append(self._failed(check, violations, "; ".join(parts)))
            else:
                expectation = (
                    "all required columns are present"
                    if check.expected_types is None
                    else "all expected columns have the declared Spark types"
                )
                results.append(self._passed(check, expectation))
        return results

    def _run_aggregate_checks(self) -> list[CheckResult]:
        results: list[CheckResult] = []
        by_table: dict[str, list[_AggregateCheck]] = defaultdict(list)
        for check in self._aggregate_checks:
            by_table[check.table_name].append(check)

        for table_name, checks in by_table.items():
            df = self._tables[table_name]
            runnable: list[_AggregateCheck] = []
            for check in checks:
                missing = find_missing_required_columns(df, check.required_columns)
                if missing:
                    results.append(
                        self._failed(
                            check,
                            len(missing),
                            f"not evaluated because required columns are missing: {missing}",
                        )
                    )
                else:
                    runnable.append(check)
            if not runnable:
                continue

            aliases = [f"_dq_check_{index}" for index in range(len(runnable))]
            try:
                row = df.agg(
                    *[
                        F.coalesce(check.expression.cast("long"), F.lit(0)).alias(alias)
                        for check, alias in zip(runnable, aliases, strict=True)
                    ]
                ).first()
            except Exception as error:  # pragma: no cover - Spark runtime defensive path
                for check in runnable:
                    results.append(
                        self._failed(
                            check,
                            1,
                            f"check execution failed with {type(error).__name__}: {error}",
                        )
                    )
                continue

            for check, alias in zip(runnable, aliases, strict=True):
                violation_count = int(row[alias])
                results.append(
                    self._passed(check, check.expectation)
                    if violation_count == 0
                    else self._failed(
                        check,
                        violation_count,
                        f"Expected {check.expectation}; found {violation_count} violation(s)",
                    )
                )
        return results

    def _run_reference_checks(self) -> list[CheckResult]:
        results: list[CheckResult] = []
        by_table: dict[str, list[_ReferenceCheck]] = defaultdict(list)
        for check in self._reference_checks:
            by_table[check.table_name].append(check)

        for table_name, checks in by_table.items():
            source_df = self._tables[table_name]
            runnable: list[_ReferenceCheck] = []
            for check in checks:
                source_missing = find_missing_required_columns(source_df, check.source_columns)
                reference_df = self._tables[check.reference_table]
                reference_missing = find_missing_required_columns(
                    reference_df, check.reference_columns
                )
                if source_missing or reference_missing:
                    missing_details = []
                    if source_missing:
                        missing_details.append(f"source columns {source_missing}")
                    if reference_missing:
                        missing_details.append(
                            f"{check.reference_table} columns {reference_missing}"
                        )
                    results.append(
                        self._failed(
                            check,
                            len(source_missing) + len(reference_missing),
                            "not evaluated because required "
                            + " and ".join(missing_details)
                            + " are missing",
                        )
                    )
                else:
                    runnable.append(check)
            if not runnable:
                continue

            working = source_df
            invalid_predicates: list[Column] = []
            for index, check in enumerate(runnable):
                marker = f"_dq_reference_match_{index}"
                renamed_columns = [
                    f"_dq_reference_{index}_{column_index}"
                    for column_index in range(len(check.reference_columns))
                ]
                reference_keys = (
                    self._tables[check.reference_table]
                    .select(
                        *[
                            F.col(column).alias(renamed)
                            for column, renamed in zip(
                                check.reference_columns,
                                renamed_columns,
                                strict=True,
                            )
                        ]
                    )
                    .dropDuplicates()
                    .withColumn(marker, F.lit(1))
                )
                join_condition = reduce(
                    and_,
                    [
                        working[source] == reference_keys[renamed]
                        for source, renamed in zip(
                            check.source_columns,
                            renamed_columns,
                            strict=True,
                        )
                    ],
                )
                working = working.join(reference_keys, join_condition, "left")
                unmatched = F.col(marker).isNull()
                if check.ignore_nulls:
                    source_is_complete = reduce(
                        and_, (F.col(column).isNotNull() for column in check.source_columns)
                    )
                    unmatched = source_is_complete & unmatched
                invalid_predicates.append(unmatched)

            aliases = [f"_dq_reference_result_{index}" for index in range(len(runnable))]
            try:
                row = working.agg(
                    *[
                        F.coalesce(
                            F.sum(F.when(predicate, F.lit(1)).otherwise(F.lit(0))),
                            F.lit(0),
                        ).alias(alias)
                        for predicate, alias in zip(
                            invalid_predicates,
                            aliases,
                            strict=True,
                        )
                    ]
                ).first()
            except Exception as error:  # pragma: no cover - Spark runtime defensive path
                for check in runnable:
                    failure_details = (
                        "reference check execution failed with " f"{type(error).__name__}: {error}"
                    )
                    results.append(
                        self._failed(
                            check,
                            1,
                            failure_details,
                        )
                    )
                continue

            for check, alias in zip(runnable, aliases, strict=True):
                violations = int(row[alias])
                expectation = (
                    f"{list(check.source_columns)} resolves to "
                    f"{check.reference_table}.{list(check.reference_columns)}"
                )
                results.append(
                    self._passed(check, expectation)
                    if violations == 0
                    else self._failed(
                        check,
                        violations,
                        f"Expected {expectation}; found {violations} unresolved row(s)",
                    )
                )
        return results

    def _add_predicate(
        self,
        table_name: str,
        check_name: str,
        required_columns: Sequence[str],
        invalid_when: Column,
        expectation: str,
        critical: bool,
    ) -> None:
        expression = F.sum(F.when(invalid_when, F.lit(1)).otherwise(F.lit(0)))
        self._add_aggregate(
            table_name,
            check_name,
            required_columns,
            expression,
            expectation,
            critical,
        )

    def _add_aggregate(
        self,
        table_name: str,
        check_name: str,
        required_columns: Sequence[str],
        expression: Column,
        expectation: str,
        critical: bool,
    ) -> None:
        self._require_table(table_name)
        self._register_name(table_name, check_name)
        self._aggregate_checks.append(
            _AggregateCheck(
                table_name,
                check_name,
                tuple(required_columns),
                expression,
                expectation,
                critical,
            )
        )

    def _register_name(self, table_name: str, check_name: str) -> None:
        identity = (table_name, check_name)
        if identity in self._registered_names:
            raise ValueError(f"Duplicate data-quality check name: {table_name}.{check_name}")
        self._registered_names.add(identity)

    def _require_table(self, table_name: str) -> None:
        if table_name not in self._tables:
            raise ValueError(f"Data-quality table is not registered: {table_name}")

    @staticmethod
    def _passed(check: _RegisteredCheck, details: str) -> CheckResult:
        return CheckResult(
            table_name=check.table_name,
            check_name=check.check_name,
            status="PASS",
            violation_count=0,
            details=details,
            critical=check.critical,
        )

    @staticmethod
    def _failed(
        check: _RegisteredCheck,
        violation_count: int,
        details: str,
    ) -> CheckResult:
        return CheckResult(
            table_name=check.table_name,
            check_name=check.check_name,
            status="FAIL",
            violation_count=violation_count,
            details=details,
            critical=check.critical,
        )


__all__ = [
    "CheckResult",
    "DataQualityFailure",
    "DataQualityReport",
    "DataQualitySuite",
]
