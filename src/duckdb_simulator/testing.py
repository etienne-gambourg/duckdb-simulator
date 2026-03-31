"""
duckdb_executor.testing
-----------------------
Fluent test-data builder and DataFrame assertion helpers.
Usable in any project that tests against a SQLExecutor-compatible backend.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from .seeder import DuckdbSQLSeeder

# ---------------------------------------------------------------------------
# FixtureBuilder — fluent API for building seed data in-memory
# ---------------------------------------------------------------------------


class _TableBuilder:
    """Intermediate builder returned by FixtureBuilder.table()."""

    def __init__(self, parent: FixtureBuilder, name: str) -> None:
        self._parent = parent
        self._name = name
        self._rows: list[dict[str, Any]] = []

    def row(self, **kwargs: Any) -> _TableBuilder:
        """Append a row to the current table."""
        self._rows.append(kwargs)
        return self

    def table(self, name: str) -> _TableBuilder:
        """Start a new table (delegates to parent)."""
        self._flush()
        return self._parent.table(name)

    def build(self) -> DuckdbSQLSeeder:
        """Flush the current table and return a seeded DuckdbSQLSeeder."""
        self._flush()
        return self._parent.build()

    def _flush(self) -> None:
        self._parent._data[self._name] = self._rows


class FixtureBuilder:
    """Fluent builder for DuckdbSQLSeeder seed data.

    Example::

        seeder = (
            FixtureBuilder()
            .table("orders")
              .row(id=1, country="FR", amount=120.0)
              .row(id=2, country="US", amount=200.0)
            .table("products")
              .row(id=1, name="Widget", price=9.99)
            .build()
        )
    """

    def __init__(self) -> None:
        self._data: dict[str, list[dict[str, Any]]] = {}
        self._current: _TableBuilder | None = None

    def table(self, name: str) -> _TableBuilder:
        """Start defining a table. Returns a _TableBuilder for chaining .row() calls."""
        if self._current is not None:
            self._current._flush()
        self._current = _TableBuilder(self, name)
        return self._current

    def build(self) -> DuckdbSQLSeeder:
        """Return a DuckdbSQLSeeder seeded with all defined tables."""
        if self._current is not None:
            self._current._flush()
        if not self._data:
            raise ValueError("FixtureBuilder: no tables defined. Call .table() first.")
        return DuckdbSQLSeeder(self._data)


# ---------------------------------------------------------------------------
# DataFrame assertion helpers
# ---------------------------------------------------------------------------


def assert_scalar(
    df: pd.DataFrame,
    *,
    kpi_name: str,
    expected: float | int | str,
    approx: bool = True,
    tolerance: float = 1e-6,
    **param_filters: Any,
) -> None:
    """Assert that a KPI row in ``df`` has the expected scalar value.

    Args:
        df:           DataFrame produced by MetricQueryBuilder.compose().
        kpi_name:     The ``kpi_name`` column value to filter on.
        expected:     The expected value.
        approx:       If True (default), use approximate float comparison.
        tolerance:    Relative tolerance for approx comparison.
        **param_filters: Additional column filters (e.g. ``country="FR"``).

    Raises:
        AssertionError: If no row matches or the value is wrong.
    """
    mask = df["kpi_name"] == kpi_name
    for col, val in param_filters.items():
        if col not in df.columns:
            raise AssertionError(
                f"Column {col!r} not in DataFrame. Got: {list(df.columns)}"
            )
        mask = mask & (df[col] == val)

    rows = df[mask]
    if rows.empty:
        filters = {"kpi_name": kpi_name, **param_filters}
        raise AssertionError(f"No rows found for filters {filters}. DataFrame:\n{df}")
    if len(rows) > 1:
        raise AssertionError(
            f"Multiple rows found for filters — be more specific:\n{rows}"
        )

    actual = rows.iloc[0]["value"]
    if approx and isinstance(expected, float):
        if abs(actual - expected) > tolerance * max(abs(expected), 1):
            raise AssertionError(
                f"Expected {expected} ≈ actual {actual} (tol={tolerance})"
            )
    else:
        assert actual == expected, f"Expected {expected!r}, got {actual!r}"


def assert_shape(
    df: pd.DataFrame,
    *,
    n_kpis: int,
    n_param_combos: int,
) -> None:
    """Assert the DataFrame has the expected number of rows (n_kpis × n_param_combos).

    Args:
        df:             DataFrame produced by MetricQueryBuilder.compose().
        n_kpis:         Number of distinct KPIs expected.
        n_param_combos: Number of distinct parameter combinations expected.
    """
    expected_rows = n_kpis * n_param_combos
    actual_rows = len(df)
    assert actual_rows == expected_rows, (
        f"Expected {n_kpis} KPIs × {n_param_combos} combos = {expected_rows} rows, "
        f"got {actual_rows}."
    )
    assert (
        "kpi_name" in df.columns
    ), f"Missing 'kpi_name' column. Got: {list(df.columns)}"
    assert "value" in df.columns, f"Missing 'value' column. Got: {list(df.columns)}"
    assert (
        "value_type" in df.columns
    ), f"Missing 'value_type' column. Got: {list(df.columns)}"


def assert_value_types(df: pd.DataFrame, expected_type: str) -> None:
    """Assert that all rows in ``df`` have the given ``value_type``.

    Args:
        df:            DataFrame produced by MetricQueryBuilder.compose().
        expected_type: One of ``"int"``, ``"float"``, ``"str"``.
    """
    bad = df[df["value_type"] != expected_type]
    assert bad.empty, (
        f"Expected all rows to have value_type={expected_type!r}. "
        f"Found unexpected:\n{bad}"
    )
