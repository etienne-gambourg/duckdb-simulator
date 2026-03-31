"""Tests for the duckdb_simulator testing toolkit."""

from __future__ import annotations

import pytest

from duckdb_simulator import (
    DuckdbSQLExecutor,
    DuckdbSQLSeeder,
    Dialect,
    FixtureBuilder,
    assert_scalar,
    assert_shape,
    assert_value_types,
    fixtures,
)

# ---------------------------------------------------------------------------
# FixtureBuilder
# ---------------------------------------------------------------------------


def test_fixture_builder_single_table():
    seeder = (
        FixtureBuilder()
        .table("orders")
        .row(id=1, country="FR", amount=100.0)
        .row(id=2, country="US", amount=200.0)
        .build()
    )
    executor = DuckdbSQLExecutor(dialect=Dialect.DUCKDB, seeder=seeder)
    df = executor.query_to_df("SELECT COUNT(*) AS n FROM orders")
    assert df.iloc[0]["n"] == 2


def test_fixture_builder_multiple_tables():
    seeder = (
        FixtureBuilder()
        .table("orders")
        .row(id=1, amount=50.0)
        .table("products")
        .row(id=1, name="Widget")
        .build()
    )
    executor = DuckdbSQLExecutor(dialect=Dialect.DUCKDB, seeder=seeder)
    assert executor.query_to_df("SELECT COUNT(*) AS n FROM orders").iloc[0]["n"] == 1
    assert executor.query_to_df("SELECT COUNT(*) AS n FROM products").iloc[0]["n"] == 1


def test_fixture_builder_empty_raises():
    with pytest.raises(ValueError, match="no tables defined"):
        FixtureBuilder().build()


def test_fixture_builder_data_integrity():
    seeder = FixtureBuilder().table("users").row(id=1, country="FR", score=9.5).build()
    executor = DuckdbSQLExecutor(dialect=Dialect.DUCKDB, seeder=seeder)
    df = executor.query_to_df("SELECT score FROM users WHERE country = 'FR'")
    assert df.iloc[0]["score"] == pytest.approx(9.5)


# ---------------------------------------------------------------------------
# Built-in fixtures
# ---------------------------------------------------------------------------


def test_orders_fixture_has_known_rows():
    seeder = DuckdbSQLSeeder(fixtures.ORDERS)
    executor = DuckdbSQLExecutor(dialect=Dialect.DUCKDB, seeder=seeder)
    df = executor.query_to_df("SELECT COUNT(*) AS n FROM orders")
    assert df.iloc[0]["n"] == 10


def test_orders_fixture_fr_revenue():
    seeder = DuckdbSQLSeeder(fixtures.ORDERS)
    executor = DuckdbSQLExecutor(dialect=Dialect.DUCKDB, seeder=seeder)
    df = executor.query_to_df(
        "SELECT SUM(amount) AS rev FROM orders WHERE country = 'FR'"
    )
    assert df.iloc[0]["rev"] == pytest.approx(305.0)


def test_orders_fixture_us_revenue():
    seeder = DuckdbSQLSeeder(fixtures.ORDERS)
    executor = DuckdbSQLExecutor(dialect=Dialect.DUCKDB, seeder=seeder)
    df = executor.query_to_df(
        "SELECT SUM(amount) AS rev FROM orders WHERE country = 'US'"
    )
    assert df.iloc[0]["rev"] == pytest.approx(830.0)


def test_products_fixture():
    seeder = DuckdbSQLSeeder(fixtures.PRODUCTS)
    executor = DuckdbSQLExecutor(dialect=Dialect.DUCKDB, seeder=seeder)
    df = executor.query_to_df("SELECT COUNT(*) AS n FROM products")
    assert df.iloc[0]["n"] == 4


def test_users_fixture():
    seeder = DuckdbSQLSeeder(fixtures.USERS)
    executor = DuckdbSQLExecutor(dialect=Dialect.DUCKDB, seeder=seeder)
    df = executor.query_to_df("SELECT COUNT(*) AS n FROM users WHERE active = TRUE")
    assert df.iloc[0]["n"] == 5


def test_full_fixture_joins():
    seeder = DuckdbSQLSeeder(fixtures.FULL)
    executor = DuckdbSQLExecutor(dialect=Dialect.DUCKDB, seeder=seeder)
    df = executor.query_to_df(
        "SELECT COUNT(*) AS n FROM orders o JOIN users u ON o.country = u.country"
    )
    assert df.iloc[0]["n"] > 0


# ---------------------------------------------------------------------------
# assert_scalar
# ---------------------------------------------------------------------------


def _make_kpi_df():
    """Build a minimal KPI-shaped DataFrame for assertion tests."""
    import pandas as pd

    return pd.DataFrame(
        [
            {
                "kpi_name": "revenue",
                "country": "FR",
                "value": 305.0,
                "value_type": "float",
            },
            {
                "kpi_name": "revenue",
                "country": "US",
                "value": 830.0,
                "value_type": "float",
            },
            {
                "kpi_name": "order_count",
                "country": "FR",
                "value": 4,
                "value_type": "int",
            },
        ]
    )


def test_assert_scalar_passes():
    df = _make_kpi_df()
    assert_scalar(df, kpi_name="revenue", country="FR", expected=305.0)


def test_assert_scalar_int():
    df = _make_kpi_df()
    assert_scalar(df, kpi_name="order_count", country="FR", expected=4, approx=False)


def test_assert_scalar_wrong_value():
    df = _make_kpi_df()
    with pytest.raises(AssertionError):
        assert_scalar(df, kpi_name="revenue", country="FR", expected=999.0)


def test_assert_scalar_no_match():
    df = _make_kpi_df()
    with pytest.raises(AssertionError, match="No rows found"):
        assert_scalar(df, kpi_name="revenue", country="DE", expected=200.0)


def test_assert_scalar_bad_column():
    df = _make_kpi_df()
    with pytest.raises(AssertionError, match="Column"):
        assert_scalar(df, kpi_name="revenue", nonexistent_col="X", expected=1.0)


# ---------------------------------------------------------------------------
# assert_shape
# ---------------------------------------------------------------------------


def test_assert_shape_passes():
    import pandas as pd

    square_df = pd.DataFrame(
        [
            {
                "kpi_name": "revenue",
                "country": "FR",
                "value": 305.0,
                "value_type": "float",
            },
            {
                "kpi_name": "revenue",
                "country": "US",
                "value": 830.0,
                "value_type": "float",
            },
            {
                "kpi_name": "order_count",
                "country": "FR",
                "value": 4,
                "value_type": "int",
            },
            {
                "kpi_name": "order_count",
                "country": "US",
                "value": 4,
                "value_type": "int",
            },
        ]
    )
    assert_shape(square_df, n_kpis=2, n_param_combos=2)


def test_assert_shape_wrong_count():
    df = _make_kpi_df()
    with pytest.raises(AssertionError, match="rows"):
        assert_shape(df, n_kpis=5, n_param_combos=5)


# ---------------------------------------------------------------------------
# assert_value_types
# ---------------------------------------------------------------------------


def test_assert_value_types_passes():
    import pandas as pd

    df = pd.DataFrame(
        [
            {"kpi_name": "revenue", "value": 1.0, "value_type": "float"},
            {"kpi_name": "revenue", "value": 2.0, "value_type": "float"},
        ]
    )
    assert_value_types(df, "float")


def test_assert_value_types_fails():
    import pandas as pd

    df = pd.DataFrame(
        [
            {"kpi_name": "revenue", "value": 1.0, "value_type": "float"},
            {"kpi_name": "order_count", "value": 2, "value_type": "int"},
        ]
    )
    with pytest.raises(AssertionError):
        assert_value_types(df, "float")


# ---------------------------------------------------------------------------
# pytest_plugin fixtures (imported directly, not via conftest)
# ---------------------------------------------------------------------------


def test_pytest_plugin_orders_executor():
    from duckdb_simulator.pytest_plugin import orders_executor as _fixture

    assert callable(_fixture)


def test_pytest_plugin_full_executor_query(full_executor):
    df = full_executor.query_to_df("SELECT COUNT(*) AS n FROM orders")
    assert df.iloc[0]["n"] == 10
