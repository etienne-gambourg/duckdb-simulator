"""
duckdb_simulator.pytest_plugin
-----------------------------
Ready-made pytest fixtures. Register in conftest.py:

    from duckdb_simulator.pytest_plugin import *  # noqa: F401, F403

Or selectively:

    from duckdb_simulator.pytest_plugin import orders_executor
"""

from __future__ import annotations

import pytest

from .executor import DuckdbSQLExecutor
from .fixtures import FULL, ORDERS, PRODUCTS, USERS
from .models import Dialect
from .seeder import DuckdbSQLSeeder


@pytest.fixture
def orders_executor() -> DuckdbSQLExecutor:
    """DuckDB executor pre-seeded with the generic orders table."""
    seeder = DuckdbSQLSeeder(ORDERS)
    return DuckdbSQLExecutor(dialect=Dialect.DUCKDB, seeder=seeder)


@pytest.fixture
def products_executor() -> DuckdbSQLExecutor:
    """DuckDB executor pre-seeded with the generic products table."""
    seeder = DuckdbSQLSeeder(PRODUCTS)
    return DuckdbSQLExecutor(dialect=Dialect.DUCKDB, seeder=seeder)


@pytest.fixture
def users_executor() -> DuckdbSQLExecutor:
    """DuckDB executor pre-seeded with the generic users table."""
    seeder = DuckdbSQLSeeder(USERS)
    return DuckdbSQLExecutor(dialect=Dialect.DUCKDB, seeder=seeder)


@pytest.fixture
def full_executor() -> DuckdbSQLExecutor:
    """DuckDB executor pre-seeded with orders + products + users tables."""
    seeder = DuckdbSQLSeeder(FULL)
    return DuckdbSQLExecutor(dialect=Dialect.DUCKDB, seeder=seeder)


@pytest.fixture
def blank_executor() -> DuckdbSQLExecutor:
    """DuckDB executor with no tables — seed it yourself via FixtureBuilder."""
    seeder = DuckdbSQLSeeder({"_empty": [{"_": 1}]})
    return DuckdbSQLExecutor(dialect=Dialect.DUCKDB, seeder=seeder)
