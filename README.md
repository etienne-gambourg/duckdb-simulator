# duckdb-simulator

A lightweight local SQL executor that mimics Dataiku's `SQLExecutor2.query_to_df()` interface, backed by DuckDB and [sqlglot](https://github.com/tobymao/sqlglot) for multi-dialect translation.

Use it to write and run SQL tests locally — no Dataiku licence, no database connection required.

---

## Features

- **Drop-in Dataiku mock** — any object implementing `query_to_df(query) -> DataFrame` satisfies the `SQLExecutor` protocol
- **Multi-dialect support** — T-SQL, PostgreSQL, BigQuery, Snowflake, MySQL and more, translated to DuckDB via sqlglot
- **Fluent fixture builder** — define in-memory tables with a chainable API
- **Built-in assertion helpers** — `assert_scalar`, `assert_shape`, `assert_value_types` for KPI DataFrames
- **pytest fixtures** — `orders_executor`, `full_executor`, etc. available out of the box
- **SQL injection protection** — table names validated on seed

---

## Install

```bash
uv add git+https://github.com/etienne-gambourg/duckdb-simulator
```

---

## Quick start

### Seed and query

```python
from duckdb_simulator import DuckdbSQLSeeder, DuckdbSQLExecutor, Dialect

seeder = DuckdbSQLSeeder({
    "orders": [
        {"id": 1, "country": "FR", "amount": 120.0},
        {"id": 2, "country": "US", "amount": 200.0},
    ]
})

executor = DuckdbSQLExecutor(dialect=Dialect.DUCKDB, seeder=seeder)
df = executor.query_to_df("SELECT country, SUM(amount) AS revenue FROM orders GROUP BY country")
#   country  revenue
# 0      FR    120.0
# 1      US    200.0
```

### Fluent FixtureBuilder

```python
from duckdb_simulator import FixtureBuilder, DuckdbSQLExecutor, Dialect

seeder = (
    FixtureBuilder()
    .table("orders")
      .row(id=1, country="FR", amount=120.0)
      .row(id=2, country="US", amount=200.0)
    .table("products")
      .row(id=1, name="Widget", price=9.99)
    .build()
)

executor = DuckdbSQLExecutor(dialect=Dialect.DUCKDB, seeder=seeder)
```

### Multi-dialect (T-SQL → DuckDB)

```python
executor = DuckdbSQLExecutor(dialect=Dialect.TSQL, seeder=seeder)
df = executor.query_to_df("SELECT TOP 1 * FROM orders ORDER BY amount DESC")
```

---

## Testing toolkit

### Assertion helpers

```python
from duckdb_simulator import assert_scalar, assert_shape, assert_value_types

# Assert a single KPI value
assert_scalar(df, kpi_name="revenue", country="FR", expected=305.0)

# Assert DataFrame shape (n_kpis × n_param_combos rows)
assert_shape(df, n_kpis=2, n_param_combos=3)

# Assert all rows have the same value_type
assert_value_types(df, "float")
```

### Built-in fixtures

```python
from duckdb_simulator import fixtures, DuckdbSQLSeeder, DuckdbSQLExecutor, Dialect

# Pre-built datasets: ORDERS (10 rows), PRODUCTS (4 rows), USERS (6 rows), FULL (all joined)
seeder = DuckdbSQLSeeder(fixtures.ORDERS)
executor = DuckdbSQLExecutor(dialect=Dialect.DUCKDB, seeder=seeder)
```

### pytest fixtures (conftest-free)

Register the plugin in `conftest.py`:

```python
# conftest.py
pytest_plugins = ["duckdb_simulator.pytest_plugin"]
```

Then use in any test:

```python
def test_order_count(orders_executor):
    df = orders_executor.query_to_df("SELECT COUNT(*) AS n FROM orders")
    assert df.iloc[0]["n"] == 10

def test_join(full_executor):
    df = full_executor.query_to_df(
        "SELECT COUNT(*) AS n FROM orders o JOIN users u ON o.country = u.country"
    )
    assert df.iloc[0]["n"] > 0
```

Available fixtures: `orders_executor`, `products_executor`, `users_executor`, `full_executor`, `blank_executor`.

---

## SQLExecutor protocol

```python
from duckdb_simulator import SQLExecutor

# Any object with query_to_df(query: str) -> pd.DataFrame satisfies it
assert isinstance(executor, SQLExecutor)

# Works with Dataiku's SQLExecutor2 too:
# from dataiku.core.sql import SQLExecutor2
# assert isinstance(SQLExecutor2(), SQLExecutor)
```

---

## Supported dialects

| Constant | SQL dialect |
|----------|-------------|
| `Dialect.DUCKDB` | DuckDB (no translation) |
| `Dialect.TSQL` | T-SQL / SQL Server / Azure Synapse |
| `Dialect.POSTGRES` | PostgreSQL |
| `Dialect.MYSQL` | MySQL |
| `Dialect.BIGQUERY` | BigQuery |
| `Dialect.SNOWFLAKE` | Snowflake |
| `Dialect.SPARK` | Spark SQL |

---

## Error types

```python
from duckdb_simulator import QueryTranslationError, QueryExecutionError

try:
    executor.query_to_df("INVALID SQL @@##")
except QueryTranslationError as e:
    print("sqlglot couldn't translate:", e)
except QueryExecutionError as e:
    print("DuckDB rejected the query:", e)
```

---

## Dev setup

```bash
git clone https://github.com/etienne-gambourg/duckdb-simulator
cd duckdb-simulator
uv sync
uv run python -m pytest -v
```
