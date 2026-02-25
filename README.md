# DuckDB SQL Simulator

A lightweight local SQL executor that returns pandas DataFrames. This tool enables testing of SQL queries across multiple dialects (T-SQL, PostgreSQL, BigQuery, etc.) by translating them to DuckDB via `sqlglot`. Perfect for local development and testing without needing a database connection.

## Features

- 🔄 **Multi-dialect support** - T-SQL, PostgreSQL, MySQL, BigQuery, Snowflake, and more
- 🛡️ **SQL injection protection** - Validated table names
- 📊 **Pandas output** - Returns standard pandas DataFrames
- 🧪 **Flexible mocking** - Seed from Python dicts or JSON files
- ⚡ **Fast execution** - Powered by DuckDB in-memory engine

## Setup

The project uses `uv` for lightning-fast package management. First, install `uv` (if not already installed). 
Then, clone the repo and sync dependencies:

```bash
uv sync --all-extras
```

## Usage Example

### 1. Simple Dictionary Seeding

You can seed your DuckDB instance with local mock data using python dictionaries or a JSON file.

```python
from duckdb_simulator import DuckdbSQLSeeder, DuckdbSQLExecutor, Dialect

# 1. Define your mock data
mock_data = {
    "users": [
        {"id": 1, "name": "Alice", "role": "Admin"},
        {"id": 2, "name": "Bob", "role": "User"}
    ]
}

# 2. Initialize Seeder
seeder = DuckdbSQLSeeder(mock_data)

# 3. Initialize Executor (let's simulate Azure Synapse T-SQL)
executor = DuckdbSQLExecutor(dialect=Dialect.AZURE_SYNAPSE, seeder=seeder)

# 4. Write query in T-SQL dialect (Notice the TOP keyword instead of LIMIT)
query = "SELECT TOP 1 * FROM users ORDER BY id DESC"

# 5. Execute - the query is automatically translated to DuckDB SQL before execution!
df = executor.query_to_df(query)

print(df)
#    id name  role
# 0   2  Bob  User
```

## Testing

To run the unit tests across syntax transpilation and seeding:

```bash
uv run pytest
```
