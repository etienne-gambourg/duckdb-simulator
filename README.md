# DuckDB SQL Simulator for Dataiku

This repository simulates the behavior of Dataiku's `SQLExecutor2.query_to_df(query)` for local testing. It translates syntax from multiple SQL dialects into DuckDB SQL via `sqlglot`, dynamically seeds test data into DuckDB from Python dictionaries or JSON configuration files, and fetches the execution results as standard `pandas` DataFrames.

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
from duckdb_executor import DuckdbSQLSeeder, DuckdbSQLExecutor, Dialect

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
