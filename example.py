from duckdb_executor import DuckdbSQLSeeder, DuckdbSQLExecutor, Dialect
import pandas as pd

mock_data = {
    "users": [
        {"id": 1, "name": "Alice", "role": "Admin"},
        {"id": 2, "name": "Bob", "role": "User"}
    ]
}

seeder = DuckdbSQLSeeder(mock_data)
executor = DuckdbSQLExecutor(dialect=Dialect.AZURE_SYNAPSE, seeder=seeder)
query = "SELECT TOP 1 * FROM users ORDER BY id DESC"
df = executor.query_to_df(query)

print("T-SQL Result in DuckDB:")
print(df)
