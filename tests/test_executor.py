import pytest
import pandas as pd
from duckdb_executor.seeder import DuckdbSQLSeeder
from duckdb_executor.executor import DuckdbSQLExecutor
from duckdb_executor.models import Dialect

@pytest.fixture
def mock_seeder():
    data = {
        "employees": [
            {"id": 1, "name": "Alice", "salary": 50000},
            {"id": 2, "name": "Bob", "salary": 60000},
            {"id": 3, "name": "Charlie", "salary": 70000}
        ]
    }
    return DuckdbSQLSeeder(data)

def test_executor_tsql_translation(mock_seeder):
    # Use azure-synapse-t-sql which maps to tsql in sqlglot
    executor = DuckdbSQLExecutor(dialect=Dialect.AZURE_SYNAPSE, seeder=mock_seeder)
    
    # TSQL uses TOP N instead of LIMIT N
    query = "SELECT TOP 2 * FROM employees ORDER BY salary DESC"
    
    df = executor.query_to_df(query)
    
    assert len(df) == 2
    assert list(df['name']) == ["Charlie", "Bob"]
    assert list(df['salary']) == [70000, 60000]

def test_executor_invalid_query(mock_seeder):
    executor = DuckdbSQLExecutor(dialect=Dialect.POSTGRES, seeder=mock_seeder)
    with pytest.raises(Exception):
        executor.query_to_df("SELECT * FROM nonexistent_table")

def test_executor_protocol_compliance(mock_seeder):
    from duckdb_executor.protocols import SQLExecutor
    executor = DuckdbSQLExecutor(dialect=Dialect.TSQL, seeder=mock_seeder)
    
    # Verify it matches the protocol structurally using isinstance
    assert isinstance(executor, SQLExecutor)
