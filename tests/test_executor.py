import pytest
import pandas as pd
from duckdb_executor.seeder import DuckdbSQLSeeder
from duckdb_executor.executor import DuckdbSQLExecutor
from duckdb_executor.models import Dialect

@pytest.fixture
def mock_seeder():
    data = {
        "employees": [
            {"id": 1, "name": "Alice", "salary": 50000, "dept_id": 1},
            {"id": 2, "name": "Bob", "salary": 60000, "dept_id": 1},
            {"id": 3, "name": "Charlie", "salary": 70000, "dept_id": 2}
        ],
        "departments": [
            {"id": 1, "name": "Engineering"},
            {"id": 2, "name": "Marketing"}
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

def test_executor_join_query(mock_seeder):
    executor = DuckdbSQLExecutor(dialect=Dialect.POSTGRES, seeder=mock_seeder)
    query = """
    SELECT e.name AS employee_name, d.name AS department_name
    FROM employees e
    JOIN departments d ON e.dept_id = d.id
    ORDER BY e.id
    """
    df = executor.query_to_df(query)
    assert len(df) == 3
    assert list(df['employee_name']) == ["Alice", "Bob", "Charlie"]
    assert list(df['department_name']) == ["Engineering", "Engineering", "Marketing"]

def test_executor_cte_query(mock_seeder):
    executor = DuckdbSQLExecutor(dialect=Dialect.POSTGRES, seeder=mock_seeder)
    query = """
    WITH HighEarners AS (
        SELECT name, salary FROM employees WHERE salary > 55000
    )
    SELECT * FROM HighEarners ORDER BY salary DESC
    """
    df = executor.query_to_df(query)
    assert len(df) == 2
    assert list(df['name']) == ["Charlie", "Bob"]
    assert list(df['salary']) == [70000, 60000]

def test_executor_window_function_query(mock_seeder):
    executor = DuckdbSQLExecutor(dialect=Dialect.POSTGRES, seeder=mock_seeder)
    query = """
    SELECT name, salary, 
           RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) as dept_rank
    FROM employees
    ORDER BY id
    """
    df = executor.query_to_df(query)
    assert len(df) == 3
    assert list(df['name']) == ["Alice", "Bob", "Charlie"]
    # Alice (Engineering) has lowest salary (50k) so rank 2 in dept 1
    # Bob (Engineering) has highest salary (60k) so rank 1 in dept 1
    # Charlie (Marketing) has 70k so rank 1 in dept 2
    assert list(df['dept_rank']) == [2, 1, 1]

