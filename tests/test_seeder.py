import pytest
from duckdb_simulator.seeder import DuckdbSQLSeeder
import json

def test_seeder_with_dict():
    mock_data = {
        "users": [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ],
        "orders": [
            {"user_id": 1, "amount": 100},
            {"user_id": 2, "amount": 200}
        ]
    }
    seeder = DuckdbSQLSeeder(mock_data)
    conn = seeder.get_connection()
    
    # query to verify
    df_users = conn.execute("SELECT * FROM users").fetchdf()
    assert len(df_users) == 2
    assert list(df_users['name']) == ["Alice", "Bob"]

    df_orders = conn.execute("SELECT * FROM orders").fetchdf()
    assert len(df_orders) == 2

def test_seeder_with_json_file(tmp_path):
    mock_data = {
        "products": [
            {"id": 1, "price": 10.5},
            {"id": 2, "price": 20.0}
        ]
    }
    file_path = tmp_path / "mock.json"
    with open(file_path, 'w') as f:
        json.dump(mock_data, f)
        
    seeder = DuckdbSQLSeeder(str(file_path))
    conn = seeder.get_connection()
    
    df_products = conn.execute("SELECT * FROM products").fetchdf()
    assert len(df_products) == 2

def test_seeder_invalid_table_name_with_special_chars():
    """Test that table names with special characters are rejected."""
    mock_data = {
        "users; DROP TABLE users--": [
            {"id": 1, "name": "Alice"}
        ]
    }
    with pytest.raises(ValueError, match="Invalid table name"):
        DuckdbSQLSeeder(mock_data)

def test_seeder_invalid_table_name_starting_with_number():
    """Test that table names starting with numbers are rejected."""
    mock_data = {
        "123users": [
            {"id": 1, "name": "Alice"}
        ]
    }
    with pytest.raises(ValueError, match="Invalid table name"):
        DuckdbSQLSeeder(mock_data)

def test_seeder_invalid_table_name_with_spaces():
    """Test that table names with spaces are rejected."""
    mock_data = {
        "user table": [
            {"id": 1, "name": "Alice"}
        ]
    }
    with pytest.raises(ValueError, match="Invalid table name"):
        DuckdbSQLSeeder(mock_data)

def test_seeder_valid_table_names_with_underscores():
    """Test that valid table names with underscores work correctly."""
    mock_data = {
        "user_accounts": [
            {"id": 1, "name": "Alice"}
        ],
        "_private_data": [
            {"id": 1, "value": 100}
        ],
        "table_123_test": [
            {"id": 1}
        ]
    }
    seeder = DuckdbSQLSeeder(mock_data)
    conn = seeder.get_connection()
    
    # Verify all tables were created
    df = conn.execute("SELECT * FROM user_accounts").fetchdf()
    assert len(df) == 1
    
    df = conn.execute("SELECT * FROM _private_data").fetchdf()
    assert len(df) == 1
    
    df = conn.execute("SELECT * FROM table_123_test").fetchdf()
    assert len(df) == 1

def test_seeder_invalid_json_file():
    """Test that non-existent JSON files raise ValueError."""
    with pytest.raises(ValueError, match="Config must be a valid file path"):
        DuckdbSQLSeeder("/path/to/nonexistent/file.json")

def test_seeder_multiple_tables_no_conflict():
    """Test that multiple tables can be created without temp name conflicts."""
    mock_data = {
        "table1": [{"id": 1}],
        "table2": [{"id": 2}],
        "table3": [{"id": 3}],
    }
    seeder = DuckdbSQLSeeder(mock_data)
    conn = seeder.get_connection()
    
    # Verify all tables exist
    for i in range(1, 4):
        df = conn.execute(f"SELECT * FROM table{i}").fetchdf()
        assert len(df) == 1
        assert df['id'].iloc[0] == i