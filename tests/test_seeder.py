import pytest
import pandas as pd
from duckdb_executor.seeder import DuckdbSQLSeeder
import json
import os

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
