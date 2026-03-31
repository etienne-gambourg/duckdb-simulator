import json

import pytest

from duckdb_simulator.seeder import DuckdbSQLSeeder


def test_seeder_with_dict():
    mock_data = {
        "users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
        "orders": [{"user_id": 1, "amount": 100}, {"user_id": 2, "amount": 200}],
    }
    seeder = DuckdbSQLSeeder(mock_data)
    conn = seeder.get_connection()

    df_users = conn.execute("SELECT * FROM users").fetchdf()
    assert len(df_users) == 2
    assert list(df_users["name"]) == ["Alice", "Bob"]

    df_orders = conn.execute("SELECT * FROM orders").fetchdf()
    assert len(df_orders) == 2


def test_seeder_with_json_file(tmp_path):
    mock_data = {"products": [{"id": 1, "price": 10.5}, {"id": 2, "price": 20.0}]}
    file_path = tmp_path / "mock.json"
    with open(file_path, "w") as f:
        json.dump(mock_data, f)

    seeder = DuckdbSQLSeeder(str(file_path))
    conn = seeder.get_connection()
    df_products = conn.execute("SELECT * FROM products").fetchdf()
    assert len(df_products) == 2


def test_seeder_invalid_table_name_sql_injection():
    with pytest.raises(ValueError, match="Invalid table name"):
        DuckdbSQLSeeder({"users; DROP TABLE users--": [{"id": 1}]})


def test_seeder_invalid_table_name_starts_with_number():
    with pytest.raises(ValueError, match="Invalid table name"):
        DuckdbSQLSeeder({"123users": [{"id": 1}]})


def test_seeder_invalid_table_name_with_spaces():
    with pytest.raises(ValueError, match="Invalid table name"):
        DuckdbSQLSeeder({"user table": [{"id": 1}]})


def test_seeder_valid_table_names_with_underscores():
    seeder = DuckdbSQLSeeder(
        {
            "user_accounts": [{"id": 1}],
            "_private_data": [{"value": 100}],
            "table_123_test": [{"id": 1}],
        }
    )
    conn = seeder.get_connection()
    assert len(conn.execute("SELECT * FROM user_accounts").fetchdf()) == 1
    assert len(conn.execute("SELECT * FROM _private_data").fetchdf()) == 1
    assert len(conn.execute("SELECT * FROM table_123_test").fetchdf()) == 1


def test_seeder_invalid_json_file():
    with pytest.raises(ValueError, match="Config must be a valid file path"):
        DuckdbSQLSeeder("/path/to/nonexistent/file.json")


def test_seeder_multiple_tables_no_conflict():
    seeder = DuckdbSQLSeeder(
        {
            "table1": [{"id": 1}],
            "table2": [{"id": 2}],
            "table3": [{"id": 3}],
        }
    )
    conn = seeder.get_connection()
    for i in range(1, 4):
        df = conn.execute(f"SELECT * FROM table{i}").fetchdf()
        assert df["id"].iloc[0] == i
