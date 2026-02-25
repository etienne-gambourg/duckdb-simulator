import duckdb
import pandas as pd
import json
from typing import Union, Dict, Any
import os
import re

class DuckdbSQLSeeder:
    """
    Seeds a DuckDB database with mock data from JSON configuration or python dictionaries.
    """

    def __init__(self, config: Union[str, Dict[str, Any]]):
        """
        Initializes the DuckDB connection and seeds it based on the config.

        Args:
            config (Union[str, Dict[str, Any]]): A file path to a JSON configuration 
                                                 or a dictionary mapping table names to data (list of dicts).
        """
        self.conn = duckdb.connect(':memory:')  # Use in-memory DB for tests
        self._seed(config)

    def _validate_table_name(self, name: str) -> None:
        """
        Validates that a table name follows SQL identifier rules to prevent SQL injection.
        
        Args:
            name (str): Table name to validate
            
        Raises:
            ValueError: If table name contains invalid characters
        """
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
            raise ValueError(
                f"Invalid table name: '{name}'. "
                "Table names must start with a letter or underscore and contain only alphanumeric characters and underscores."
            )

    def _seed(self, config: Union[str, Dict[str, Any]]) -> None:
        """Seeds database with provided configuration."""
        data_dict = {}
        if isinstance(config, str) and os.path.exists(config):
            with open(config, 'r') as f:
                data_dict = json.load(f)
        elif isinstance(config, dict):
            data_dict = config
        else:
            raise ValueError("Config must be a valid file path or a dictionary.")
        
        for table_name, table_data in data_dict.items():
            # Validate table name to prevent SQL injection
            self._validate_table_name(table_name)
            
            # Convert list of dicts to pandas DataFrame, which DuckDB can read natively.
            df = pd.DataFrame(table_data)
            
            # Register dataframe directly as a table in duckdb
            # Use a unique temporary name based on the table to avoid conflicts
            temp_name = f"_temp_{table_name}_{id(df)}"
            self.conn.register(temp_name, df)
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {temp_name}")
            self.conn.unregister(temp_name)

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Returns the seeded duckdb connection.
        """
        return self.conn