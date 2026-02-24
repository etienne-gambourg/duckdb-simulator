import duckdb
import pandas as pd
import json
from typing import Union, Dict, Any
import os

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

    def _seed(self, config: Union[str, Dict[str, Any]]):
        data_dict = {}
        if isinstance(config, str) and os.path.exists(config):
            with open(config, 'r') as f:
                data_dict = json.load(f)
        elif isinstance(config, dict):
            data_dict = config
        else:
            raise ValueError("Config must be a valid file path or a dictionary.")
        
        for table_name, table_data in data_dict.items():
            # Convert list of dicts to pandas DataFrame, which DuckDB can read natively.
            df = pd.DataFrame(table_data)
            
            # Register dataframe directly as a table in duckdb
            self.conn.register('temp_df', df)
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
            self.conn.unregister('temp_df')

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Returns the seeded duckdb connection.
        """
        return self.conn
