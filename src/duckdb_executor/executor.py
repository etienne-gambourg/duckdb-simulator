import pandas as pd
import sqlglot
from .models import Dialect
from .seeder import DuckdbSQLSeeder

class DuckdbSQLExecutor:
    """
    A local DuckDB-based executor that translates SQL queries using sqlglot,
    and runs them against mock data seeded via DuckdbSQLSeeder.
    Compliant with the SQLExecutor protocol.
    """
    def __init__(self, dialect: str, seeder: DuckdbSQLSeeder):
        """
        Initializes the executor with a specific dialect and a seeded DB connection.
        
        Args:
            dialect (str): The dialect of the input queries (e.g. "azure-synapse-t-sql").
            seeder (DuckdbSQLSeeder): An initialized seeder with populated tables.
        """
        try:
            # Check if dialect is a supported valid Enum, and extract its logic
            self.dialect_enum = Dialect(dialect)
            self.read_dialect = self.dialect_enum.to_sqlglot_dialect(dialect)
        except ValueError:
            # Fallback to direct string if someone passes a valid sqlglot dialect not in Enum
            self.read_dialect = dialect
        
        self.conn = seeder.get_connection()

    def query_to_df(self, query: str) -> pd.DataFrame:
        """
        Translates the given query from the source dialect to duckdb dialect,
        executes it, and returns a pandas DataFrame.
        """
        try:
            # Parse the query with the defined read dialect and transpile to duckdb
            translated_query = sqlglot.transpile(query, read=self.read_dialect, write="duckdb")[0]
        except Exception as e:
            raise ValueError(f"Failed to parse and translate query: {e}")
        
        try:
            # Execute the query using DuckDB's execute method, which supports returning a pandas df directly.
            result = self.conn.execute(translated_query)
            if result is None:
                raise Exception("Query returned no result object.")
            return result.fetchdf()
        except duckdb.Error as e:
            raise Exception(f"Database execution failed: {e}")
        except Exception as e:
            raise Exception(f"An unexpected error occurred during execution: {e}")
