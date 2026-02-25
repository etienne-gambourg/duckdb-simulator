import pandas as pd
from typing import Protocol, runtime_checkable

@runtime_checkable
class SQLExecutor(Protocol):
    """
    Protocol for SQL executor compatibility.
    Compatible with SQLExecutor2.query_to_df
    """

    def query_to_df(self, query: str) -> pd.DataFrame:
        """
        Executes a SQL query and returns results as DataFrame.

        Args:
            query (str): SQL query to execute

        Returns:
            pd.DataFrame: Result of SQL query

        Raises:
            ValueError: If query is invalid
            Exception: If execution fails
        """
        ...
