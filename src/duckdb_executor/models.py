from enum import Enum
import sqlglot

class Dialect(str, Enum):
   """
    Enum representing support SQL dialects via sqlglot.
   """
   # Map common dialects directly to sqlglot's string representations.
   TSQL = "tsql"
   AZURE_SYNAPSE = "azure-synapse-t-sql" # usually mapped to tsql internally by sqlglot but we can retain the name
   POSTGRES = "postgres"
   MYSQL = "mysql"
   SNOWFLAKE = "snowflake"
   BIGQUERY = "bigquery"
   ORACLE = "oracle"
   REDSHIFT = "redshift"
   SQLITE = "sqlite"
   PRESTO = "presto"
   TRINO = "trino"
   SPARK = "spark"
   DATABRICKS = "databricks"
   DUCKDB = "duckdb"

   @classmethod
   def to_sqlglot_dialect(cls, dialect: str) -> str:
        """
        Converts the provided dialect string to a valid sqlglot dialect if needed.
        For example, 'azure-synapse-t-sql' resolves to 'tsql'.
        """
        if dialect == cls.AZURE_SYNAPSE:
            return "tsql"
        return dialect