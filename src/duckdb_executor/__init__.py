from .models import Dialect
from .seeder import DuckdbSQLSeeder
from .executor import DuckdbSQLExecutor
from .protocols import SQLExecutor
from .testing import FixtureBuilder, assert_scalar, assert_shape, assert_value_types
from . import fixtures

__all__ = [
    # Core
    "Dialect",
    "DuckdbSQLSeeder",
    "DuckdbSQLExecutor",
    "SQLExecutor",
    # Testing toolkit
    "FixtureBuilder",
    "assert_scalar",
    "assert_shape",
    "assert_value_types",
    "fixtures",
]
