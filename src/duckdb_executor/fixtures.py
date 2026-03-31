"""
duckdb_executor.fixtures
------------------------
Pre-built generic seed datasets for common testing scenarios.
Import and pass directly to DuckdbSQLSeeder or FixtureBuilder.
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Generic orders dataset
# ---------------------------------------------------------------------------

ORDERS: dict[str, list[dict[str, Any]]] = {
    "orders": [
        {"id": 1, "country": "FR", "product": "Widget", "amount": 120.0, "quantity": 2},
        {"id": 2, "country": "FR", "product": "Gadget", "amount": 80.0, "quantity": 1},
        {"id": 3, "country": "US", "product": "Widget", "amount": 200.0, "quantity": 4},
        {
            "id": 4,
            "country": "US",
            "product": "Doohickey",
            "amount": 150.0,
            "quantity": 3,
        },
        {"id": 5, "country": "FR", "product": "Widget", "amount": 60.0, "quantity": 1},
        {"id": 6, "country": "US", "product": "Gadget", "amount": 300.0, "quantity": 5},
        {"id": 7, "country": "DE", "product": "Gadget", "amount": 90.0, "quantity": 2},
        {
            "id": 8,
            "country": "DE",
            "product": "Doohickey",
            "amount": 110.0,
            "quantity": 2,
        },
        {
            "id": 9,
            "country": "FR",
            "product": "Doohickey",
            "amount": 45.0,
            "quantity": 1,
        },
        {
            "id": 10,
            "country": "US",
            "product": "Widget",
            "amount": 180.0,
            "quantity": 3,
        },
    ]
}

# Known aggregates for assertions:
# revenue FR  = 120 + 80 + 60 + 45 = 305.0
# revenue US  = 200 + 150 + 300 + 180 = 830.0
# revenue DE  = 90 + 110 = 200.0
# order_count FR = 4, US = 4, DE = 2
# avg_basket FR  = 305 / 4 = 76.25, US = 830 / 4 = 207.5, DE = 200 / 2 = 100.0

# ---------------------------------------------------------------------------
# Generic products dataset
# ---------------------------------------------------------------------------

PRODUCTS: dict[str, list[dict[str, Any]]] = {
    "products": [
        {"id": 1, "name": "Widget", "category": "hardware", "price": 49.99},
        {"id": 2, "name": "Gadget", "category": "hardware", "price": 79.99},
        {"id": 3, "name": "Doohickey", "category": "software", "price": 29.99},
        {"id": 4, "name": "Thingamajig", "category": "software", "price": 19.99},
    ]
}

# ---------------------------------------------------------------------------
# Generic users dataset
# ---------------------------------------------------------------------------

USERS: dict[str, list[dict[str, Any]]] = {
    "users": [
        {"id": 1, "country": "FR", "segment": "premium", "active": True},
        {"id": 2, "country": "FR", "segment": "standard", "active": True},
        {"id": 3, "country": "US", "segment": "premium", "active": True},
        {"id": 4, "country": "US", "segment": "standard", "active": False},
        {"id": 5, "country": "DE", "segment": "premium", "active": True},
        {"id": 6, "country": "DE", "segment": "standard", "active": True},
    ]
}

# ---------------------------------------------------------------------------
# Combined: orders + products + users (for join scenarios)
# ---------------------------------------------------------------------------

FULL: dict[str, list[dict[str, Any]]] = {
    **ORDERS,
    **PRODUCTS,
    **USERS,
}
