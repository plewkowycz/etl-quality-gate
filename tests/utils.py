"""Common test utilities and fixtures."""

import time
from typing import Any
from unittest.mock import patch

import requests

from src.api import BaseAPIClient


class MockResponse:
    """Mock response object for testing."""

    def __init__(self, json_data: dict[str, Any], status_code: int = 200):
        """Initialize mock response with JSON data and status code."""
        self.json_data = json_data
        self.status_code = status_code
        self.text = str(json_data)
        self.headers = {}

    def json(self) -> dict[str, Any]:
        """Return JSON data from mock response."""
        return self.json_data

    def raise_for_status(self) -> None:
        """Raise HTTP error if status code indicates error."""
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"HTTP {self.status_code} Error")


class APITestHelper:
    """Helper class for API testing utilities."""

    @staticmethod
    def create_mock_client(base_url: str = "https://api.coingecko.com/api/v3") -> BaseAPIClient:
        """Create a BaseAPIClient for testing."""
        return BaseAPIClient(base_url=base_url, timeout_seconds=1)

    @staticmethod
    def measure_response_time(func, *args, **kwargs) -> tuple[Any, float]:
        """Measure execution time of a function."""
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        return result, end_time - start_time

    @staticmethod
    def mock_api_response(response_data: dict[str, Any], status_code: int = 200):
        """Create a mock for API responses."""
        mock_response = MockResponse(response_data, status_code)
        return patch.object(BaseAPIClient, "get", return_value=mock_response)

    @staticmethod
    def create_rate_limit_response() -> dict[str, Any]:
        """Create a rate limit error response."""
        return {"error": "Rate limit exceeded", "status_code": 429, "retry_after": 60}


class DatabaseTestHelper:
    """Helper class for database testing utilities."""

    @staticmethod
    def create_test_db_url() -> str:
        """Create test database URL."""
        return "postgresql://postgres:admin@localhost:5432/test_coincap_db"

    @staticmethod
    def assert_table_exists(engine, table_name: str) -> None:
        """Assert database table exists."""
        with engine.connect() as conn:
            query = (
                "SELECT EXISTS (SELECT FROM information_schema.tables "
                "WHERE table_name = :table_name)"
            )
            result = conn.execute(query, {"table_name": table_name})
            assert result.fetchone()[0], f"Table {table_name} does not exist"

    @staticmethod
    def count_table_rows(engine, table_name: str) -> int:
        """Count rows in database table."""
        with engine.connect() as conn:
            result = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            return result.fetchone()[0]


class TestDataManager:
    """Manage test data lifecycle."""

    def __init__(self):
        """Initialize test data manager."""
        self.created_resources: list[Any] = []

    def add_resource(self, resource: Any) -> None:
        """Track created resource for cleanup."""
        self.created_resources.append(resource)

    def cleanup(self) -> None:
        """Clean up all created resources."""
        for resource in self.created_resources:
            try:
                if hasattr(resource, "delete"):
                    resource.delete()
                elif hasattr(resource, "close"):
                    resource.close()
            except Exception:
                pass  # Ignore cleanup errors
        self.created_resources.clear()


# Context manager for test data cleanup
def with_test_data():
    """Context manager for automatic test data cleanup."""
    return TestDataManager()
