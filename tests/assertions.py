"""Custom assertions for test validation."""

from typing import Any

from src.models import Asset, CoinListItem


class APIAssertions:
    """Custom assertions for API testing."""

    @staticmethod
    def assert_valid_asset(asset: Asset) -> None:
        """Assert Asset has valid structure and values."""
        assert isinstance(asset.id, str) and len(asset.id) > 0, "Asset ID must be non-empty string"
        assert isinstance(asset.symbol, str) and len(asset.symbol) > 0, (
            "Asset symbol must be non-empty string"
        )
        assert asset.symbol == asset.symbol.upper(), "Asset symbol should be uppercase"
        assert isinstance(asset.name, str) and len(asset.name) > 0, (
            "Asset name must be non-empty string"
        )
        assert isinstance(asset.current_price, float) and asset.current_price > 0, (
            "Asset price must be positive float"
        )
        assert isinstance(asset.market_cap, float) and asset.market_cap >= 0, (
            "Asset market cap must be non-negative float"
        )
        assert isinstance(asset.total_volume, float) and asset.total_volume >= 0, (
            "Asset volume must be non-negative float"
        )

    @staticmethod
    def assert_valid_coin_list_item(item: CoinListItem) -> None:
        """Assert CoinListItem has valid structure."""
        assert isinstance(item.id, str) and len(item.id) > 0, "Coin ID must be non-empty string"
        assert isinstance(item.symbol, str) and len(item.symbol) > 0, (
            "Coin symbol must be non-empty string"
        )
        assert isinstance(item.name, str) and len(item.name) > 0, (
            "Coin name must be non-empty string"
        )

    @staticmethod
    def assert_assets_sorted_by_market_cap(assets: list[Asset]) -> None:
        """Assert assets are sorted by market cap in descending order."""
        for i in range(len(assets) - 1):
            current_cap = assets[i].market_cap
            next_cap = assets[i + 1].market_cap
            assert current_cap >= next_cap, (
                f"Assets not sorted by market cap: {current_cap} < {next_cap}"
            )

    @staticmethod
    def assert_response_contains_required_fields(
        response: dict[str, Any], required_fields: list[str]
    ) -> None:
        """Assert response contains all required fields."""
        for field in required_fields:
            assert field in response, f"Response missing required field: {field}"
            assert response[field] is not None, f"Response field {field} is None"

    @staticmethod
    def assert_error_response_structure(error_response: dict[str, Any]) -> None:
        """Assert error response has expected structure."""
        assert "error" in error_response, "Error response missing 'error' field"
        assert "status_code" in error_response, "Error response missing 'status_code' field"
        assert isinstance(error_response["status_code"], int), "Status code must be integer"
        assert 400 <= error_response["status_code"] <= 599, (
            "Status code must be in HTTP error range"
        )


class PerformanceAssertions:
    """Custom assertions for performance testing."""

    @staticmethod
    def assert_response_time(response_time: float, max_seconds: float = 5.0) -> None:
        """Assert response time is within acceptable limits."""
        assert response_time <= max_seconds, (
            f"Response time {response_time}s exceeds limit {max_seconds}s"
        )

    @staticmethod
    def assert_response_size(response_size: int, max_bytes: int = 1024 * 1024) -> None:
        """Assert response size is within acceptable limits."""
        assert response_size <= max_bytes, (
            f"Response size {response_size} bytes exceeds limit {max_bytes}"
        )
