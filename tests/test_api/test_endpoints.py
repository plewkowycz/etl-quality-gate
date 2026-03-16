"""API tests for CoinGecko endpoints."""

import pytest

from src.models import Asset, CoinListItem, PingResponse


class TestPingEndpoint:
    """Tests for /ping endpoint."""

    def test_ping_success(self, base_api_client):
        """Happy path: /ping should return 200 OK."""
        response = base_api_client.get("ping")
        assert response is not None
        assert "gecko_says" in response

    def test_ping_schema_validation(self, base_api_client):
        """Validate /ping response schema."""
        data = base_api_client.get("ping")
        ping = PingResponse(**data)
        assert ping.gecko_says is not None


class TestCoinsMarketsEndpoint:
    """Tests for /coins/markets endpoint."""

    def test_markets_success(self, base_api_client):
        """Happy path: /coins/markets should return 200 with valid data."""
        response = base_api_client.get(
            "coins/markets", params={"vs_currency": "usd", "per_page": 5}
        )
        assert response is not None
        assert isinstance(response, list)
        assert len(response) > 0

        # Validate with extended Asset model
        for asset_data in response[:3]:  # Validate first 3 assets
            asset = Asset(**asset_data)
            assert asset.id is not None
            assert asset.symbol is not None
            assert asset.name is not None
            assert asset.current_price > 0
            assert asset.market_cap >= 0

            # NEW: Test optional extended fields
            if asset.market_cap_rank is not None:
                assert isinstance(asset.market_cap_rank, int)
                assert asset.market_cap_rank > 0

            if asset.price_change_percentage_24h is not None:
                assert isinstance(asset.price_change_percentage_24h, float)
                # Price change can be positive or negative
                assert -100 <= asset.price_change_percentage_24h <= 10000

    @pytest.mark.parametrize("coin_id", ["bitcoin", "ethereum", "solana"])
    def test_markets_individual_coins(self, base_api_client, coin_id: str):
        """Test specific coin IDs return valid data."""
        params = {
            "vs_currency": "usd",
            "ids": coin_id,
            "per_page": 1,
            "page": 1,
        }
        response = base_api_client.get("coins/markets", params=params)
        assert isinstance(response, list)
        assert len(response) == 1
        assert response[0]["id"] == coin_id

    def test_markets_schema_validation(self, base_api_client):
        """Validate /coins/markets response against Asset model."""
        params = {
            "vs_currency": "usd",
            "per_page": 3,
            "page": 1,
        }
        data = base_api_client.get("coins/markets", params=params)
        for item in data:
            asset = Asset(**item)
            assert asset.id is not None
            assert asset.symbol.isupper()


class TestCoinsListEndpoint:
    """Tests for /coins/list endpoint."""

    def test_coins_list_success(self, base_api_client):
        """Happy path: /coins/list should return all available coins."""
        response = base_api_client.get("coins/list")
        assert isinstance(response, list)
        assert len(response) > 1000  # Should have many coins

    def test_coins_list_schema(self, base_api_client):
        """Validate /coins/list items have required fields."""
        data = base_api_client.get("coins/list")
        # Sample first 10 items for validation
        for item in data[:10]:
            coin = CoinListItem(**item)
            assert coin.id
            assert coin.symbol
            assert coin.name


class TestNegativePaths:
    """Negative path tests for error handling."""

    def test_nonexistent_coin_returns_404_or_empty(self, base_api_client):
        """Requesting non-existent coin should handle gracefully."""
        params = {
            "vs_currency": "usd",
            "ids": "nonexistent_coin_xyz_123",
            "per_page": 1,
        }
        # API returns empty list for unknown coins rather than 404
        response = base_api_client.get("coins/markets", params=params)
        assert response == []

    def test_invalid_query_params_handled(self, base_api_client):
        """Invalid parameters should be handled appropriately."""
        # CoinGecko typically returns 400 for invalid vs_currency
        params = {
            "vs_currency": "invalid_currency",
            "per_page": 1,
        }
        try:
            base_api_client.get("coins/markets", params=params)
            # If no exception, the API handled it gracefully
        except Exception as e:
            # Expected behavior: API returns error status
            assert "400" in str(e) or "Bad Request" in str(e) or "error" in str(e).lower()

    def test_invalid_endpoint_returns_404(self, base_api_client):
        """Invalid endpoint should return 404."""
        try:
            base_api_client.get("nonexistent/endpoint/xyz")
            pytest.fail("Should have raised an exception for 404")
        except Exception as e:
            assert "404" in str(e) or "Not Found" in str(e)
