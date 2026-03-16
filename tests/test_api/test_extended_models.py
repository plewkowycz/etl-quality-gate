"""Tests for extended Asset model fields."""

from src.models import Asset


class TestExtendedAssetModel:
    """Tests for extended Asset model with new optional fields."""

    def test_asset_with_market_cap_rank(self, api_client):
        """Test Asset model with market_cap_rank field."""
        response = api_client.get("coins/markets", params={"vs_currency": "usd", "per_page": 10})

        # Find assets with market_cap_rank
        assets_with_rank = [asset for asset in response if asset.get("market_cap_rank")]

        if assets_with_rank:
            # Test the first asset with market_cap_rank
            asset_data = assets_with_rank[0]
            asset = Asset(**asset_data)

            assert asset.market_cap_rank is not None
            assert isinstance(asset.market_cap_rank, int)
            assert asset.market_cap_rank > 0
            assert asset.market_cap_rank <= 10000  # Reasonable upper bound

    def test_asset_with_price_change_percentage(self, api_client):
        """Test Asset model with price_change_percentage_24h field."""
        response = api_client.get("coins/markets", params={"vs_currency": "usd", "per_page": 10})

        # Find assets with price change data
        assets_with_change = [
            asset for asset in response if asset.get("price_change_percentage_24h") is not None
        ]

        if assets_with_change:
            # Test the first asset with price change
            asset_data = assets_with_change[0]
            asset = Asset(**asset_data)

            assert asset.price_change_percentage_24h is not None
            assert isinstance(asset.price_change_percentage_24h, float)
            # Price change can be positive or negative, but should be reasonable
            assert -100 <= asset.price_change_percentage_24h <= 10000  # Reasonable bounds

    def test_asset_without_optional_fields(self, api_client):
        """Test Asset model when optional fields are None or missing."""
        response = api_client.get("coins/markets", params={"vs_currency": "usd", "per_page": 5})

        # Test all assets - some may have optional fields, some may not
        for asset_data in response:
            asset = Asset(**asset_data)

            # Required fields should always be present
            assert asset.id is not None
            assert asset.symbol is not None
            assert asset.name is not None
            assert asset.current_price > 0
            assert asset.market_cap >= 0
            assert asset.total_volume >= 0

            # Optional fields can be None
            assert asset.market_cap_rank is None or isinstance(asset.market_cap_rank, int)
            assert asset.price_change_percentage_24h is None or isinstance(
                asset.price_change_percentage_24h, float
            )

    def test_extended_asset_validation(self):
        """Test validation of extended Asset model fields."""
        # Test with all fields present
        asset_data = {
            "id": "bitcoin",
            "symbol": "BTC",
            "name": "Bitcoin",
            "current_price": 50000.0,
            "market_cap": 1000000000.0,
            "total_volume": 1000000.0,
            "market_cap_rank": 1,
            "price_change_percentage_24h": 2.5,
        }

        asset = Asset(**asset_data)
        assert asset.market_cap_rank == 1
        assert asset.price_change_percentage_24h == 2.5

    def test_extended_asset_with_negative_price_change(self):
        """Test Asset model with negative price change."""
        asset_data = {
            "id": "ethereum",
            "symbol": "ETH",
            "name": "Ethereum",
            "current_price": 3000.0,
            "market_cap": 500000000.0,
            "total_volume": 500000.0,
            "market_cap_rank": 2,
            "price_change_percentage_24h": -1.5,
        }

        asset = Asset(**asset_data)
        assert asset.price_change_percentage_24h == -1.5

    def test_extended_asset_zero_optional_fields(self):
        """Test Asset model with zero values for optional fields."""
        asset_data = {
            "id": "test-coin",
            "symbol": "TEST",
            "name": "Test Coin",
            "current_price": 1.0,
            "market_cap": 0.0,
            "total_volume": 0.0,
            "market_cap_rank": 0,  # Some APIs might return 0 for unranked coins
            "price_change_percentage_24h": 0.0,
        }

        asset = Asset(**asset_data)
        assert asset.market_cap_rank == 0
        assert asset.price_change_percentage_24h == 0.0
