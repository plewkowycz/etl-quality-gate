"""Tests for /coins/list endpoint."""

from src.models import CoinListItem


class TestCoinsListEndpoint:
    """Tests for /coins/list endpoint."""

    def test_coins_list_success(self, api_client):
        """Happy path: get_coins_list should return 200 with valid data."""
        response = api_client.get_coins_list()
        assert response is not None
        assert isinstance(response, list)
        assert len(response) > 0

    def test_coins_list_schema_validation(self, api_client):
        """Validate /coins/list response schema."""
        response = api_client.get_coins_list()

        # Validate first few items
        for item in response[:5]:  # Validate first 5 items
            coin = CoinListItem(**item)
            assert coin.id is not None
            assert coin.symbol is not None
            assert coin.name is not None

    def test_coins_list_structure(self, api_client):
        """Test /coins/list has expected structure."""
        response = api_client.get_coins_list()

        # Verify it's a list of dictionaries
        assert isinstance(response, list)
        assert len(response) > 0

        # Check first item structure
        first_item = response[0]
        required_fields = ["id", "symbol", "name"]
        for field in required_fields:
            assert field in first_item, f"Missing required field: {field}"
            assert first_item[field] is not None, f"Field {field} is None"

    def test_coins_list_popular_coins(self, api_client):
        """Test that popular coins are in the list."""
        response = api_client.get_coins_list()
        coin_ids = [coin["id"] for coin in response]

        # Check for well-known cryptocurrencies
        popular_coins = ["bitcoin", "ethereum", "litecoin", "ripple"]
        for coin in popular_coins:
            assert coin in coin_ids, f"Popular coin {coin} not found in list"

    def test_coins_list_no_duplicates(self, api_client):
        """Test that /coins/list doesn't contain duplicate IDs."""
        response = api_client.get_coins_list()
        coin_ids = [coin["id"] for coin in response]
        unique_ids = set(coin_ids)

        assert len(coin_ids) == len(unique_ids), "Duplicate coin IDs found in list"

    def test_coins_list_symbol_format(self, api_client):
        """Test that coin symbols follow expected format."""
        response = api_client.get_coins_list()

        # Check first 10 coins for basic format requirements
        for coin in response[:10]:
            symbol = coin["symbol"]
            assert isinstance(symbol, str), f"Symbol should be string, got {type(symbol)}"
            assert len(symbol) > 0, "Symbol should not be empty"
            # Most symbols are alphanumeric, but some coins have special characters
            # We just verify it's a valid string format
            assert symbol.strip() == symbol, (
                f"Symbol {symbol} should not have leading/trailing whitespace"
            )
