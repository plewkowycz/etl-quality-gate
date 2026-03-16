"""Test search endpoint functionality."""

from src.models import SearchResponse


class TestSearchEndpoint:
    """Tests for /search endpoint."""

    def test_search_coins_success(self, api_client):
        """Happy path: search_coins should return coin results."""
        response = api_client.search_coins("bitcoin")
        assert response is not None
        assert "coins" in response
        assert len(response["coins"]) > 0

        # Verify Bitcoin is in results
        bitcoin_found = any(coin["id"] == "bitcoin" for coin in response["coins"])
        assert bitcoin_found, "Bitcoin should be found in search results"

    def test_search_categories_success(self, api_client):
        """Test search returns categories."""
        response = api_client.search_coins("ethereum")
        assert response is not None
        assert "categories" in response
        assert len(response["categories"]) > 0

    def test_search_empty_query(self, api_client):
        """Test search with empty query."""
        response = api_client.search_coins("")
        assert response is not None
        # Should return empty results for empty query
        assert "coins" in response
        assert len(response["coins"]) == 0

    def test_search_nonexistent_coin(self, api_client):
        """Test search for non-existent coin."""
        response = api_client.search_coins("nonexistentcoinxyz123")
        assert response is not None
        assert "coins" in response
        assert len(response["coins"]) == 0

    def test_search_response_structure(self, api_client):
        """Test search response has expected structure."""
        response = api_client.search_coins("ethereum")

        # Validate response structure with SearchResponse model
        search_response = SearchResponse(**response)

        # Verify all expected sections exist
        assert hasattr(search_response, "coins")
        assert hasattr(search_response, "exchanges")
        assert hasattr(search_response, "categories")
        assert hasattr(search_response, "nfts")
        assert hasattr(search_response, "icos")

        # Verify coin structure
        if search_response.coins:
            coin = search_response.coins[0]
            coin_fields = [
                "id",
                "name",
                "api_symbol",
                "symbol",
                "market_cap_rank",
                "thumb",
                "large",
            ]
            for field in coin_fields:
                assert field in coin, f"Coin missing expected field: {field}"
