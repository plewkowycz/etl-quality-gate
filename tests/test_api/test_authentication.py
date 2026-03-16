"""Test authentication scenarios for CoinGecko API."""


from src.api import CoinGeckoClient


class TestAuthentication:
    """Test authentication behavior and error handling."""

    def test_invalid_token_works_for_public_api(self):
        """Test invalid API token works for public endpoints (CoinGecko doesn't validate)."""
        client = CoinGeckoClient(api_key="invalid_token_12345")

        # CoinGecko public API doesn't validate tokens, so this should work
        response = client.get("ping")
        assert "gecko_says" in response

    def test_no_token_works_for_public_endpoints(self, api_client):
        """Test public endpoints work without authentication."""
        # api_client fixture should not have auth by default

        # Public endpoints should work without auth
        response = api_client.get("ping")
        assert response is not None
        assert "gecko_says" in response

    def test_auth_header_injection(self):
        """Test custom header injection works."""
        client = CoinGeckoClient()
        client.update_headers({"X-Custom-Header": "test-value"})

        # Verify header is set (we can't easily test server-side, but can check client state)
        assert "X-Custom-Header" in client.session.headers
        assert client.session.headers["X-Custom-Header"] == "test-value"

    def test_api_key_from_constructor(self):
        """Test API key can be set via constructor."""
        api_key = "test_api_key_123"
        client = CoinGeckoClient(api_key=api_key)

        # Verify auth header is set
        assert "Authorization" in client.session.headers
        assert client.session.headers["Authorization"] == f"Bearer {api_key}"
