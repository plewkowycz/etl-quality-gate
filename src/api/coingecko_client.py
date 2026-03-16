"""CoinGecko API client using the shared BaseAPIClient."""

import logging
import os

from src.api.base_client import BaseAPIClient, exponential_backoff_retry
from src.models import Asset, ValidationError

logger = logging.getLogger(__name__)


class CoinGeckoClient(BaseAPIClient):
    """API client for CoinGecko inheriting from BaseAPIClient.

    Uses CoinGecko Public API for unauthenticated access with exponential backoff
    to handle 429 rate limit errors gracefully.
    """

    def __init__(
        self,
        base_url: str = "https://api.coingecko.com/api/v3",
        timeout_seconds: int = 10,
        max_retries: int = 5,
        rate_limit_sleep_seconds: int = 20,
        api_key: str | None = None,
    ):
        """Initialize the CoinGecko client.

        Args:
            base_url: CoinGecko API base URL.
            timeout_seconds: Request timeout in seconds.
            max_retries: Maximum retry attempts for transient failures.
            rate_limit_sleep_seconds: Seconds to sleep on 429 rate limit.
            api_key: Optional CoinGecko API key for authenticated endpoints.
        """
        super().__init__(
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            rate_limit_sleep_seconds=rate_limit_sleep_seconds,
        )

        # Set authentication if API key provided
        if api_key:
            self.set_auth_header(api_key)
        elif os.getenv("COINGECKO_API_KEY"):
            self.set_auth_header(os.getenv("COINGECKO_API_KEY"))

    @exponential_backoff_retry(
        max_retries=5,
        base_delay=1.0,
        max_delay=20.0,  # Match rate_limit_sleep_seconds default
        exceptions=(Exception,),
    )
    def _fetch_markets_with_retry(self, limit: int) -> list[dict]:
        """Internal method with exponential backoff retry wrapper.

        The retry decorator handles:
        - 429 Rate Limit: Exponential backoff respects CoinGecko's 30 req/min limit
        - 5xx Server Errors: Automatic retry with increasing delays
        - Network timeouts: Graceful recovery from transient failures
        """
        logger.info("Fetching markets data from CoinGecko (limit=%s)", limit)

        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": limit,
            "page": 1,
            "sparkline": "false",
        }

        return self.get("coins/markets", params=params)

    def get_assets(self, limit: int = 10) -> list[Asset]:
        """Fetch market data from CoinGecko and return validated Asset instances.

        Args:
            limit: Maximum number of assets to fetch.

        Returns:
            List of validated Asset model instances.

        Raises:
            Exception: If all retry attempts fail.
        """
        raw_assets = self._fetch_markets_with_retry(limit)

        assets: list[Asset] = []
        for raw in raw_assets:
            mapped = {
                "id": raw.get("id"),
                "symbol": raw.get("symbol"),
                "name": raw.get("name"),
                "current_price": raw.get("current_price"),
                "market_cap": raw.get("market_cap"),
                "total_volume": raw.get("total_volume"),
            }
            try:
                assets.append(Asset(**mapped))
            except ValidationError as exc:
                logger.warning("Skipping invalid asset record from CoinGecko: %s", exc)

        logger.info("Fetched and validated %s assets from CoinGecko", len(assets))
        return assets

    def search_coins(self, query: str) -> dict[str, any]:
        """Search for coins, categories, and exchanges.

        Args:
            query: Search query string.

        Returns:
            Dictionary containing search results with coins, exchanges, categories, etc.
        """
        return self.get("search", params={"query": query})

    def get_coins_list(self) -> list[dict[str, any]]:
        """Get list of all supported coins.

        Returns:
            List of coin dictionaries with id, symbol, and name.
        """
        return self.get("coins/list")
