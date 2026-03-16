"""API client package for REST API interactions.

This package provides:
- BaseAPIClient: Generic REST client with retry logic
- CoinGeckoClient: Specialized client for CoinGecko API
- exponential_backoff_retry: Decorator for retry strategies
"""

from .base_client import BaseAPIClient, exponential_backoff_retry
from .coingecko_client import CoinGeckoClient

__all__ = ["BaseAPIClient", "exponential_backoff_retry", "CoinGeckoClient"]
