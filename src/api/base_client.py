"""Base API client with HTTP method support and exponential backoff retry logic.

This module provides a generic BaseAPIClient that can be extended for any REST API.
It supports GET, POST, PUT, DELETE methods with configurable retry logic.
"""

import logging
from collections.abc import Callable
from functools import wraps
from time import sleep
from typing import Any, TypeVar

import requests
from requests import RequestException, Response

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def exponential_backoff_retry(
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exceptions: tuple = (Exception,),
) -> Callable[[F], F]:
    """Decorator implementing exponential backoff retry strategy.

    Why exponential backoff?
    - Many APIs have rate limits (e.g., 30 req/min)
    - Exponential backoff (1s, 2s, 4s, 8s, 16s...) respects limits while minimizing downtime
    - Prevents thundering herd problems when multiple workers retry simultaneously
    - Random jitter could be added in production for better distribution
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = base_delay

            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    if attempt == max_retries:
                        logger.error(
                            "Function %s failed after %s attempts: %s",
                            func.__name__,
                            max_retries,
                            exc,
                        )
                        raise

                    logger.warning(
                        "Attempt %s/%s failed for %s: %s. Retrying in %.1fs...",
                        attempt,
                        max_retries,
                        func.__name__,
                        exc,
                        delay,
                    )
                    sleep(delay)
                    delay = min(delay * 2, max_delay)

        return wrapper  # type: ignore[return-value]

    return decorator


class BaseAPIClient:
    """Generic REST API client with retry resilience and header injection.

    Supports GET, POST, PUT, DELETE methods with:
    - Configurable base URLs
    - Dynamic header injection (e.g., Bearer tokens)
    - Exponential backoff retry logic
    - Session management for connection pooling

    This base class enables horizontal scaling by partitioning requests
    across multiple workers, each with independent client instances.
    """

    def __init__(
        self,
        base_url: str,
        timeout_seconds: int = 10,
        max_retries: int = 5,
        rate_limit_sleep_seconds: int = 20,
        default_headers: dict[str, str] | None = None,
    ):
        """Initialize the API client.

        Args:
            base_url: The API base URL (e.g., "https://api.example.com/v1").
            timeout_seconds: Request timeout in seconds.
            max_retries: Maximum retry attempts for transient failures.
            rate_limit_sleep_seconds: Seconds to sleep on 429 rate limit.
            default_headers: Default headers to include in all requests.
        """
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.rate_limit_sleep_seconds = rate_limit_sleep_seconds

        # Session for connection pooling and header persistence
        self.session = requests.Session()
        if default_headers:
            self.session.headers.update(default_headers)

    def set_auth_header(self, token: str, header_name: str = "Authorization") -> None:
        """Set authentication header (e.g., Bearer token).

        Args:
            token: The authentication token.
            header_name: The header name (default: Authorization).
        """
        self.session.headers[header_name] = f"Bearer {token}"
        logger.info("Set %s header for API client", header_name)

    def update_headers(self, headers: dict[str, str]) -> None:
        """Update session headers dynamically.

        Args:
            headers: Dictionary of headers to add/update.
        """
        self.session.headers.update(headers)

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> Response:
        """Execute HTTP request with retry logic.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE).
            endpoint: API endpoint path.
            params: Query parameters for GET requests.
            json_data: JSON payload for POST/PUT requests.
            extra_headers: Additional headers for this specific request.

        Returns:
            Response object.

        Raises:
            RequestException: If all retry attempts fail.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        last_exc: Exception | None = None
        delay = 1.0

        # Merge extra headers for this request
        headers = {}
        if extra_headers:
            headers.update(extra_headers)

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(
                    "[%s] %s (attempt %s/%s)",
                    method.upper(),
                    url,
                    attempt,
                    self.max_retries,
                )

                response = self.session.request(
                    method=method.upper(),
                    url=url,
                    params=params,
                    json=json_data,
                    headers=headers if headers else None,
                    timeout=self.timeout_seconds,
                )

                # Handle rate limiting (429)
                if response.status_code == 429:
                    retry_after_header = response.headers.get("Retry-After")
                    try:
                        retry_after = (
                            int(retry_after_header) if retry_after_header is not None else None
                        )
                    except ValueError:
                        retry_after = None

                    wait_seconds = retry_after or self.rate_limit_sleep_seconds
                    logger.warning(
                        "Received 429 (rate limit), sleeping for %s seconds before retrying",
                        wait_seconds,
                    )
                    sleep(wait_seconds)
                    last_exc = RequestException("Rate limited (429)")
                    continue

                # Handle server errors (5xx) with exponential backoff
                if 500 <= response.status_code < 600:
                    logger.warning("Server error (status %s), retrying", response.status_code)
                    last_exc = RequestException(f"Server error {response.status_code}")
                    sleep(delay)
                    delay = min(delay * 2, 60.0)
                    continue

                response.raise_for_status()
                return response

            except (requests.Timeout, requests.ConnectionError) as exc:
                logger.warning("Transient network error: %s", exc)
                last_exc = exc
                sleep(delay)
                delay = min(delay * 2, 60.0)

        assert last_exc is not None
        logger.error("Failed to complete %s request after %s attempts", method, self.max_retries)
        raise last_exc

    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Execute GET request and return JSON response.

        GET is idempotent and stateless, allowing horizontal scaling
        by partitioning requests across multiple workers.

        Args:
            endpoint: API endpoint path.
            params: Query parameters.
            extra_headers: Additional headers for this request.

        Returns:
            JSON response as dictionary.
        """
        response = self._make_request("GET", endpoint, params=params, extra_headers=extra_headers)
        return response.json()

    def post(
        self,
        endpoint: str,
        json_data: dict[str, Any],
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Execute POST request and return JSON response.

        Args:
            endpoint: API endpoint path.
            json_data: JSON payload.
            extra_headers: Additional headers for this request.

        Returns:
            JSON response as dictionary.
        """
        response = self._make_request(
            "POST", endpoint, json_data=json_data, extra_headers=extra_headers
        )
        return response.json()

    def put(
        self,
        endpoint: str,
        json_data: dict[str, Any],
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Execute PUT request and return JSON response.

        Args:
            endpoint: API endpoint path.
            json_data: JSON payload.
            extra_headers: Additional headers for this request.

        Returns:
            JSON response as dictionary.
        """
        response = self._make_request(
            "PUT", endpoint, json_data=json_data, extra_headers=extra_headers
        )
        return response.json()

    def delete(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Execute DELETE request and return JSON response.

        Args:
            endpoint: API endpoint path.
            params: Query parameters.
            extra_headers: Additional headers for this request.

        Returns:
            JSON response as dictionary.
        """
        response = self._make_request(
            "DELETE", endpoint, params=params, extra_headers=extra_headers
        )
        return response.json()
