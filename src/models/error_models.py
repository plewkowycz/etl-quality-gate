"""Error response models for CoinGecko API."""

from pydantic import BaseModel, Field
from pydantic.config import ConfigDict


class CoinGeckoError(BaseModel):
    """Model for CoinGecko API error responses."""

    error: str
    status_code: int = Field(ge=400, le=599)

    model_config = ConfigDict(populate_by_name=True)


class RateLimitError(CoinGeckoError):
    """Model for rate limit error responses."""

    error: str = Field(default="Rate limit exceeded")
    status_code: int = Field(default=429)

    retry_after: int | None = Field(default=None, description="Seconds to wait before retrying")


class AuthenticationError(CoinGeckoError):
    """Model for authentication error responses."""

    error: str = Field(default="Invalid API key")
    status_code: int = Field(default=401)
