"""Pydantic models for CoinGecko API responses."""

from typing import Any

from pydantic import BaseModel, Field, ValidationError, field_validator
from pydantic.config import ConfigDict

from .error_models import AuthenticationError, CoinGeckoError, RateLimitError


class Asset(BaseModel):
    """Asset representation aligned with CoinGecko markets API.

    Uses CoinGecko Public API for unauthenticated access.
    """

    id: str
    symbol: str
    name: str
    current_price: float = Field(gt=0)
    market_cap: float = Field(ge=0)
    total_volume: float = Field(ge=0)
    market_cap_rank: int | None = Field(default=None)
    price_change_percentage_24h: float | None = Field(default=None)

    @field_validator("current_price", "market_cap", "total_volume", mode="before")
    @classmethod
    def _coerce_float(cls, v: Any) -> float:
        """Coerce numeric-like values into floats (CoinGecko returns real numbers)."""
        return float(v)

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol(cls, v: str) -> str:
        """Ensure asset symbols are always stored uppercased."""
        if not v:
            raise ValueError("symbol must be non-empty")
        return v.upper()


class CoinListItem(BaseModel):
    """Model for /coins/list endpoint response."""

    id: str
    symbol: str
    name: str


class PingResponse(BaseModel):
    """Model for /ping endpoint response."""

    gecko_says: str = Field(alias="gecko_says")

    model_config = ConfigDict(populate_by_name=True)


class SearchResponse(BaseModel):
    """Model for /search endpoint response."""

    coins: list[dict[str, Any]]
    exchanges: list[dict[str, Any]]
    categories: list[dict[str, Any]]
    nfts: list[dict[str, Any]]
    icos: list[dict[str, Any]]


__all__ = [
    "Asset",
    "CoinListItem",
    "PingResponse",
    "ValidationError",
    "CoinGeckoError",
    "RateLimitError",
    "AuthenticationError",
    "SearchResponse",
]
