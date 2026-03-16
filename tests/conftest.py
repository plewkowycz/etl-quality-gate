"""Shared pytest fixtures for all tests."""

import os
import sys

from dotenv import load_dotenv
from sqlalchemy import text

# Add src to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

import pytest

from src.api import BaseAPIClient, CoinGeckoClient
from src.db import DBManager

# Load environment variables from .env file if it exists
load_dotenv()

EXTRACTION_LIMIT = int(os.getenv("EXTRACTION_LIMIT", "5"))


@pytest.fixture(scope="function")
def test_asset_data():
    """Provide test asset data for CRUD operations."""
    return {
        "id": "test-bitcoin",
        "symbol": "BTC",
        "name": "Bitcoin",
        "current_price": 50000.0,
        "market_cap": 1000000000.0,
        "total_volume": 1000000.0,
    }


@pytest.fixture(scope="function")
def clean_db_manager(db_manager):
    """Provide clean database for each test."""
    # Clean up before test
    engine = db_manager.connect()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM assets"))

    yield db_manager

    # Clean up after test
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM assets"))


@pytest.fixture(scope="session")
def base_api_client():
    """Provide a shared BaseAPIClient instance for API tests."""
    return BaseAPIClient(base_url="https://api.coingecko.com/api/v3")


@pytest.fixture(scope="session")
def api_client():
    """Provide a shared CoinGecko API client instance for tests."""
    return CoinGeckoClient()


@pytest.fixture(scope="session")
def db_manager():
    """Provide a shared DBManager and ensure tables exist."""
    db_url = os.getenv("DB_URL", "postgresql://postgres:admin@localhost:5432/coingecko_db")
    manager = DBManager(db_url=db_url)
    manager.create_tables()
    return manager
