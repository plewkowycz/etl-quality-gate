"""End-to-end tests for the CoinGecko-based ELT pipeline."""

from collections.abc import Generator

import pytest
import requests
from sqlalchemy import text

from src.api.coingecko_client import CoinGeckoClient
from src.db import DBManager
from src.models import Asset
from src.utils import save_assets_to_csv
from tests.conftest import EXTRACTION_LIMIT


@pytest.fixture(scope="function")
def populated_db(
    api_client: CoinGeckoClient,
    db_manager: DBManager,
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[tuple[DBManager, int], None, None]:
    """Run a full Extract → CSV → Load cycle and yield a populated DB.

    Ensures each test using this fixture gets a fresh snapshot of asset data.
    """
    assets = api_client.get_assets(limit=EXTRACTION_LIMIT)
    tmp_dir = tmp_path_factory.mktemp("elt_data")
    csv_path = tmp_dir / "assets.csv"
    save_assets_to_csv(assets, str(csv_path))
    db_manager.load_csv_to_db(str(csv_path))
    yield db_manager, EXTRACTION_LIMIT


@pytest.mark.parametrize("coin_id", ["bitcoin", "ethereum", "solana"])
def test_api_health(api_client, coin_id: str) -> None:
    """Ensure CoinGecko markets endpoint is healthy for key coins."""
    # Use the client's base URL/session for a lightweight health check.
    url = f"{api_client.base_url}/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": coin_id,
        "order": "market_cap_desc",
        "per_page": 1,
        "page": 1,
        "sparkline": "false",
    }
    response = requests.get(url, params=params, timeout=10)
    assert response.status_code == 200, (
        f"CoinGecko returned {response.status_code} for coin_id={coin_id}, expected 200"
    )


def test_schema_integrity(api_client) -> None:
    """Verify CoinGecko payloads correctly initialize the Asset model."""
    assets = api_client.get_assets(limit=1)
    assert len(assets) == 1, f"Expected exactly 1 asset, got {len(assets)}"

    asset = assets[0]
    assert isinstance(asset, Asset), f"Expected Asset instance, got {type(asset)}"
    assert isinstance(asset.id, str), "Asset.id should be a string"
    assert isinstance(asset.symbol, str), "Asset.symbol should be a string"
    assert asset.symbol == asset.symbol.upper(), "Asset.symbol should be uppercased by validator"
    assert isinstance(asset.name, str), "Asset.name should be a string"
    assert isinstance(asset.current_price, float), "Asset.current_price should be a float"
    assert isinstance(asset.market_cap, float), "Asset.market_cap should be a float"
    assert isinstance(asset.total_volume, float), "Asset.total_volume should be a float"


def test_data_persistence(populated_db: tuple[DBManager, int]) -> None:
    """Ensure the number of DB records matches the extraction limit."""
    db_manager, extraction_limit = populated_db
    engine = db_manager.connect()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM assets"))
        count = result.scalar_one()

    assert count == extraction_limit == EXTRACTION_LIMIT, (
        f"Expected {EXTRACTION_LIMIT} rows in assets table, found {count}"
    )


def test_data_reconciliation(api_client, populated_db: tuple[DBManager, int]) -> None:
    """Compare Bitcoin price between live API and DB to validate the quality gate."""
    # Get live price for bitcoin from API using the resilient client.
    assets = api_client.get_assets(limit=5)
    by_id = {a.id: a for a in assets}
    assert "bitcoin" in by_id, "Bitcoin not present in CoinGecko get_assets response"
    api_price = by_id["bitcoin"].current_price

    # Get price for bitcoin from DB.
    db_manager, _ = populated_db
    engine = db_manager.connect()
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT current_price FROM assets WHERE id = :coin_id"),
            {"coin_id": "bitcoin"},
        )
        row = result.fetchone()

    assert row is not None, "Bitcoin record not found in database 'assets' table"
    db_price = float(row[0])

    # Allow 1% tolerance for price fluctuation between API calls
    price_diff_pct = abs(api_price - db_price) / api_price * 100
    assert price_diff_pct < 1.0, (
        f"API price ${api_price:,.2f} vs DB price ${db_price:,.2f} "
        f"differs by {price_diff_pct:.2f}% (tolerance: 1%)"
    )
