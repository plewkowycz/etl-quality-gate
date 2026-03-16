"""Tests for database CRUD operations."""


class TestDatabaseCRUD:
    """Test database CRUD operations."""

    def test_create_asset(self, clean_db_manager):
        """Test creating a new asset record."""
        asset_data = {
            "id": "test-bitcoin",
            "symbol": "BTC",
            "name": "Bitcoin",
            "current_price": 50000.0,
            "market_cap": 1000000000.0,
            "total_volume": 1000000.0,
        }

        created = clean_db_manager.create_asset(asset_data)

        assert created.id == "test-bitcoin"
        assert created.symbol == "BTC"
        assert created.name == "Bitcoin"
        assert created.current_price == 50000.0
        assert created.market_cap == 1000000000.0
        assert created.total_volume == 1000000.0

    def test_get_asset_by_id(self, clean_db_manager, test_asset_data):
        """Test retrieving asset by ID."""
        # Create asset first
        clean_db_manager.create_asset(test_asset_data)

        # Retrieve by ID
        retrieved = clean_db_manager.get_asset_by_id(test_asset_data["id"])
        assert retrieved is not None
        assert retrieved.id == test_asset_data["id"]
        assert retrieved.symbol == test_asset_data["symbol"]
        assert retrieved.name == test_asset_data["name"]

    def test_get_nonexistent_asset(self, clean_db_manager):
        """Test retrieving non-existent asset returns None."""
        retrieved = clean_db_manager.get_asset_by_id("non-existent-id")
        assert retrieved is None

    def test_get_all_assets(self, clean_db_manager):
        """Test retrieving all assets."""
        # Create multiple assets
        assets_data = [
            {
                "id": "btc",
                "symbol": "BTC",
                "name": "Bitcoin",
                "current_price": 50000.0,
                "market_cap": 1000000000.0,
                "total_volume": 1000000.0,
            },
            {
                "id": "eth",
                "symbol": "ETH",
                "name": "Ethereum",
                "current_price": 3000.0,
                "market_cap": 500000000.0,
                "total_volume": 500000.0,
            },
        ]

        for asset_data in assets_data:
            clean_db_manager.create_asset(asset_data)

        # Retrieve all assets
        all_assets = clean_db_manager.get_all_assets()

        assert len(all_assets) >= 2
        asset_ids = [asset.id for asset in all_assets]
        assert "btc" in asset_ids
        assert "eth" in asset_ids

    def test_update_asset(self, clean_db_manager, test_asset_data):
        """Test updating asset fields."""
        # Create asset first
        clean_db_manager.create_asset(test_asset_data)

        # Update price
        updated = clean_db_manager.update_asset(
            test_asset_data["id"], current_price=55000.0
        )
        assert updated is not None
        assert updated.current_price == 55000.0
        assert updated.symbol == test_asset_data["symbol"]  # Other fields unchanged

    def test_update_nonexistent_asset(self, clean_db_manager):
        """Test updating non-existent asset returns None."""
        result = clean_db_manager.update_asset("non-existent-id", current_price=1000.0)
        assert result is None

    def test_delete_asset(self, clean_db_manager, test_asset_data):
        """Test deleting asset by ID."""
        # Create asset first
        created = clean_db_manager.create_asset(test_asset_data)
        assert created.id == test_asset_data["id"]

        # Delete asset
        deleted = clean_db_manager.delete_asset(test_asset_data["id"])
        assert deleted is True

        # Verify deletion
        found = clean_db_manager.get_asset_by_id(test_asset_data["id"])
        assert found is None

    def test_delete_nonexistent_asset(self, clean_db_manager):
        """Test deleting non-existent asset returns False."""
        result = clean_db_manager.delete_asset("non-existent-id")
        assert result is False

    def test_get_assets_by_symbol(self, clean_db_manager):
        """Test searching assets by symbol."""
        # Create assets with same symbol (case-insensitive test)
        assets_data = [
            {
                "id": "btc1",
                "symbol": "BTC",
                "name": "Bitcoin",
                "current_price": 50000.0,
                "market_cap": 1000000000.0,
                "total_volume": 1000000.0,
            },
            {
                "id": "btc2",
                "symbol": "btc",
                "name": "Bitcoin Test",
                "current_price": 49000.0,
                "market_cap": 900000000.0,
                "total_volume": 900000.0,
            },
            {
                "id": "eth",
                "symbol": "ETH",
                "name": "Ethereum",
                "current_price": 3000.0,
                "market_cap": 500000000.0,
                "total_volume": 500000.0,
            },
        ]

        for asset_data in assets_data:
            clean_db_manager.create_asset(asset_data)

        # Search by symbol (case-insensitive)
        btc_assets = clean_db_manager.get_assets_by_symbol("BTC")
        eth_assets = clean_db_manager.get_assets_by_symbol("eth")

        assert len(btc_assets) == 2  # Should find both BTC and btc
        assert len(eth_assets) == 1

        # Verify all returned assets have correct symbol
        for asset in btc_assets:
            assert asset.symbol.upper() == "BTC"
        for asset in eth_assets:
            assert asset.symbol.upper() == "ETH"

    def test_get_top_assets_by_market_cap(self, clean_db_manager):
        """Test sorting assets by market capitalization."""
        # Create assets with different market caps
        assets_data = [
            {
                "id": "large",
                "symbol": "LARGE",
                "name": "Large Cap",
                "current_price": 1000.0,
                "market_cap": 1000000000.0,
                "total_volume": 100000.0,
            },
            {
                "id": "medium",
                "symbol": "MEDIUM",
                "name": "Medium Cap",
                "current_price": 500.0,
                "market_cap": 500000000.0,
                "total_volume": 50000.0,
            },
            {
                "id": "small",
                "symbol": "SMALL",
                "name": "Small Cap",
                "current_price": 100.0,
                "market_cap": 100000000.0,
                "total_volume": 10000.0,
            },
        ]

        for asset_data in assets_data:
            clean_db_manager.create_asset(asset_data)

        # Get top 2 assets by market cap
        top_assets = clean_db_manager.get_top_assets_by_market_cap(limit=2)

        assert len(top_assets) == 2

        # Verify sorting (descending by market cap)
        assert top_assets[0].market_cap >= top_assets[1].market_cap

        # Verify the largest is included
        asset_ids = [asset.id for asset in top_assets]
        assert "large" in asset_ids

    def test_count_assets(self, clean_db_manager):
        """Test counting total number of assets."""
        # Initially should be empty
        assert clean_db_manager.count_assets() == 0

        # Add some assets
        assets_data = [
            {
                "id": "btc",
                "symbol": "BTC",
                "name": "Bitcoin",
                "current_price": 50000.0,
                "market_cap": 1000000000.0,
                "total_volume": 1000000.0,
            },
            {
                "id": "eth",
                "symbol": "ETH",
                "name": "Ethereum",
                "current_price": 3000.0,
                "market_cap": 500000000.0,
                "total_volume": 500000.0,
            },
        ]

        for asset_data in assets_data:
            clean_db_manager.create_asset(asset_data)

        # Count should be 2
        assert clean_db_manager.count_assets() == 2

    def test_health_check(self, clean_db_manager):
        """Test database health check."""
        is_healthy = clean_db_manager.health_check()
        assert is_healthy is True

    def test_context_manager(self, clean_db_manager):
        """Test database manager as context manager."""
        with clean_db_manager as db:
            # Should work normally
            assert db is not None
            assert db.health_check() is True

        # Connection should be properly closed
        # Note: In tests, we reuse the same session, so this mainly tests the interface
