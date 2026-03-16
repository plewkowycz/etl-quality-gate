import logging
import os

import pandas as pd
from sqlalchemy import Engine, Float, String, create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    """Base class for SQLAlchemy ORM models."""


class AssetRecord(Base):
    """ORM model for the `assets` table aligned with CoinGecko data."""

    __tablename__ = "assets"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    current_price: Mapped[float] = mapped_column(Float, nullable=False)
    market_cap: Mapped[float] = mapped_column(Float, nullable=False)
    total_volume: Mapped[float] = mapped_column(Float, nullable=False)


class DBManager:
    """Database manager for PostgreSQL connections using SQLAlchemy ORM."""

    def __init__(self, db_url: str | None = None):
        """Initialize database manager with connection URL.

        Args:
            db_url: PostgreSQL connection string. If None, uses DB_URL env var or default.
        """
        if db_url is None:
            db_url = os.getenv("DB_URL", "postgresql://postgres:admin@localhost:5432/coingecko_db")

        self.db_url = db_url
        self.engine: Engine | None = None

    def connect(self) -> Engine:
        """Create and return SQLAlchemy engine."""
        if self.engine is None:
            logger.info("Creating SQLAlchemy engine for %s", self.db_url)
            self.engine = create_engine(self.db_url)
        return self.engine

    def create_tables(self) -> None:
        """Create database tables for the ELT pipeline using ORM metadata."""
        engine = self.connect()
        logger.info("Creating tables in PostgreSQL database")
        Base.metadata.create_all(engine)
        logger.info("Tables created (if not already existing)")

    # CRUD Operations
    def get_asset_by_id(self, asset_id: str) -> AssetRecord | None:
        """Get asset by ID.

        Args:
            asset_id: The asset ID to retrieve.

        Returns:
            AssetRecord if found, None otherwise.
        """
        engine = self.connect()
        with Session(engine) as session:
            return session.get(AssetRecord, asset_id)

    def get_all_assets(self) -> list[AssetRecord]:
        """Get all assets from database.

        Returns:
            List of all AssetRecord objects.
        """
        engine = self.connect()
        with Session(engine) as session:
            return session.query(AssetRecord).all()

    def create_asset(self, asset_data: dict[str, any]) -> AssetRecord:
        """Create a new asset record.

        Args:
            asset_data: Dictionary containing asset data.

        Returns:
            Created AssetRecord object.

        Raises:
            SQLAlchemyError: If creation fails.
        """
        engine = self.connect()
        try:
            with Session(engine) as session:
                asset = AssetRecord(**asset_data)
                session.add(asset)
                session.commit()
                session.refresh(asset)
                logger.info("Created asset: %s", asset.id)
                return asset
        except SQLAlchemyError as exc:
            logger.error("Failed to create asset: %s", exc)
            raise

    def update_asset(self, asset_id: str, **kwargs) -> AssetRecord | None:
        """Update asset fields.

        Args:
            asset_id: The asset ID to update.
            **kwargs: Fields to update.

        Returns:
            Updated AssetRecord if found, None otherwise.
        """
        engine = self.connect()
        try:
            with Session(engine) as session:
                asset = session.get(AssetRecord, asset_id)
                if asset:
                    for key, value in kwargs.items():
                        if hasattr(asset, key):
                            setattr(asset, key, value)
                    session.commit()
                    session.refresh(asset)
                    logger.info("Updated asset: %s", asset_id)
                return asset
        except SQLAlchemyError as exc:
            logger.error("Failed to update asset %s: %s", asset_id, exc)
            raise

    def delete_asset(self, asset_id: str) -> bool:
        """Delete asset by ID.

        Args:
            asset_id: The asset ID to delete.

        Returns:
            True if deleted, False if not found.
        """
        engine = self.connect()
        try:
            with Session(engine) as session:
                asset = session.get(AssetRecord, asset_id)
                if asset:
                    session.delete(asset)
                    session.commit()
                    logger.info("Deleted asset: %s", asset_id)
                    return True
                return False
        except SQLAlchemyError as exc:
            logger.error("Failed to delete asset %s: %s", asset_id, exc)
            raise

    def get_assets_by_symbol(self, symbol: str) -> list[AssetRecord]:
        """Get assets by symbol (case-insensitive).

        Args:
            symbol: The symbol to search for.

        Returns:
            List of matching AssetRecord objects.
        """
        engine = self.connect()
        with Session(engine) as session:
            return session.query(AssetRecord).filter(AssetRecord.symbol.ilike(symbol)).all()

    def get_top_assets_by_market_cap(self, limit: int = 10) -> list[AssetRecord]:
        """Get top assets by market capitalization.

        Args:
            limit: Maximum number of assets to return.

        Returns:
            List of top AssetRecord objects.
        """
        engine = self.connect()
        with Session(engine) as session:
            return (
                session.query(AssetRecord)
                .order_by(AssetRecord.market_cap.desc())
                .limit(limit)
                .all()
            )

    def count_assets(self) -> int:
        """Count total number of assets.

        Returns:
            Total number of assets in database.
        """
        engine = self.connect()
        with Session(engine) as session:
            return session.query(AssetRecord).count()

    def load_csv_to_db(self, file_path: str) -> None:
        """Load asset data from CSV into the Postgres `assets` table."""
        engine = self.connect()
        logger.info("Loading CSV %s into database table 'assets'", file_path)

        try:
            df = pd.read_csv(file_path)

            # Map CSV columns explicitly to ORM fields to guard against schema drift.
            records: list[AssetRecord] = []
            for _, row in df.iterrows():
                record = AssetRecord(
                    id=str(row["id"]),
                    symbol=str(row["symbol"]),
                    name=str(row["name"]),
                    current_price=float(row["current_price"]),
                    market_cap=float(row["market_cap"]),
                    total_volume=float(row["total_volume"]),
                )
                records.append(record)

            with Session(engine) as session:
                session.query(AssetRecord).delete()
                session.add_all(records)
                session.commit()

            logger.info("Loaded %s records into 'assets' table", len(records))
        except Exception as exc:
            logger.error("Failed to load CSV into database: %s", exc)
            raise

    def fetch_top_assets(self, limit: int = 3) -> list[tuple]:
        """Return top assets ordered by market cap for smoke testing output."""
        engine = self.connect()
        query = text(
            "SELECT id, symbol, name, current_price, market_cap "
            "FROM assets "
            "ORDER BY market_cap DESC "
            "LIMIT :limit"
        )
        with engine.connect() as conn:
            rows = conn.execute(query, {"limit": limit}).fetchall()
        return [tuple(row) for row in rows]

    def close(self) -> None:
        """Close database connections and clean up resources."""
        if self.engine:
            logger.info("Closing database connection")
            self.engine.dispose()
            self.engine = None

    def health_check(self) -> bool:
        """Check if database connection is healthy."""
        try:
            engine = self.connect()
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as exc:
            logger.error("Database health check failed: %s", exc)
            return False

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with automatic cleanup."""
        self.close()
