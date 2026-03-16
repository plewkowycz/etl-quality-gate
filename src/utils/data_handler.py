"""Data transformation and file handling utilities."""

import logging
import os
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.models import Asset

logger = logging.getLogger(__name__)


def validate_file_path(file_path: str, check_writable: bool = True) -> None:
    """Validate file path and ensure directory exists with proper permissions.

    Args:
        file_path: Path to validate
        check_writable: Whether to check write permissions

    Raises:
        PermissionError: If cannot write to directory
        ValueError: If path is invalid
    """
    path = Path(file_path)

    # Create directory if it doesn't exist
    path.parent.mkdir(parents=True, exist_ok=True)

    # Check write permissions
    if check_writable and not os.access(path.parent, os.W_OK):
        raise PermissionError(f"Cannot write to directory: {path.parent}")

    logger.debug("File path validated: %s", file_path)


def backup_file(file_path: str) -> str:
    """Create backup of existing file with timestamp.

    Args:
        file_path: Path to file to backup

    Returns:
        Backup file path

    Raises:
        FileNotFoundError: If original file doesn't exist
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File to backup does not exist: {file_path}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = path.with_suffix(f".{timestamp}.bak")

    path.rename(backup_path)
    logger.info("Created backup: %s", backup_path)

    return str(backup_path)


def save_assets_to_csv(assets: Iterable[Asset], file_path: str, backup: bool = True) -> str:
    """Persist a collection of Asset objects to a CSV file using pandas.

    Args:
        assets: Collection of Asset objects to save
        file_path: Output CSV file path
        backup: Whether to backup existing file

    Returns:
        Path of the written file

    Raises:
        PermissionError: If cannot write to file
        OSError: If file operation fails
    """
    asset_list: list[Asset] = list(assets)
    logger.info("Saving %s assets to CSV at %s", len(asset_list), file_path)

    # Validate file path
    validate_file_path(file_path)

    # Create backup if file exists and backup is requested
    if backup and Path(file_path).exists():
        backup_file(file_path)

    try:
        if not asset_list:
            logger.warning("No assets provided to save; writing empty CSV with schema.")
            # Create empty CSV with headers based on Asset model
            sample_asset = Asset(
                id="", symbol="", name="", current_price=0.0, market_cap=0.0, total_volume=0.0
            )
            empty_df = pd.DataFrame(columns=list(sample_asset.model_dump().keys()))
            empty_df.to_csv(file_path, index=False)
        else:
            df = pd.DataFrame([a.model_dump() for a in asset_list])
            df.to_csv(file_path, index=False)

        logger.info("Successfully wrote assets CSV to %s", file_path)
        return file_path

    except pd.errors.EmptyDataError as exc:
        logger.error("No data to save to CSV: %s", exc)
        raise
    except OSError as exc:
        logger.error("Failed to write CSV file %s: %s", file_path, exc)
        raise


def load_assets_from_csv(file_path: str) -> list[Asset]:
    """Load assets from CSV file and return Asset objects.

    Args:
        file_path: Path to CSV file

    Returns:
        List of Asset objects

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If CSV format is invalid
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"CSV file does not exist: {file_path}")

    try:
        logger.info("Loading assets from CSV: %s", file_path)
        df = pd.read_csv(file_path)

        assets = []
        for _, row in df.iterrows():
            try:
                asset = Asset(**row.to_dict())
                assets.append(asset)
            except Exception as exc:
                logger.warning("Skipping invalid asset row: %s - %s", row.to_dict(), exc)
                continue

        logger.info("Loaded %s valid assets from CSV", len(assets))
        return assets

    except pd.errors.EmptyDataError as exc:
        logger.error("CSV file is empty: %s", file_path)
        raise ValueError(f"CSV file is empty: {file_path}") from exc
    except Exception as exc:
        logger.error("Failed to load CSV file %s: %s", file_path, exc)
        raise


def filter_assets_by_price(assets: list[Asset], min_price: float) -> list[Asset]:
    """Filter assets by minimum price.

    Args:
        assets: List of assets to filter
        min_price: Minimum price threshold

    Returns:
        Filtered list of assets
    """
    filtered = [asset for asset in assets if asset.current_price >= min_price]
    logger.info("Filtered %s assets with price >= %s", len(filtered), min_price)
    return filtered


def sort_assets_by_market_cap(assets: list[Asset], descending: bool = True) -> list[Asset]:
    """Sort assets by market capitalization.

    Args:
        assets: List of assets to sort
        descending: Whether to sort in descending order

    Returns:
        Sorted list of assets
    """
    sorted_assets = sorted(assets, key=lambda x: x.market_cap or 0, reverse=descending)
    logger.info(
        "Sorted %s assets by market cap (%s)", len(sorted_assets), "desc" if descending else "asc"
    )
    return sorted_assets


def calculate_portfolio_value(assets: list[Asset]) -> float:
    """Calculate total portfolio value (sum of all current prices).

    Args:
        assets: List of assets

    Returns:
        Total portfolio value
    """
    total_value = sum(asset.current_price for asset in assets)
    logger.info("Calculated portfolio value: %s assets, total value: %s", len(assets), total_value)
    return total_value


def get_asset_statistics(assets: list[Asset]) -> dict[str, float]:
    """Calculate basic statistics for asset collection.

    Args:
        assets: List of assets

    Returns:
        Dictionary with statistics
    """
    if not assets:
        return {
            "count": 0,
            "avg_price": 0.0,
            "total_market_cap": 0.0,
            "total_volume": 0.0,
            "max_price": 0.0,
            "min_price": 0.0,
        }

    prices = [asset.current_price for asset in assets]
    market_caps = [asset.market_cap or 0 for asset in assets]
    volumes = [asset.total_volume or 0 for asset in assets]

    stats = {
        "count": len(assets),
        "avg_price": sum(prices) / len(prices),
        "total_market_cap": sum(market_caps),
        "total_volume": sum(volumes),
        "max_price": max(prices),
        "min_price": min(prices),
    }

    logger.info("Calculated statistics for %s assets", len(assets))
    return stats


def export_assets_summary(assets: list[Asset], file_path: str) -> str:
    """Export assets summary with statistics to CSV.

    Args:
        assets: List of assets
        file_path: Output file path

    Returns:
        Path of written file
    """
    validate_file_path(file_path)

    # Create summary data
    stats = get_asset_statistics(assets)
    summary_data = [
        {"metric": "Total Assets", "value": stats["count"]},
        {"metric": "Average Price", "value": round(stats["avg_price"], 2)},
        {"metric": "Total Market Cap", "value": stats["total_market_cap"]},
        {"metric": "Total Volume", "value": stats["total_volume"]},
        {"metric": "Max Price", "value": round(stats["max_price"], 2)},
        {"metric": "Min Price", "value": round(stats["min_price"], 2)},
    ]

    df = pd.DataFrame(summary_data)
    df.to_csv(file_path, index=False)

    logger.info("Exported assets summary to %s", file_path)
    return file_path
