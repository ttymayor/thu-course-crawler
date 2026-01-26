"""
Centralized configuration module for the THU Course Crawler.

This module loads and validates all environment variables in one place,
providing type-safe access with proper defaults.
"""

import os
from dataclasses import dataclass
from typing import Literal

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class Config:
    """Application configuration loaded from environment variables."""

    # Database Configuration
    db_name: str
    db_uri: str
    db_env: Literal["dev", "prod"] = "prod"

    # Academic Configuration
    academic_year: str = "114"
    academic_semester: str = "2"

    # Development Configuration
    dev_data_limit: int = 10

    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.db_name:
            raise ValueError("DB_NAME must be set in .env file")
        if not self.db_uri:
            raise ValueError("DB_URI must be set in .env file")

        # Validate db_env
        if self.db_env not in ("dev", "prod"):
            raise ValueError(f"DB_ENV must be 'dev' or 'prod', got: {self.db_env}")

    def get_collection_name(self, base_name: str) -> str:
        """
        Get collection name based on environment.

        Args:
            base_name: Base name of the collection

        Returns:
            Collection name with environment suffix if in dev mode
        """
        if self.db_env == "dev":
            return f"{base_name}_dev"
        return base_name


def load_config() -> Config:
    """
    Load configuration from environment variables.

    Returns:
        Config instance with all settings

    Raises:
        ValueError: If required environment variables are missing
    """
    return Config(
        db_name=os.getenv("DB_NAME", ""),
        db_uri=os.getenv("DB_URI", ""),
        db_env=os.getenv("DB_ENV", "prod"),  # type: ignore
        academic_year=os.getenv("ACADEMIC_YEAR", "114"),
        academic_semester=os.getenv("ACADEMIC_SEMESTER", "2"),
        dev_data_limit=int(os.getenv("DEV_DATA_LIMIT", "10")),
    )


# Global configuration instance
config = load_config()
