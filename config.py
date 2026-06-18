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
    academic_year: str = "115"
    academic_semester: str = "1"
    academic_terms: tuple[tuple[str, str], ...] = ()
    refresh_all_terms: bool = False

    # Development Configuration
    dev_data_limit: int = 10
    concurrency_limit: int = 3

    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.db_name:
            raise ValueError("DB_NAME must be set in .env file")
        if not self.db_uri:
            raise ValueError("DB_URI must be set in .env file")

        # Validate db_env
        if self.db_env not in ("dev", "prod"):
            raise ValueError(f"DB_ENV must be 'dev' or 'prod', got: {self.db_env}")
        if self.concurrency_limit < 1:
            raise ValueError(
                f"CONCURRENCY_LIMIT must be a positive integer, got: {self.concurrency_limit}"
            )
        if not self.academic_terms:
            self.academic_terms = ((self.academic_year, self.academic_semester),)

        for academic_year, academic_semester in self.academic_terms:
            if not academic_year.isdigit():
                raise ValueError(f"Academic year must be numeric, got: {academic_year}")
            if not academic_semester.isdigit():
                raise ValueError(
                    f"Academic semester must be numeric, got: {academic_semester}"
                )

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
    academic_year = os.getenv("ACADEMIC_YEAR", "115")
    academic_semester = os.getenv("ACADEMIC_SEMESTER", "1")
    academic_terms = parse_academic_terms(
        os.getenv("ACADEMIC_TERMS", ""), academic_year, academic_semester
    )

    return Config(
        db_name=os.getenv("DB_NAME", ""),
        db_uri=os.getenv("DB_URI", ""),
        db_env=os.getenv("DB_ENV", "prod"),  # type: ignore
        academic_year=academic_year,
        academic_semester=academic_semester,
        academic_terms=academic_terms,
        refresh_all_terms=parse_bool(os.getenv("REFRESH_ALL_TERMS", "false")),
        dev_data_limit=int(os.getenv("DEV_DATA_LIMIT", "10")),
        concurrency_limit=int(os.getenv("CONCURRENCY_LIMIT", "3")),
    )


def parse_bool(raw_value: str) -> bool:
    return raw_value.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_academic_terms(
    raw_terms: str, fallback_year: str, fallback_semester: str
) -> tuple[tuple[str, str], ...]:
    """
    Parse ACADEMIC_TERMS values like "114-1,114-2,115-1".

    Falls back to ACADEMIC_YEAR / ACADEMIC_SEMESTER when ACADEMIC_TERMS is empty.
    """
    if not raw_terms.strip():
        return ((fallback_year, fallback_semester),)

    terms: list[tuple[str, str]] = []
    for raw_term in raw_terms.split(","):
        term = raw_term.strip()
        if not term:
            continue
        try:
            academic_year, academic_semester = term.split("-", maxsplit=1)
        except ValueError as exc:
            raise ValueError(
                "ACADEMIC_TERMS must use comma-separated YEAR-SEMESTER values "
                f"(example: 114-1,114-2), got: {term}"
            ) from exc

        terms.append((academic_year.strip(), academic_semester.strip()))

    return tuple(terms) or ((fallback_year, fallback_semester),)


# Global configuration instance
config = load_config()
