"""Database connection utilities."""

import os

from sqlalchemy import create_engine


def get_engine():
    """Create a SQLAlchemy engine from environment variables."""
    host = os.environ.get("DB_HOST", "localhost")
    port = os.environ.get("DB_PORT", "5432")
    name = os.environ.get("DB_NAME", "health_analytics")
    user = os.environ.get("DB_USER", "moh_analyst")
    password = os.environ.get("DB_PASSWORD", "moh_secure_2025")
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{name}")
