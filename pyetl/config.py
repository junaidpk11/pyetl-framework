"""
pyetl/config.py

Loads environment config from .env file.
Provides a single Config object used across the framework.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Target database
    TARGET_DB_TYPE  = os.getenv("TARGET_DB_TYPE", "duckdb")
    TARGET_HOST     = os.getenv("TARGET_HOST", "localhost")
    TARGET_PORT     = int(os.getenv("TARGET_PORT", 5432))
    TARGET_NAME     = os.getenv("TARGET_NAME", "pyetl_dwh")
    TARGET_USER     = os.getenv("TARGET_USER", "")
    TARGET_PASSWORD = os.getenv("TARGET_PASSWORD", "")

    # DuckDB paths
    DUCKDB_PATH     = Path(os.getenv("DUCKDB_PATH", "data/pyetl_dwh.duckdb"))
    CONTROL_DB_PATH = Path(os.getenv("CONTROL_DB_PATH", "data/pyetl_control.duckdb"))

    @classmethod
    def postgres_url(cls) -> str:
        """SQLAlchemy connection URL for PostgreSQL."""
        pwd = f":{cls.TARGET_PASSWORD}" if cls.TARGET_PASSWORD else ""
        return (
            f"postgresql+psycopg2://{cls.TARGET_USER}{pwd}"
            f"@{cls.TARGET_HOST}:{cls.TARGET_PORT}/{cls.TARGET_NAME}"
        )

    @classmethod
    def is_postgres(cls) -> bool:
        return cls.TARGET_DB_TYPE.lower() == "postgres"

    @classmethod
    def summary(cls):
        return (
            f"TARGET_DB_TYPE={cls.TARGET_DB_TYPE}  "
            f"TARGET_NAME={cls.TARGET_NAME}  "
            f"DUCKDB_PATH={cls.DUCKDB_PATH}"
        )