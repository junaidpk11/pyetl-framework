"""
pyetl/extractor.py

Reads from any registered source and loads raw data
into the bronze layer of the DWH database.
Driven entirely by TableConfig from the registry.
"""

import duckdb
import pandas as pd
import logging
import traceback
from pathlib import Path
from typing import Optional

from pyetl.registry import TableConfig, ColumnConfig

log = logging.getLogger(__name__)

DEFAULT_DWH_DB = Path("data/pyetl_dwh.duckdb")


class Extractor:

    def __init__(self, dwh_db: Path = DEFAULT_DWH_DB):
        self.dwh_db = dwh_db
        self.dwh_db.parent.mkdir(parents=True, exist_ok=True)

    def extract(self, table: TableConfig) -> tuple[bool, int, int, str]:
        """
        Extract source data and load into bronze.
        Returns (success, rows_extracted, rows_loaded, error_message)
        """
        log.info(f"  Extracting {table.source_table} → {table.full_target}")

        try:
            # ── Read source ───────────────────────────────────────────────────
            df = self._read_source(table)
            if df is None:
                return False, 0, 0, "Could not read source"

            rows_extracted = len(df)
            log.info(f"    Read {rows_extracted:,} rows from source")

            # ── Normalise column names ────────────────────────────────────────
            df.columns = (
                df.columns
                .str.strip()
                .str.lower()
                .str.replace(" ", "_", regex=False)
                .str.replace(r"[^\w]", "_", regex=True)
            )

            # ── Load into bronze ──────────────────────────────────────────────
            rows_loaded = self._load_bronze(table, df)

            return True, rows_extracted, rows_loaded, None

        except Exception as e:
            error = str(e)
            log.error(f"    Extraction failed: {error}")
            log.debug(traceback.format_exc())
            return False, 0, 0, error

    def _read_source(self, table: TableConfig) -> Optional[pd.DataFrame]:
        """Read data from source based on source_type."""
        source_type = table.source_type.upper()

        if source_type == "CSV":
            return self._read_csv(table)
        elif source_type == "EXCEL":
            return self._read_excel(table)
        elif source_type == "DUCKDB":
            return self._read_duckdb(table)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

    def _read_csv(self, table: TableConfig) -> pd.DataFrame:
        """Read a CSV file from the connection path."""
        base_path = Path(table.connection)
        file_path = base_path / table.source_table

        if not file_path.exists():
            raise FileNotFoundError(f"CSV not found: {file_path}")

        log.info(f"    Reading CSV: {file_path}")

        # Use registered columns if available, else read all
        if table.columns:
            source_cols = [c.source_column for c in table.active_columns]
            df = pd.read_csv(file_path, low_memory=False)
            # Only keep registered columns that exist in the file
            available = [c for c in source_cols if c in df.columns]
            df = df[available]
        else:
            df = pd.read_csv(file_path, low_memory=False)

        return df

    def _read_excel(self, table: TableConfig) -> pd.DataFrame:
        """Read an Excel file."""
        base_path = Path(table.connection)
        file_path = base_path / table.source_table

        if not file_path.exists():
            raise FileNotFoundError(f"Excel not found: {file_path}")

        log.info(f"    Reading Excel: {file_path}")

        # Check if sheet name specified in source_table (e.g. "file.xlsx:Sheet1")
        if ":" in table.source_table:
            fname, sheet = table.source_table.split(":", 1)
            file_path = base_path / fname
            df = pd.read_excel(file_path, sheet_name=sheet)
        else:
            df = pd.read_excel(file_path)

        return df

    def _read_duckdb(self, table: TableConfig) -> pd.DataFrame:
        """Read from another DuckDB database."""
        db_path = Path(table.connection)

        if not db_path.exists():
            raise FileNotFoundError(f"DuckDB not found: {db_path}")

        log.info(f"    Reading DuckDB: {db_path} / {table.source_table}")

        with duckdb.connect(str(db_path), read_only=True) as con:
            df = con.execute(
                f"SELECT * FROM {table.source_table}"
            ).df()

        return df

    def _load_bronze(self, table: TableConfig, df: pd.DataFrame) -> int:
        """Load DataFrame into bronze schema."""
        schema = table.target_schema
        tbl    = table.target_table

        with duckdb.connect(str(self.dwh_db)) as con:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

            # Truncate if configured
            if table.load_config and table.load_config.truncate_before_load:
                con.execute(
                    f"DROP TABLE IF EXISTS {schema}.{tbl}"
                )

            # Load
            con.register("_staging", df)
            con.execute(f"""
                CREATE OR REPLACE TABLE {schema}.{tbl} AS
                SELECT * FROM _staging
            """)
            con.unregister("_staging")

            # Verify
            count = con.execute(
                f"SELECT COUNT(*) FROM {schema}.{tbl}"
            ).fetchone()[0]

        log.info(f"    Loaded {count:,} rows → {schema}.{tbl}")
        return count