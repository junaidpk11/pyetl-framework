"""
pyetl/adapters/postgres.py

PostgreSQL adapter for pyetl-framework.
Supports:
  - Schema and table creation
  - Full load (truncate + insert)
  - Append load (insert only)
  - Upsert (insert or update on conflict)
  - Fast bulk upsert via staging table
  - Protected fields (never overwritten on upsert)
"""

from __future__ import annotations

import logging
from typing import Optional, Iterable

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

log = logging.getLogger(__name__)


class PostgresLoader:
    def __init__(self, connection_url: str):
        self.connection_url = connection_url
        self.engine = create_engine(connection_url)

    def test_connection(self) -> bool:
        """Test the database connection."""
        try:
            with self.engine.connect() as con:
                con.execute(text("SELECT 1"))
            log.info("  PostgreSQL connection OK")
            return True
        except SQLAlchemyError as e:
            log.error(f"  PostgreSQL connection failed: {e}")
            return False

    def create_schema(self, schema: str) -> None:
        """Create schema if it doesn't exist."""
        sql = f'CREATE SCHEMA IF NOT EXISTS "{schema}"'
        with self.engine.begin() as con:
            con.execute(text(sql))
        log.info(f"  Schema ready: {schema}")

    def create_table_from_df(
        self,
        df: pd.DataFrame,
        schema: str,
        table: str,
        primary_keys: Optional[list[str]] = None,
        extra_columns: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Create a table from a DataFrame's structure.

        extra_columns: additional columns not in the DataFrame
                       e.g. {"exclude": "BOOLEAN DEFAULT FALSE",
                             "notes": "TEXT"}
        """
        if df is None:
            raise ValueError("df cannot be None")

        col_defs: list[str] = []

        type_map = {
            "int64": "BIGINT",
            "int32": "INTEGER",
            "float64": "DOUBLE PRECISION",
            "float32": "REAL",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP",
            "object": "TEXT",
        }

        for col, dtype in df.dtypes.items():
            pg_type = type_map.get(str(dtype), "TEXT")
            col_defs.append(f'    "{col}" {pg_type}')

        if extra_columns:
            for col_name, col_def in extra_columns.items():
                col_defs.append(f'    "{col_name}" {col_def}')

        if primary_keys:
            pk_cols = ", ".join(f'"{k}"' for k in primary_keys)
            col_defs.append(f"    PRIMARY KEY ({pk_cols})")

        cols_sql = ",\n".join(col_defs)
        sql = f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (\n{cols_sql}\n)'

        with self.engine.begin() as con:
            con.execute(text(sql))

        log.info(f"  Table ready: {schema}.{table}")

    def load_full(
        self,
        df: pd.DataFrame,
        schema: str,
        table: str,
    ) -> int:
        """
        Full load — truncate then insert.
        Returns rows inserted.
        """
        if df is None:
            raise ValueError("df cannot be None")

        with self.engine.begin() as con:
            con.execute(text(f'TRUNCATE TABLE "{schema}"."{table}"'))

        if not df.empty:
            df.to_sql(
                name=table,
                schema=schema,
                con=self.engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

        log.info(f"  Full load: {len(df):,} rows → {schema}.{table}")
        return len(df)

    def load_append(
        self,
        df: pd.DataFrame,
        schema: str,
        table: str,
    ) -> int:
        """
        Append load — insert only, no updates.
        Returns rows inserted.
        """
        if df is None:
            raise ValueError("df cannot be None")

        if not df.empty:
            df.to_sql(
                name=table,
                schema=schema,
                con=self.engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

        log.info(f"  Append load: {len(df):,} rows → {schema}.{table}")
        return len(df)

    def load_upsert(
        self,
        df: pd.DataFrame,
        schema: str,
        table: str,
        primary_keys: list[str],
        protected_columns: Optional[list[str]] = None,
    ) -> tuple[int, int]:
        """
        Upsert — insert new rows, update existing rows on PK conflict.
        protected_columns: columns that are NEVER overwritten on update.
                           Use this for manual fields like 'exclude', 'notes'.

        Returns (processed_count, estimated_updated_count).
        Note: PostgreSQL does not cleanly expose inserted vs updated row counts
        for this pattern through SQLAlchemy rowcount, so this is best treated
        as processed rows plus an approximate updated count.
        """
        if df is None:
            raise ValueError("df cannot be None")
        if not primary_keys:
            raise ValueError("primary_keys required for upsert")
        if df.empty:
            log.info(f"  Upsert: 0 rows → {schema}.{table}")
            return 0, 0

        protected = set(protected_columns or [])
        all_cols = list(df.columns)

        update_cols = [
            c for c in all_cols
            if c not in primary_keys and c not in protected
        ]

        col_names = ", ".join(f'"{c}"' for c in all_cols)
        placeholders = ", ".join(f":{c}" for c in all_cols)
        conflict_pk = ", ".join(f'"{c}"' for c in primary_keys)

        if update_cols:
            conflict_action = "DO UPDATE SET " + ", ".join(
                f'"{c}" = EXCLUDED."{c}"' for c in update_cols
            )
        else:
            conflict_action = "DO NOTHING"

        sql = f"""
            INSERT INTO "{schema}"."{table}" ({col_names})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_pk})
            {conflict_action}
        """

        processed = 0

        with self.engine.begin() as con:
            for _, row in df.iterrows():
                con.execute(text(sql), row.to_dict())
                processed += 1

        log.info(f"  Upsert: processed={processed:,} rows → {schema}.{table}")
        return processed, 0

    def load_upsert_fast(
        self,
        df: pd.DataFrame,
        schema: str,
        table: str,
        primary_keys: list[str],
        protected_columns: Optional[list[str]] = None,
    ) -> int:
        """
        Fast bulk upsert using a staging table.
        Much faster than row-by-row for large DataFrames.
        Returns total rows processed.
        """
        if df is None:
            raise ValueError("df cannot be None")
        if not primary_keys:
            raise ValueError("primary_keys required for upsert")
        if df.empty:
            log.info(f"  Fast upsert: 0 rows → {schema}.{table}")
            return 0

        protected = set(protected_columns or [])
        all_cols = list(df.columns)

        update_cols = [
            c for c in all_cols
            if c not in primary_keys and c not in protected
        ]

        conflict_pk = ", ".join(f'"{c}"' for c in primary_keys)
        col_names = ", ".join(f'"{c}"' for c in all_cols)
        staging_tbl = f"_staging_{table}"

        if update_cols:
            conflict_action = "DO UPDATE SET " + ", ".join(
                f'"{c}" = EXCLUDED."{c}"' for c in update_cols
            )
        else:
            conflict_action = "DO NOTHING"

        try:
            with self.engine.begin() as con:
                con.execute(text(
                    f'DROP TABLE IF EXISTS "{schema}"."{staging_tbl}"'
                ))
                con.execute(text(
                    f'CREATE TABLE "{schema}"."{staging_tbl}" '
                    f'AS SELECT * FROM "{schema}"."{table}" WHERE 1=0'
                ))

            df.to_sql(
                name=staging_tbl,
                schema=schema,
                con=self.engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )

            merge_sql = f"""
                INSERT INTO "{schema}"."{table}" ({col_names})
                SELECT {col_names}
                FROM "{schema}"."{staging_tbl}"
                ON CONFLICT ({conflict_pk})
                {conflict_action}
            """

            with self.engine.begin() as con:
                con.execute(text(merge_sql))

            log.info(f"  Fast upsert: {len(df):,} rows → {schema}.{table}")
            return len(df)

        finally:
            try:
                with self.engine.begin() as con:
                    con.execute(text(
                        f'DROP TABLE IF EXISTS "{schema}"."{staging_tbl}"'
                    ))
            except Exception as cleanup_error:
                log.warning(
                    f'  Failed to drop staging table "{schema}"."{staging_tbl}": '
                    f"{cleanup_error}"
                )

    def row_count(self, schema: str, table: str) -> int:
        """Return current row count of a table."""
        with self.engine.connect() as con:
            result = con.execute(
                text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            )
            return int(result.scalar() or 0)

    def execute(self, sql: str) -> None:
        """Execute raw SQL."""
        with self.engine.begin() as con:
            con.execute(text(sql))

    def dispose(self) -> None:
        """Dispose SQLAlchemy engine connections."""
        self.engine.dispose()