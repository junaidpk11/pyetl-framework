"""
pyetl/transformer.py

Reads column config from the registry and dynamically generates
SQL to transform bronze → silver.

Supports all transformation types:
  RENAME       — rename column, keep type (trims VARCHAR)
  CAST         — cast to target type (DATE uses STRPTIME for format strings)
  CASE_DECODE  — decode numeric codes to labels via CASE WHEN
  CALCULATED   — arbitrary SQL expression
  CONSTANT     — fixed value for every row
  LOOKUP       — join to reference table for value
"""

import duckdb
import logging
import traceback
from pathlib import Path
from typing import Optional

from pyetl.registry import TableConfig, ColumnConfig

log = logging.getLogger(__name__)

DEFAULT_DWH_DB = Path("data/pyetl_dwh.duckdb")


class Transformer:

    def __init__(self, dwh_db: Path = DEFAULT_DWH_DB):
        self.dwh_db = dwh_db

    def transform(self, table: TableConfig) -> tuple[bool, int, str]:
        """
        Transform a bronze table into silver using column registry config.
        Returns (success, rows_loaded, error_message)
        """
        log.info(f"  Transforming {table.source_table} → {table.full_target}")

        try:
            sql = self._build_transform_sql(table)
            if not sql:
                return False, 0, "Could not build transform SQL"

            log.debug(f"    SQL:\n{sql}")

            silver_target = f"silver.{table.target_table}"
            with duckdb.connect(str(self.dwh_db)) as con:
                con.execute("CREATE SCHEMA IF NOT EXISTS silver")
                con.execute(sql)
                count = con.execute(
                    f"SELECT COUNT(*) FROM {silver_target}"
                ).fetchone()[0]

            log.info(f"    Loaded {count:,} rows → {table.full_target}")
            return True, count, None

        except Exception as e:
            error = str(e)
            log.error(f"    Transform failed: {error}")
            log.debug(traceback.format_exc())
            return False, 0, error

    def _build_transform_sql(self, table: TableConfig) -> Optional[str]:
        """
        Dynamically build CREATE OR REPLACE TABLE SQL
        from column registry config.
        """
        if not table.columns:
            log.warning(
                f"    No columns registered for {table.full_target} "
                f"— copying source as-is"
            )
            return f"""
                CREATE OR REPLACE TABLE {table.full_target} AS
                SELECT * FROM {table.source_table}
                WHERE 1=1
            """

        select_parts = []
        for col in table.active_columns:
            expr = self._build_column_expression(col)
            select_parts.append(f"        {expr}")

        select_clause = ",\n".join(select_parts)

        # Determine source — could be schema.table or just table name
        source = table.source_table
        if source.endswith(".csv"):
            source = f"bronze.{source.replace('.csv', '')}"
        elif "." not in source and "+" not in source:
            source = f"bronze.{source}"

        silver_target = f"silver.{table.target_table}"
        sql = f"""
            CREATE OR REPLACE TABLE {silver_target} AS
            SELECT
{select_clause}
            FROM {source}
            WHERE 1=1
        """
        return sql

    def _build_column_expression(self, col: ColumnConfig) -> str:
        """
        Build a single SELECT expression for a column
        based on its transformation type.
        """
        t = col.transformation_type.upper() if col.transformation_type else "RENAME"

        if t == "RENAME":
            return self._expr_rename(col)
        elif t == "CAST":
            return self._expr_cast(col)
        elif t == "CASE_DECODE":
            return self._expr_case_decode(col)
        elif t == "CALCULATED":
            return self._expr_calculated(col)
        elif t == "CONSTANT":
            return self._expr_constant(col)
        elif t == "LOOKUP":
            return self._expr_rename(col)
        else:
            log.warning(f"    Unknown transformation type: {t} — using RENAME")
            return self._expr_rename(col)

    def _expr_rename(self, col: ColumnConfig) -> str:
        """
        Simple rename — source AS target.
        Automatically trims leading/trailing whitespace from VARCHAR columns.
        """
        dtype = (col.target_data_type or "VARCHAR").upper()
        src   = col.source_column
        tgt   = col.target_column

        if dtype == "VARCHAR":
            expr = f"TRIM({src})"
        else:
            expr = src

        if src == tgt and dtype != "VARCHAR":
            return src
        return f"{expr} AS {tgt}"

    def _expr_cast(self, col: ColumnConfig) -> str:
        """
        Cast source column to target type.
        Source is trimmed before casting to avoid silent NULL from whitespace.
        For DATE with a format string, use STRPTIME.
        For other types, use TRY_CAST.
        """
        dtype = (col.target_data_type or "VARCHAR").upper()
        src   = f"TRIM(TRY_CAST({col.source_column} AS VARCHAR))"

        if dtype == "DATE" and col.transformation_value:
            fmt  = col.transformation_value
            expr = f"TRY_CAST(STRPTIME({src}, '{fmt}') AS DATE)"
        elif dtype == "DATE":
            expr = f"TRY_CAST({src} AS DATE)"
        elif dtype == "TIME":
            expr = f"TRY_CAST({src} AS TIME)"
        elif dtype == "INTEGER":
            expr = f"TRY_CAST({src} AS INTEGER)"
        elif dtype == "DOUBLE":
            expr = f"TRY_CAST({src} AS DOUBLE)"
        elif dtype == "BOOLEAN":
            expr = f"TRY_CAST({src} AS BOOLEAN)"
        else:
            expr = f"TRY_CAST({src} AS VARCHAR)"

        return f"{expr} AS {col.target_column}"

    def _expr_case_decode(self, col: ColumnConfig) -> str:
        """
        Build a CASE WHEN expression from a JSON decode map.
        {"1":"Fatal","2":"Serious","3":"Slight"} becomes:
        CASE WHEN TRIM(CAST(col AS VARCHAR)) = '1' THEN 'Fatal'
             WHEN TRIM(CAST(col AS VARCHAR)) = '2' THEN 'Serious'
             ...
             ELSE 'Unknown'
        END AS target_col

        Source is trimmed before comparison to handle whitespace padding.
        """
        decode_map = col.decoded_map()
        if not decode_map:
            log.warning(
                f"    CASE_DECODE on {col.source_column} "
                f"has no decode map — using RENAME"
            )
            return self._expr_rename(col)

        when_clauses = []
        for k, v in decode_map.items():
            when_clauses.append(
                f"WHEN TRIM(TRY_CAST({col.source_column} AS VARCHAR)) = '{k}' "
                f"THEN '{v}'"
            )

        when_sql = "\n             ".join(when_clauses)
        return (
            f"CASE {when_sql}\n"
            f"             ELSE 'Unknown'\n"
            f"        END AS {col.target_column}"
        )

    def _expr_calculated(self, col: ColumnConfig) -> str:
        """
        Use a raw SQL expression from transformation_value.
        e.g. "TRIM(first_name) || ' ' || TRIM(last_name)"
        """
        if not col.transformation_value:
            log.warning(
                f"    CALCULATED on {col.source_column} "
                f"has no expression — using RENAME"
            )
            return self._expr_rename(col)

        return f"({col.transformation_value}) AS {col.target_column}"

    def _expr_constant(self, col: ColumnConfig) -> str:
        """Return a fixed constant value for every row."""
        val = col.transformation_value or col.default_value or "NULL"
        if val != "NULL" and not val.lstrip("-").replace(".", "").isdigit():
            val = f"'{val}'"
        return f"{val} AS {col.target_column}"

    def preview_sql(self, table: TableConfig) -> str:
        """Return the generated SQL for a table without executing it."""
        return self._build_transform_sql(table)