"""
control/seed.py

Helper functions for registering sources, tables, columns,
transformations, relationships, load config, and quality rules
into the pyetl control database.

Usage:
    from control.seed import PyETLRegistry
    r = PyETLRegistry()
    src = r.register_source("MY_SOURCE", "CSV", "/data/raw")
    tbl = r.register_table(src, "sales.csv", "bronze", "sales", "FACT")
    r.register_column(tbl, "sale_date", "sale_date", "VARCHAR", "DATE",
                      transformation_type="CAST",
                      transformation_value="%d/%m/%Y")
"""

import duckdb
import json
import logging
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

DEFAULT_CONTROL_DB = Path("data/pyetl_control.duckdb")


class PyETLRegistry:

    def __init__(self, db_path: Path = DEFAULT_CONTROL_DB):
        self.db_path = db_path

    def _con(self):
        return duckdb.connect(str(self.db_path))

    def _next_id(self, con, table: str, id_col: str) -> int:
        result = con.execute(
            f"SELECT COALESCE(MAX({id_col}), 0) + 1 FROM {table}"
        ).fetchone()
        return result[0]

    # ── source_registry ───────────────────────────────────────────────────────

    def register_source(
        self,
        source_name: str,
        source_type: str,
        connection_string: str,
        schema_name: str = None,
        database_name: str = None,
        load_priority: int = 1,
        description: str = None,
        is_active: bool = True,
    ) -> int:
        """
        Register a source system. Returns source_id.
        source_type: CSV | EXCEL | DUCKDB | SQL_SERVER | ORACLE | API
        """
        with self._con() as con:
            existing = con.execute(
                "SELECT source_id FROM source_registry WHERE source_name = ?",
                [source_name]
            ).fetchone()

            if existing:
                log.info(f"  Source already registered: {source_name} (id={existing[0]})")
                return existing[0]

            source_id = self._next_id(con, "source_registry", "source_id")
            con.execute("""
                INSERT INTO source_registry (
                    source_id, source_name, source_type,
                    connection_string, schema_name, database_name,
                    load_priority, description, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                source_id, source_name, source_type,
                connection_string, schema_name, database_name,
                load_priority, description, is_active
            ])
            log.info(f"  Registered source: {source_name} ({source_type}) id={source_id}")
            return source_id

    # ── table_registry ────────────────────────────────────────────────────────

    def register_table(
        self,
        source_id: int,
        source_table: str,
        target_schema: str,
        target_table: str,
        table_type: str,
        load_order: int = 1,
        description: str = None,
        is_active: bool = True,
    ) -> int:
        """
        Register a table mapping. Returns table_id.
        table_type: FACT | DIMENSION | BRIDGE | STAGING
        target_schema: bronze | silver | gold
        """
        with self._con() as con:
            existing = con.execute("""
                SELECT table_id FROM table_registry
                WHERE source_id = ? AND target_schema = ? AND target_table = ?
            """, [source_id, target_schema, target_table]).fetchone()

            if existing:
                log.info(f"  Table already registered: {target_schema}.{target_table} (id={existing[0]})")
                return existing[0]

            table_id = self._next_id(con, "table_registry", "table_id")
            con.execute("""
                INSERT INTO table_registry (
                    table_id, source_id, source_table,
                    target_schema, target_table, table_type,
                    load_order, description, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                table_id, source_id, source_table,
                target_schema, target_table, table_type,
                load_order, description, is_active
            ])
            log.info(f"  Registered table: {source_table} → {target_schema}.{target_table} ({table_type}) id={table_id}")
            return table_id

    # ── column_registry ───────────────────────────────────────────────────────

    def register_column(
        self,
        table_id: int,
        source_column: str,
        target_column: str,
        source_data_type: str = "VARCHAR",
        target_data_type: str = "VARCHAR",
        transformation_type: str = "RENAME",
        transformation_value: str = None,
        is_primary_key: bool = False,
        is_foreign_key: bool = False,
        is_nullable: bool = True,
        ordinal_position: int = 1,
        default_value: str = None,
        description: str = None,
    ) -> int:
        """
        Register a column mapping. Returns column_id.

        transformation_type options:
          RENAME       — just rename, no transform
          CAST         — cast to target type, transformation_value = format if date
          CASE_DECODE  — transformation_value = JSON {"1":"Fatal","2":"Serious"}
          CALCULATED   — transformation_value = SQL expression
          CONSTANT     — transformation_value = fixed value
          LOOKUP       — transformation_value = "schema.table.key.value"
        """
        with self._con() as con:
            existing = con.execute("""
                SELECT column_id FROM column_registry
                WHERE table_id = ? AND source_column = ? AND target_column = ?
            """, [table_id, source_column, target_column]).fetchone()

            if existing:
                return existing[0]

            column_id = self._next_id(con, "column_registry", "column_id")
            con.execute("""
                INSERT INTO column_registry (
                    column_id, table_id,
                    source_column, target_column,
                    source_data_type, target_data_type,
                    transformation_type, transformation_value,
                    is_primary_key, is_foreign_key, is_nullable,
                    ordinal_position, default_value, description
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                column_id, table_id,
                source_column, target_column,
                source_data_type, target_data_type,
                transformation_type, transformation_value,
                is_primary_key, is_foreign_key, is_nullable,
                ordinal_position, default_value, description
            ])
            return column_id

    def register_columns_bulk(self, table_id: int, columns: list[dict]) -> int:
        """
        Register multiple columns at once.
        Each dict: source_column, target_column, source_data_type,
                   target_data_type, transformation_type, transformation_value,
                   is_primary_key, is_foreign_key, ordinal_position
        Returns count of columns registered.
        """
        count = 0
        for i, col in enumerate(columns, 1):
            self.register_column(
                table_id           = table_id,
                source_column      = col["source_column"],
                target_column      = col.get("target_column", col["source_column"]),
                source_data_type   = col.get("source_data_type", "VARCHAR"),
                target_data_type   = col.get("target_data_type", "VARCHAR"),
                transformation_type= col.get("transformation_type", "RENAME"),
                transformation_value=col.get("transformation_value"),
                is_primary_key     = col.get("is_primary_key", False),
                is_foreign_key     = col.get("is_foreign_key", False),
                is_nullable        = col.get("is_nullable", True),
                ordinal_position   = col.get("ordinal_position", i),
                default_value      = col.get("default_value"),
                description        = col.get("description"),
            )
            count += 1
        log.info(f"  Registered {count} columns for table_id={table_id}")
        return count

    # ── transformation_rule ───────────────────────────────────────────────────

    def register_transformation_rule(
        self,
        rule_name: str,
        rule_type: str,
        rule_definition: str,
        description: str = None,
    ) -> int:
        """
        Register a reusable transformation rule. Returns rule_id.
        rule_type: CASE_DECODE | CALCULATED | DATE_FORMAT | LOOKUP
        """
        with self._con() as con:
            existing = con.execute(
                "SELECT rule_id FROM transformation_rule WHERE rule_name = ?",
                [rule_name]
            ).fetchone()

            if existing:
                return existing[0]

            rule_id = self._next_id(con, "transformation_rule", "rule_id")
            con.execute("""
                INSERT INTO transformation_rule (
                    rule_id, rule_name, rule_type, rule_definition, description
                ) VALUES (?, ?, ?, ?, ?)
            """, [rule_id, rule_name, rule_type, rule_definition, description])
            log.info(f"  Registered rule: {rule_name} ({rule_type}) id={rule_id}")
            return rule_id

    # ── table_relationship ────────────────────────────────────────────────────

    def register_relationship(
        self,
        parent_table_id: int,
        parent_column: str,
        child_table_id: int,
        child_column: str,
        relationship_type: str,
        join_type: str = "LEFT",
        description: str = None,
    ) -> int:
        """
        Register a relationship between two tables. Returns relationship_id.
        relationship_type: FACT_TO_DIM | FACT_TO_BRIDGE | BRIDGE_TO_DIM
        """
        with self._con() as con:
            existing = con.execute("""
                SELECT relationship_id FROM table_relationship
                WHERE parent_table_id = ? AND parent_column = ?
                  AND child_table_id  = ? AND child_column  = ?
            """, [parent_table_id, parent_column, child_table_id, child_column]).fetchone()

            if existing:
                return existing[0]

            rel_id = self._next_id(con, "table_relationship", "relationship_id")
            con.execute("""
                INSERT INTO table_relationship (
                    relationship_id, parent_table_id, parent_column,
                    child_table_id, child_column,
                    relationship_type, join_type, description
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                rel_id, parent_table_id, parent_column,
                child_table_id, child_column,
                relationship_type, join_type, description
            ])
            log.info(f"  Registered relationship: table {parent_table_id}.{parent_column} → table {child_table_id}.{child_column} ({relationship_type})")
            return rel_id

    # ── load_config ───────────────────────────────────────────────────────────

    def register_load_config(
        self,
        table_id: int,
        load_type: str = "FULL",
        watermark_column: str = None,
        watermark_value: str = None,
        batch_size: int = 500000,
        truncate_before_load: bool = False,
        scd_key_columns: list = None,
        scd_track_columns: list = None,
        partition_column: str = None,
        partition_count: int = 4,
    ) -> int:
        """
        Register load configuration for a table. Returns config_id.
        load_type: FULL | INCREMENTAL | SCD1 | SCD2
        """
        with self._con() as con:
            existing = con.execute(
                "SELECT config_id FROM load_config WHERE table_id = ?",
                [table_id]
            ).fetchone()

            if existing:
                return existing[0]

            config_id = self._next_id(con, "load_config", "config_id")
            con.execute("""
                INSERT INTO load_config (
                    config_id, table_id, load_type,
                    watermark_column, watermark_value,
                    batch_size, truncate_before_load,
                    scd_key_columns, scd_track_columns,
                    partition_column, partition_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                config_id, table_id, load_type,
                watermark_column, watermark_value,
                batch_size, truncate_before_load,
                json.dumps(scd_key_columns) if scd_key_columns else None,
                json.dumps(scd_track_columns) if scd_track_columns else None,
                partition_column, partition_count
            ])
            log.info(f"  Registered load config: table_id={table_id} type={load_type} id={config_id}")
            return config_id

    # ── data_quality_rule ─────────────────────────────────────────────────────

    def register_quality_rule(
        self,
        table_id: int,
        rule_name: str,
        rule_type: str,
        column_name: str = None,
        operator: str = None,
        threshold: float = None,
        threshold_max: float = None,
        ref_table_id: int = None,
        ref_column: str = None,
        custom_sql: str = None,
        severity: str = "ERROR",
        description: str = None,
    ) -> int:
        """
        Register a data quality rule. Returns rule_id.
        rule_type: NOT_NULL | UNIQUE | ROW_COUNT | ROW_COUNT_DELTA
                   REF_INTEGRITY | CUSTOM_SQL
        severity:  ERROR (stop pipeline) | WARNING (log and continue)
        """
        with self._con() as con:
            existing = con.execute("""
                SELECT rule_id FROM data_quality_rule
                WHERE table_id = ? AND rule_name = ?
            """, [table_id, rule_name]).fetchone()

            if existing:
                return existing[0]

            rule_id = self._next_id(con, "data_quality_rule", "rule_id")
            con.execute("""
                INSERT INTO data_quality_rule (
                    rule_id, table_id, rule_name, rule_type,
                    column_name, operator, threshold, threshold_max,
                    ref_table_id, ref_column, custom_sql,
                    severity, description
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                rule_id, table_id, rule_name, rule_type,
                column_name, operator, threshold, threshold_max,
                ref_table_id, ref_column, custom_sql,
                severity, description
            ])
            log.info(f"  Registered quality rule: {rule_name} ({rule_type}) severity={severity} id={rule_id}")
            return rule_id

    # ── summary ───────────────────────────────────────────────────────────────

    def summary(self):
        """Print a summary of everything registered."""
        with self._con() as con:
            log.info("\n=== Registry summary ===")
            for table in [
                "source_registry", "table_registry", "column_registry",
                "transformation_rule", "table_relationship",
                "load_config", "data_quality_rule"
            ]:
                count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                log.info(f"  {table:<30} {count:>5} rows")