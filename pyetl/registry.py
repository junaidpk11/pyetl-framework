"""
pyetl/registry.py

Reads the control database and returns structured configuration
objects that the ETL engine uses to drive extraction,
transformation, loading, and quality checks.
"""

import duckdb
import json
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)

DEFAULT_CONTROL_DB = Path("data/pyetl_control.duckdb")


@dataclass
class ColumnConfig:
    column_id:            int
    source_column:        str
    target_column:        str
    source_data_type:     str
    target_data_type:     str
    transformation_type:  str
    transformation_value: Optional[str]
    is_primary_key:       bool
    is_foreign_key:       bool
    is_nullable:          bool
    ordinal_position:     int
    default_value:        Optional[str]

    def decoded_map(self) -> Optional[dict]:
        """Return CASE_DECODE mapping as dict, or None."""
        if self.transformation_type == "CASE_DECODE" and self.transformation_value:
            return json.loads(self.transformation_value)
        return None


@dataclass
class LoadConfig:
    config_id:            int
    load_type:            str
    watermark_column:     Optional[str]
    watermark_value:      Optional[str]
    batch_size:           int
    truncate_before_load: bool
    scd_key_columns:      list = field(default_factory=list)
    scd_track_columns:    list = field(default_factory=list)
    partition_column:     Optional[str] = None
    partition_count:      int = 4


@dataclass
class QualityRule:
    rule_id:       int
    rule_name:     str
    rule_type:     str
    column_name:   Optional[str]
    operator:      Optional[str]
    threshold:     Optional[float]
    threshold_max: Optional[float]
    ref_table_id:  Optional[int]
    ref_column:    Optional[str]
    custom_sql:    Optional[str]
    severity:      str


@dataclass
class TableConfig:
    table_id:      int
    source_id:     int
    source_name:   str
    source_type:   str
    connection:    str
    source_table:  str
    target_schema: str
    target_table:  str
    table_type:    str
    load_order:    int
    columns:       list[ColumnConfig]   = field(default_factory=list)
    load_config:   Optional[LoadConfig] = None
    quality_rules: list[QualityRule]    = field(default_factory=list)
    relationships: list[dict]           = field(default_factory=list)

    @property
    def full_target(self) -> str:
        return f"{self.target_schema}.{self.target_table}"

    @property
    def primary_keys(self) -> list[str]:
        return [c.target_column for c in self.columns if c.is_primary_key]

    @property
    def active_columns(self) -> list[ColumnConfig]:
        return sorted(self.columns, key=lambda c: c.ordinal_position)


class PyETLRegistryReader:

    def __init__(self, db_path: Path = DEFAULT_CONTROL_DB):
        self.db_path = db_path

    def _con(self):
        return duckdb.connect(str(self.db_path), read_only=True)

    def get_active_tables(
        self,
        source_name: Optional[str] = None,
        target_schema: Optional[str] = None,
        table_type: Optional[str] = None,
    ) -> list[TableConfig]:
        """
        Returns fully hydrated TableConfig objects for all active tables.
        Optionally filter by source_name, target_schema, or table_type.
        """
        with self._con() as con:
            query = """
                SELECT
                    tr.table_id,
                    tr.source_id,
                    sr.source_name,
                    sr.source_type,
                    sr.connection_string,
                    tr.source_table,
                    tr.target_schema,
                    tr.target_table,
                    tr.table_type,
                    tr.load_order
                FROM table_registry tr
                JOIN source_registry sr ON tr.source_id = sr.source_id
                WHERE tr.is_active = true AND sr.is_active = true
            """
            params = []
            if source_name:
                query += " AND sr.source_name = ?"
                params.append(source_name)
            if target_schema:
                query += " AND tr.target_schema = ?"
                params.append(target_schema)
            if table_type:
                query += " AND tr.table_type = ?"
                params.append(table_type)
            query += " ORDER BY sr.load_priority, tr.load_order"

            rows = con.execute(query, params).fetchall()
            tables = []
            for row in rows:
                table = TableConfig(
                    table_id      = row[0],
                    source_id     = row[1],
                    source_name   = row[2],
                    source_type   = row[3],
                    connection    = row[4],
                    source_table  = row[5],
                    target_schema = row[6],
                    target_table  = row[7],
                    table_type    = row[8],
                    load_order    = row[9],
                )
                table.columns       = self._get_columns(con, table.table_id)
                table.load_config   = self._get_load_config(con, table.table_id)
                table.quality_rules = self._get_quality_rules(con, table.table_id)
                table.relationships = self._get_relationships(con, table.table_id)
                tables.append(table)

            return tables

    def _get_columns(self, con, table_id: int) -> list[ColumnConfig]:
        rows = con.execute("""
            SELECT
                column_id, source_column, target_column,
                source_data_type, target_data_type,
                transformation_type, transformation_value,
                is_primary_key, is_foreign_key, is_nullable,
                ordinal_position, default_value
            FROM column_registry
            WHERE table_id = ? AND is_active = true
            ORDER BY ordinal_position
        """, [table_id]).fetchall()

        return [ColumnConfig(
            column_id            = r[0],
            source_column        = r[1],
            target_column        = r[2],
            source_data_type     = r[3],
            target_data_type     = r[4],
            transformation_type  = r[5],
            transformation_value = r[6],
            is_primary_key       = r[7],
            is_foreign_key       = r[8],
            is_nullable          = r[9],
            ordinal_position     = r[10],
            default_value        = r[11],
        ) for r in rows]

    def _get_load_config(self, con, table_id: int) -> Optional[LoadConfig]:
        row = con.execute("""
            SELECT
                config_id, load_type,
                watermark_column, watermark_value,
                batch_size, truncate_before_load,
                scd_key_columns, scd_track_columns,
                partition_column, partition_count
            FROM load_config
            WHERE table_id = ?
        """, [table_id]).fetchone()

        if not row:
            return None

        return LoadConfig(
            config_id            = row[0],
            load_type            = row[1],
            watermark_column     = row[2],
            watermark_value      = row[3],
            batch_size           = row[4],
            truncate_before_load = row[5],
            scd_key_columns      = json.loads(row[6]) if row[6] else [],
            scd_track_columns    = json.loads(row[7]) if row[7] else [],
            partition_column     = row[8],
            partition_count      = row[9],
        )

    def _get_quality_rules(self, con, table_id: int) -> list[QualityRule]:
        rows = con.execute("""
            SELECT
                rule_id, rule_name, rule_type,
                column_name, operator,
                threshold, threshold_max,
                ref_table_id, ref_column,
                custom_sql, severity
            FROM data_quality_rule
            WHERE table_id = ? AND is_active = true
        """, [table_id]).fetchall()

        return [QualityRule(
            rule_id       = r[0],
            rule_name     = r[1],
            rule_type     = r[2],
            column_name   = r[3],
            operator      = r[4],
            threshold     = r[5],
            threshold_max = r[6],
            ref_table_id  = r[7],
            ref_column    = r[8],
            custom_sql    = r[9],
            severity      = r[10],
        ) for r in rows]

    def _get_relationships(self, con, table_id: int) -> list[dict]:
        rows = con.execute("""
            SELECT
                relationship_id,
                parent_table_id, parent_column,
                child_table_id,  child_column,
                relationship_type, join_type
            FROM table_relationship
            WHERE parent_table_id = ? AND is_active = true
        """, [table_id]).fetchall()

        return [{
            "relationship_id":   r[0],
            "parent_table_id":   r[1],
            "parent_column":     r[2],
            "child_table_id":    r[3],
            "child_column":      r[4],
            "relationship_type": r[5],
            "join_type":         r[6],
        } for r in rows]

    def get_table_by_id(self, table_id: int) -> Optional[TableConfig]:
        """Fetch a single table config by ID."""
        tables = self.get_active_tables()
        for t in tables:
            if t.table_id == table_id:
                return t
        return None

    def print_summary(self):
        """Print a human-readable summary of registered config."""
        tables = self.get_active_tables()
        log.info(f"\n=== Registry — {len(tables)} active tables ===")
        for t in tables:
            log.info(
                f"  [{t.table_id:>2}] {t.source_table:<35} → "
                f"{t.full_target:<35} "
                f"({t.table_type}) "
                f"cols={len(t.columns)} "
                f"rules={len(t.quality_rules)}"
            )