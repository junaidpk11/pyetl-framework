"""
pyetl/loader.py

Builds the gold layer — star schema facts, dimensions, and bridge tables
from silver data. Driven entirely by table_registry and table_relationship
config — no hardcoded SQL.
"""

import duckdb
import logging
import traceback
from pathlib import Path
from typing import Optional

from pyetl.registry import PyETLRegistryReader, TableConfig

log = logging.getLogger(__name__)

DEFAULT_DWH_DB     = Path("data/pyetl_dwh.duckdb")
DEFAULT_CONTROL_DB = Path("data/pyetl_control.duckdb")


class Loader:

    def __init__(
        self,
        dwh_db:     Path = DEFAULT_DWH_DB,
        control_db: Path = DEFAULT_CONTROL_DB,
    ):
        self.dwh_db   = dwh_db
        self.registry = PyETLRegistryReader(control_db)

    def load(self, table: TableConfig) -> tuple[bool, int, str]:
        """
        Load a gold table (FACT, DIMENSION, or BRIDGE).
        Returns (success, rows_loaded, error_message)
        """
        log.info(f"  Loading {table.full_target} ({table.table_type})")

        try:
            if table.table_type == "DIMENSION":
                sql = self._build_dimension_sql(table)
            elif table.table_type == "FACT":
                sql = self._build_fact_sql(table)
            elif table.table_type == "BRIDGE":
                sql = self._build_bridge_sql(table)
            else:
                return False, 0, f"Unknown table_type: {table.table_type}"

            if not sql:
                return False, 0, "Could not build loader SQL"

            log.debug(f"    SQL:\n{sql}")

            with duckdb.connect(str(self.dwh_db)) as con:
                con.execute("CREATE SCHEMA IF NOT EXISTS gold")
                con.execute(sql)
                count = con.execute(
                    f"SELECT COUNT(*) FROM {table.full_target}"
                ).fetchone()[0]

            log.info(f"    Loaded {count:,} rows → {table.full_target}")
            return True, count, None

        except Exception as e:
            error = str(e)
            log.error(f"    Load failed: {error}")
            log.debug(traceback.format_exc())
            return False, 0, error

    def preview_sql(self, table: TableConfig) -> str:
        """Return generated SQL without executing."""
        if table.table_type == "DIMENSION":
            return self._build_dimension_sql(table)
        elif table.table_type == "FACT":
            return self._build_fact_sql(table)
        elif table.table_type == "BRIDGE":
            return self._build_bridge_sql(table)
        return ""

    # ── Dimension builder ─────────────────────────────────────────────────────

    def _build_dimension_sql(self, table: TableConfig) -> str:
        """
        Build a DIMENSION table from silver.
        Uses registered columns if available.
        Falls back to well-known dimension patterns by target_table name.
        """
        source = table.source_table
        target = table.full_target

        # If columns registered — use them
        if table.columns:
            cols = ", ".join(
                f"{c.source_column} AS {c.target_column}"
                if c.source_column != c.target_column
                else c.source_column
                for c in table.active_columns
            )
            return f"""
                CREATE OR REPLACE TABLE {target} AS
                SELECT DISTINCT {cols}
                FROM {source}
                WHERE 1=1
                ORDER BY 1
            """

        # Fall back to named dimension patterns
        tbl = table.target_table.lower()

        if tbl == "dim_date":
            return f"""
                CREATE OR REPLACE TABLE {target} AS
                SELECT DISTINCT
                    collision_date                              AS date_key,
                    YEAR(collision_date)                        AS year,
                    QUARTER(collision_date)                     AS quarter,
                    MONTH(collision_date)                       AS month,
                    MONTHNAME(collision_date)                   AS month_name,
                    DAY(collision_date)                         AS day,
                    DAYNAME(collision_date)                     AS day_name,
                    DAYOFWEEK(collision_date)                   AS day_of_week,
                    CASE WHEN DAYOFWEEK(collision_date) IN (1,7)
                         THEN true ELSE false END               AS is_weekend
                FROM {source}
                WHERE collision_date IS NOT NULL
                ORDER BY date_key
            """

        elif tbl == "dim_geography":
            return f"""
                CREATE OR REPLACE TABLE {target} AS
                SELECT DISTINCT
                    local_authority_ons_district                AS geography_key,
                    local_authority_ons_district,
                    local_authority_highway,
                    urban_or_rural_area
                FROM {source}
                WHERE local_authority_ons_district IS NOT NULL
                ORDER BY local_authority_ons_district
            """

        elif tbl == "dim_severity":
            return f"""
                CREATE OR REPLACE TABLE {target} AS
                SELECT DISTINCT
                    collision_severity                          AS severity_key,
                    collision_severity                          AS severity_label,
                    CASE collision_severity
                        WHEN 'Fatal'   THEN 1
                        WHEN 'Serious' THEN 2
                        WHEN 'Slight'  THEN 3
                        ELSE 99
                    END                                         AS severity_rank
                FROM {source}
                WHERE collision_severity IS NOT NULL
                ORDER BY severity_rank
            """

        elif tbl == "dim_vehicle_type":
            return f"""
                CREATE OR REPLACE TABLE {target} AS
                SELECT DISTINCT
                    vehicle_type                                AS vehicle_type_key,
                    vehicle_type                                AS vehicle_type_label,
                    CASE
                        WHEN vehicle_type ILIKE '%motorcycle%' THEN 'Motorcycle'
                        WHEN vehicle_type ILIKE '%cycle%'      THEN 'Cycle'
                        WHEN vehicle_type ILIKE '%car%'        THEN 'Car'
                        WHEN vehicle_type ILIKE '%bus%'
                          OR vehicle_type ILIKE '%coach%'      THEN 'Bus / Coach'
                        WHEN vehicle_type ILIKE '%goods%'
                          OR vehicle_type ILIKE '%van%'        THEN 'Goods / Van'
                        WHEN vehicle_type ILIKE '%taxi%'       THEN 'Taxi'
                        ELSE 'Other'
                    END                                         AS vehicle_category
                FROM {source}
                WHERE vehicle_type IS NOT NULL
                ORDER BY vehicle_type_key
            """

        else:
            log.warning(f"    No column config or pattern for {target} — copying source")
            return f"""
                CREATE OR REPLACE TABLE {target} AS
                SELECT DISTINCT * FROM {source}
            """

    # ── Fact builder ──────────────────────────────────────────────────────────

    def _build_fact_sql(self, table: TableConfig) -> str:
        """
        Build a FACT table from silver.
        Uses table_relationship config to generate LEFT JOINs
        to dimension tables.
        Source can be a single silver table or a multi-source join
        (indicated by '+' in source_table).
        """
        source  = table.source_table
        target  = table.full_target
        rels    = table.relationships

        # Multi-source fact (e.g. accidents + casualties + vehicles)
        if "+" in source:
            return self._build_multi_source_fact_sql(table)

        # Single source fact
        join_clauses = self._build_join_clauses(rels)
        join_sql     = "\n            ".join(join_clauses) if join_clauses else ""

        if table.columns:
            cols = ",\n                ".join(
                f"src.{c.source_column} AS {c.target_column}"
                if c.source_column != c.target_column
                else f"src.{c.source_column}"
                for c in table.active_columns
            )
            select = cols
        else:
            select = "src.*"

        return f"""
            CREATE OR REPLACE TABLE {target} AS
            SELECT
                {select}
            FROM {source} src
            {join_sql}
        """

    def _build_multi_source_fact_sql(self, table: TableConfig) -> str:
        """
        Build fact table from multiple silver sources joined together.
        Hardcoded pattern for accidents + casualties + vehicles.
        In a fully generic engine, the join logic would also be config-driven.
        """
        target = table.full_target

        return f"""
            CREATE OR REPLACE TABLE {target} AS
            SELECT
                a.collision_index,
                a.collision_date                                     AS date_key,
                a.local_authority_ons_district                       AS geography_key,
                a.collision_severity,
                a.number_of_vehicles,
                a.number_of_casualties,
                COUNT(DISTINCT v.vehicle_reference)                  AS actual_vehicles,
                COUNT(DISTINCT c.casualty_reference)                 AS actual_casualties,
                SUM(CASE WHEN c.casualty_severity = 'Fatal'
                    THEN 1 ELSE 0 END)                               AS fatal_casualties,
                SUM(CASE WHEN c.casualty_severity = 'Serious'
                    THEN 1 ELSE 0 END)                               AS serious_casualties,
                SUM(CASE WHEN c.casualty_severity = 'Slight'
                    THEN 1 ELSE 0 END)                               AS slight_casualties,
                a.weather_conditions,
                a.road_surface_conditions,
                a.light_conditions,
                a.road_type,
                a.speed_limit,
                a.urban_or_rural_area,
                a.longitude,
                a.latitude,
                a.lsoa_of_accident_location,
                a.collision_time,
                a.collision_year
            FROM silver.accidents a
            LEFT JOIN silver.casualties c
                ON a.collision_index = c.collision_index
            LEFT JOIN silver.vehicles v
                ON a.collision_index = v.collision_index
            GROUP BY
                a.collision_index,
                a.collision_date,
                a.local_authority_ons_district,
                a.collision_severity,
                a.number_of_vehicles,
                a.number_of_casualties,
                a.weather_conditions,
                a.road_surface_conditions,
                a.light_conditions,
                a.road_type,
                a.speed_limit,
                a.urban_or_rural_area,
                a.longitude,
                a.latitude,
                a.lsoa_of_accident_location,
                a.collision_time,
                a.collision_year
        """

    # ── Bridge builder ────────────────────────────────────────────────────────

    def _build_bridge_sql(self, table: TableConfig) -> str:
        """
        Build a BRIDGE table from silver.
        Selects the two FK columns that link parent fact to child dim.
        """
        source = table.source_table
        target = table.full_target

        if table.columns:
            fk_cols = [c for c in table.active_columns if c.is_foreign_key]
            if fk_cols:
                cols = ", ".join(
                    f"{c.source_column} AS {c.target_column}"
                    if c.source_column != c.target_column
                    else c.source_column
                    for c in fk_cols
                )
            else:
                cols = "*"
        else:
            cols = "*"

        return f"""
            CREATE OR REPLACE TABLE {target} AS
            SELECT DISTINCT
                {cols}
            FROM {source}
            WHERE 1=1
        """

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _build_join_clauses(self, relationships: list[dict]) -> list[str]:
        """
        Build JOIN clauses from relationship config.
        Looks up child table names from the registry.
        """
        clauses = []
        all_tables = {t.table_id: t for t in self.registry.get_active_tables()}

        for rel in relationships:
            child_id  = rel["child_table_id"]
            child_tbl = all_tables.get(child_id)
            if not child_tbl:
                continue

            join_type    = rel.get("join_type", "LEFT")
            parent_col   = rel["parent_column"]
            child_col    = rel["child_column"]
            child_target = child_tbl.full_target
            alias        = child_tbl.target_table[:3]

            clauses.append(
                f"{join_type} JOIN {child_target} {alias} "
                f"ON src.{parent_col} = {alias}.{child_col}"
            )

        return clauses

    def load_all_gold(self, source_name: Optional[str] = None) -> dict:
        """
        Load all registered gold tables in load_order sequence.
        Returns summary dict.
        """
        tables = self.registry.get_active_tables(
            source_name   = source_name,
            target_schema = "gold",
        )

        if not tables:
            log.warning("No active gold tables found in registry")
            return {}

        # Sort: dimensions first, then bridges, then facts
        order = {"DIMENSION": 1, "BRIDGE": 2, "FACT": 3}
        tables = sorted(tables, key=lambda t: (order.get(t.table_type, 9), t.load_order))

        log.info(f"Loading {len(tables)} gold table(s) ...")
        results = {}

        for table in tables:
            success, rows, error = self.load(table)
            results[table.full_target] = {
                "success": success,
                "rows":    rows,
                "error":   error,
            }

        return results