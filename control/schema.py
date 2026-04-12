"""
control/schema.py

Creates and manages the pyetl control database.
Seven tables that describe every aspect of the pipeline — no hardcoding needed.
"""

import duckdb
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)
log = logging.getLogger(__name__)

DEFAULT_CONTROL_DB = Path("data/pyetl_control.duckdb")


def create_control_schema(db_path: Path = DEFAULT_CONTROL_DB):
    """Create all seven control tables."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    log.info(f"Initialising control schema at {db_path} ...\n")

    with duckdb.connect(str(db_path)) as con:

        # ── 1. source_registry ────────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS source_registry (
                source_id         INTEGER PRIMARY KEY,
                source_name       VARCHAR NOT NULL UNIQUE,
                source_type       VARCHAR NOT NULL,
                -- CSV | EXCEL | DUCKDB | SQL_SERVER | ORACLE | API
                connection_string VARCHAR,
                -- file path, DB path, or env var reference e.g. ENV:MY_CONN
                schema_name       VARCHAR,
                database_name     VARCHAR,
                is_active         BOOLEAN DEFAULT true,
                load_priority     INTEGER DEFAULT 1,
                description       VARCHAR,
                created_at        TIMESTAMP DEFAULT now(),
                updated_at        TIMESTAMP DEFAULT now()
            )
        """)
        log.info("  ✓ source_registry")

        # ── 2. table_registry ─────────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS table_registry (
                table_id          INTEGER PRIMARY KEY,
                source_id         INTEGER NOT NULL,
                source_table      VARCHAR NOT NULL,
                -- file name, table name, or API endpoint
                target_schema     VARCHAR NOT NULL,
                -- bronze | silver | gold
                target_table      VARCHAR NOT NULL,
                table_type        VARCHAR NOT NULL,
                -- FACT | DIMENSION | BRIDGE | STAGING
                load_order        INTEGER DEFAULT 1,
                is_active         BOOLEAN DEFAULT true,
                description       VARCHAR,
                created_at        TIMESTAMP DEFAULT now(),
                updated_at        TIMESTAMP DEFAULT now()
            )
        """)
        log.info("  ✓ table_registry")

        # ── 3. column_registry ────────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS column_registry (
                column_id            INTEGER PRIMARY KEY,
                table_id             INTEGER NOT NULL,
                source_column        VARCHAR NOT NULL,
                target_column        VARCHAR NOT NULL,
                source_data_type     VARCHAR,
                target_data_type     VARCHAR,
                -- VARCHAR | INTEGER | DOUBLE | DATE | TIME | BOOLEAN
                transformation_type  VARCHAR DEFAULT 'RENAME',
                -- RENAME | CAST | CASE_DECODE | CALCULATED | CONSTANT | LOOKUP
                transformation_value VARCHAR,
                -- for CAST: target type e.g. DATE
                -- for CASE_DECODE: JSON {"1":"Fatal","2":"Serious"}
                -- for CALCULATED: SQL expression e.g. "first||' '||last"
                -- for CONSTANT: fixed value e.g. "GBP"
                -- for LOOKUP: "ref_table.key_col.value_col"
                is_primary_key       BOOLEAN DEFAULT false,
                is_foreign_key       BOOLEAN DEFAULT false,
                is_nullable          BOOLEAN DEFAULT true,
                is_active            BOOLEAN DEFAULT true,
                ordinal_position     INTEGER DEFAULT 1,
                default_value        VARCHAR,
                description          VARCHAR,
                created_at           TIMESTAMP DEFAULT now()
            )
        """)
        log.info("  ✓ column_registry")

        # ── 4. transformation_rule ────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS transformation_rule (
                rule_id          INTEGER PRIMARY KEY,
                rule_name        VARCHAR NOT NULL UNIQUE,
                rule_type        VARCHAR NOT NULL,
                -- CASE_DECODE | CALCULATED | DATE_FORMAT | LOOKUP
                rule_definition  VARCHAR NOT NULL,
                -- CASE_DECODE: JSON {"source_val":"target_label",...}
                -- CALCULATED:  SQL expression referencing source columns
                -- DATE_FORMAT: strptime format e.g. '%d/%m/%Y'
                -- LOOKUP:      "schema.table.key_col.value_col"
                is_active        BOOLEAN DEFAULT true,
                description      VARCHAR,
                created_at       TIMESTAMP DEFAULT now()
            )
        """)
        log.info("  ✓ transformation_rule")

        # ── 5. table_relationship ─────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS table_relationship (
                relationship_id   INTEGER PRIMARY KEY,
                parent_table_id   INTEGER NOT NULL,
                -- e.g. fact_collisions
                parent_column     VARCHAR NOT NULL,
                -- e.g. date_key
                child_table_id    INTEGER NOT NULL,
                -- e.g. dim_date
                child_column      VARCHAR NOT NULL,
                -- e.g. date_key
                relationship_type VARCHAR NOT NULL,
                -- FACT_TO_DIM | FACT_TO_BRIDGE | BRIDGE_TO_DIM
                join_type         VARCHAR DEFAULT 'LEFT',
                -- LEFT | INNER
                is_active         BOOLEAN DEFAULT true,
                description       VARCHAR,
                created_at        TIMESTAMP DEFAULT now()
            )
        """)
        log.info("  ✓ table_relationship")

        # ── 6. load_config ────────────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS load_config (
                config_id              INTEGER PRIMARY KEY,
                table_id               INTEGER NOT NULL UNIQUE,
                load_type              VARCHAR NOT NULL DEFAULT 'FULL',
                -- FULL | INCREMENTAL | SCD1 | SCD2
                watermark_column       VARCHAR,
                watermark_value        VARCHAR,
                batch_size             INTEGER DEFAULT 500000,
                truncate_before_load   BOOLEAN DEFAULT false,
                -- SCD2 columns
                scd_key_columns        VARCHAR,
                -- JSON list of business key cols e.g. ["employee_id"]
                scd_track_columns      VARCHAR,
                -- JSON list of cols to track for changes
                scd_effective_from_col VARCHAR DEFAULT 'effective_from',
                scd_effective_to_col   VARCHAR DEFAULT 'effective_to',
                scd_is_current_col     VARCHAR DEFAULT 'is_current',
                -- Partitioning for large tables
                partition_column       VARCHAR,
                partition_count        INTEGER DEFAULT 4,
                created_at             TIMESTAMP DEFAULT now(),
                updated_at             TIMESTAMP DEFAULT now()
            )
        """)
        log.info("  ✓ load_config")

        # ── 7. data_quality_rule ──────────────────────────────────────────────
        con.execute("""
            CREATE TABLE IF NOT EXISTS data_quality_rule (
                rule_id       INTEGER PRIMARY KEY,
                table_id      INTEGER NOT NULL,
                rule_name     VARCHAR NOT NULL,
                rule_type     VARCHAR NOT NULL,
                -- NOT_NULL | UNIQUE | ROW_COUNT | ROW_COUNT_DELTA
                -- REF_INTEGRITY | CUSTOM_SQL
                column_name   VARCHAR,
                -- column to check (NOT_NULL, UNIQUE)
                operator      VARCHAR,
                -- GT | LT | EQ | GTE | LTE | BETWEEN
                threshold     DOUBLE,
                -- numeric threshold e.g. row count > 1000
                threshold_max DOUBLE,
                -- for BETWEEN
                ref_table_id  INTEGER,
                -- for REF_INTEGRITY
                ref_column    VARCHAR,
                custom_sql    VARCHAR,
                -- for CUSTOM_SQL — must return 0 rows on pass
                severity      VARCHAR DEFAULT 'ERROR',
                -- ERROR (stop pipeline) | WARNING (log and continue)
                is_active     BOOLEAN DEFAULT true,
                description   VARCHAR,
                created_at    TIMESTAMP DEFAULT now()
            )
        """)
        log.info("  ✓ data_quality_rule")

        # ── Summary ───────────────────────────────────────────────────────────
        tables = con.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            ORDER BY table_name
        """).fetchall()

        log.info(f"\n  Control schema ready — {len(tables)} tables:")
        for (t,) in tables:
            log.info(f"    - {t}")

    log.info(f"\nControl DB: {db_path.resolve()}")


if __name__ == "__main__":
    create_control_schema()