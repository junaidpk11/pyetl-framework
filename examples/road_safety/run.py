"""
examples/road_safety/run.py

Runs the complete Road Safety ETL pipeline:
  1. Initialise control schema
  2. Register source, tables, columns, relationships, and quality rules
  3. Extract CSV files → bronze
  4. Transform bronze → silver (TRIM, CAST, CASE_DECODE from config)
  5. Load silver → gold (star schema)
  6. Print final summary

Usage:
    cd ~/pyetl-framework
    python3 examples/road_safety/run.py

    # Bronze only:
    python3 examples/road_safety/run.py --bronze

    # Silver only (bronze must already exist):
    python3 examples/road_safety/run.py --silver

    # Gold only (silver must already exist):
    python3 examples/road_safety/run.py --gold
"""

import sys
import logging
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from control.schema import create_control_schema
from control.seed   import PyETLRegistry
from pyetl          import PyETLEngine, Loader, PyETLRegistryReader
from examples.road_safety.register import register

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s  %(levelname)s  %(message)s"
)
log = logging.getLogger(__name__)


def run_pipeline(bronze_only=False, silver_only=False, gold_only=False):

    log.info("=" * 60)
    log.info("  Road Safety ETL Pipeline")
    log.info("=" * 60 + "\n")

    # ── Step 1: Ensure control schema and registration ────────────────────────
    log.info("Step 1 — Initialising control framework ...")
    register()

    engine = PyETLEngine()

    # ── Step 2: Bronze ────────────────────────────────────────────────────────
    if not silver_only and not gold_only:
        log.info("\nStep 2 — Extracting sources → bronze ...")
        result = engine.run_bronze(source_name="ROAD_SAFETY_DFT")
        if result.overall_status == "FAILED":
            log.error("Bronze extraction failed — aborting")
            sys.exit(1)

    if bronze_only:
        log.info("\nBronze-only run complete.")
        return

    # ── Step 3: Silver ────────────────────────────────────────────────────────
    if not gold_only:
        log.info("\nStep 3 — Transforming bronze → silver ...")
        result = engine.run_silver(source_name="ROAD_SAFETY_DFT")
        if result.overall_status == "FAILED":
            log.error("Silver transform failed — aborting")
            sys.exit(1)

    if silver_only:
        log.info("\nSilver-only run complete.")
        return

    # ── Step 4: Gold ──────────────────────────────────────────────────────────
    log.info("\nStep 4 — Loading silver → gold (star schema) ...")
    loader  = Loader()
    results = loader.load_all_gold(source_name="ROAD_SAFETY_DFT")

    # ── Step 5: Summary ───────────────────────────────────────────────────────
    log.info("\n" + "=" * 60)
    log.info("  Final pipeline summary")
    log.info("=" * 60)

    import duckdb
    con = duckdb.connect("data/pyetl_dwh.duckdb", read_only=True)

    for schema in ["bronze", "silver", "gold"]:
        try:
            tables = con.execute(f"""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = '{schema}'
                ORDER BY table_name
            """).fetchall()
            log.info(f"\n  {schema.upper()} layer:")
            for (t,) in tables:
                count = con.execute(
                    f"SELECT COUNT(*) FROM {schema}.{t}"
                ).fetchone()[0]
                log.info(f"    {schema}.{t:<30} {count:>10,} rows")
        except Exception:
            pass

    con.close()
    log.info("\n" + "=" * 60)
    log.info("  Pipeline complete")
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Road Safety ETL Pipeline")
    parser.add_argument("--bronze", action="store_true", help="Extract bronze only")
    parser.add_argument("--silver", action="store_true", help="Transform silver only")
    parser.add_argument("--gold",   action="store_true", help="Load gold only")
    args = parser.parse_args()

    run_pipeline(
        bronze_only = args.bronze,
        silver_only = args.silver,
        gold_only   = args.gold,
    )