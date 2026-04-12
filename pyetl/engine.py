"""
pyetl/engine.py

Main ETL engine — orchestrates extract, transform, load,
and quality checks for all registered tables.

Usage:
    from pyetl.engine import PyETLEngine
    engine = PyETLEngine()
    engine.run()                          # full pipeline
    engine.run(target_schema="bronze")    # bronze only
    engine.run(source_name="MY_SOURCE")  # one source only
    engine.run(table_ids=[1, 2, 3])      # specific tables
"""

import logging
import traceback
from pathlib import Path
from datetime import datetime
from typing import Optional

from pyetl.registry import PyETLRegistryReader, TableConfig
from pyetl.extractor import Extractor
from pyetl.transformer import Transformer
from pyetl.quality import QualityChecker

log = logging.getLogger(__name__)

DEFAULT_CONTROL_DB = Path("data/pyetl_control.duckdb")
DEFAULT_DWH_DB     = Path("data/pyetl_dwh.duckdb")


class RunResult:
    """Tracks results for a single pipeline run."""

    def __init__(self):
        self.started_at    = datetime.now()
        self.completed_at  = None
        self.tables        = []
        self.success_count = 0
        self.failed_count  = 0
        self.skipped_count = 0
        self.total_rows    = 0

    def add(self, table_name: str, phase: str, success: bool,
            rows: int = 0, error: str = None):
        self.tables.append({
            "table":   table_name,
            "phase":   phase,
            "success": success,
            "rows":    rows,
            "error":   error,
        })
        if success:
            self.success_count += 1
            self.total_rows    += rows
        else:
            self.failed_count  += 1

    def complete(self):
        self.completed_at = datetime.now()

    @property
    def duration_secs(self) -> float:
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return 0.0

    @property
    def overall_status(self) -> str:
        if self.failed_count == 0:
            return "SUCCESS"
        if self.success_count > 0:
            return "PARTIAL"
        return "FAILED"

    def print_summary(self):
        log.info("\n=== Pipeline run summary ===")
        log.info(
            f"  Status:   {self.overall_status}  |  "
            f"Duration: {self.duration_secs:.1f}s  |  "
            f"Rows: {self.total_rows:,}"
        )
        log.info(
            f"  Tables:   success={self.success_count}  "
            f"failed={self.failed_count}  "
            f"skipped={self.skipped_count}"
        )
        if self.failed_count > 0:
            log.info("\n  Failed tables:")
            for t in self.tables:
                if not t["success"]:
                    log.info(f"    ✗ [{t['phase']}] {t['table']}: {t['error']}")


class PyETLEngine:

    def __init__(
        self,
        control_db: Path = DEFAULT_CONTROL_DB,
        dwh_db:     Path = DEFAULT_DWH_DB,
    ):
        self.registry  = PyETLRegistryReader(control_db)
        self.extractor = Extractor(dwh_db)
        self.transformer = Transformer(dwh_db)
        self.quality   = QualityChecker(dwh_db)
        self.dwh_db    = dwh_db

    def run(
        self,
        source_name:   Optional[str]       = None,
        target_schema: Optional[str]       = None,
        table_ids:     Optional[list[int]] = None,
    ) -> RunResult:
        """
        Run the full ETL pipeline.
        Optionally filter by source_name, target_schema, or specific table_ids.
        """
        result = RunResult()
        log.info("=" * 60)
        log.info("  PyETL Engine — pipeline started")
        log.info(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log.info("=" * 60 + "\n")

        # ── Get tables ────────────────────────────────────────────────────────
        tables = self.registry.get_active_tables(
            source_name   = source_name,
            target_schema = target_schema,
        )

        if table_ids:
            tables = [t for t in tables if t.table_id in table_ids]

        if not tables:
            log.warning("No active tables found — check registry config")
            result.complete()
            return result

        log.info(f"Found {len(tables)} table(s) to process\n")

        # ── Process each table ────────────────────────────────────────────────
        for table in tables:
            log.info(f"[{table.table_id}] {table.source_table} → {table.full_target} ({table.table_type})")

            # Phase 1 — Extract (bronze only)
            if table.target_schema == "bronze":
                success, extracted, loaded, error = self._run_extract(table, result)
                if not success:
                    log.warning(f"  Skipping quality checks for {table.full_target} due to extract failure\n")
                    continue

            # Phase 2 — Transform (bronze → silver)
            elif table.target_schema == "silver" or (
                table.target_schema == "bronze" and table.table_type == "STAGING"
            ):
                success, rows, error = self._run_transform(table, result)
                if not success:
                    continue

            # Phase 3 — Quality checks
            self._run_quality(table, result)
            log.info("")

        result.complete()
        result.print_summary()
        return result

    def run_bronze(self, source_name: Optional[str] = None) -> RunResult:
        """Extract all active bronze tables."""
        return self.run(source_name=source_name, target_schema="bronze")

    def run_silver(self, source_name: Optional[str] = None) -> RunResult:
        """Transform bronze → silver for all active tables."""
        result = RunResult()
        log.info("=== Silver transform started ===\n")

        tables = self.registry.get_active_tables(
            source_name   = source_name,
            target_schema = "bronze",
        )

        for table in tables:
            log.info(f"[{table.table_id}] {table.source_table} → silver.{table.target_table}")
            self._run_transform(table, result)
            self._run_quality(table, result)
            log.info("")

        result.complete()
        result.print_summary()
        return result

    def run_full(self, source_name: Optional[str] = None) -> RunResult:
        """
        Run complete pipeline: extract → transform → quality.
        Bronze tables are extracted then transformed to silver.
        """
        result = RunResult()
        log.info("=" * 60)
        log.info("  PyETL Engine — FULL pipeline run")
        log.info(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log.info("=" * 60 + "\n")

        tables = self.registry.get_active_tables(
            source_name   = source_name,
            target_schema = "bronze",
        )

        if not tables:
            log.warning("No active bronze tables found")
            result.complete()
            return result

        log.info(f"Found {len(tables)} bronze table(s)\n")

        for table in tables:
            log.info(
                f"[{table.table_id}] {table.source_table} → "
                f"bronze.{table.target_table} → silver.{table.target_table}"
            )

            # Step 1 — Extract to bronze
            success, extracted, loaded, error = self._run_extract(table, result)
            if not success:
                log.warning(f"  Skipping transform for {table.target_table}\n")
                continue

            # Step 2 — Quality check on bronze
            self._run_quality(table, result)

            # Step 3 — Transform to silver
            self._run_transform(table, result)

            log.info("")

        result.complete()
        result.print_summary()
        return result

    # ── Internal phase runners ────────────────────────────────────────────────

    def _run_extract(
        self, table: TableConfig, result: RunResult
    ) -> tuple[bool, int, int, Optional[str]]:
        try:
            success, extracted, loaded, error = self.extractor.extract(table)
            result.add(
                table_name = table.full_target,
                phase      = "EXTRACT",
                success    = success,
                rows       = loaded,
                error      = error,
            )
            return success, extracted, loaded, error
        except Exception as e:
            error = str(e)
            log.error(f"  Extract exception: {error}")
            result.add(table.full_target, "EXTRACT", False, 0, error)
            return False, 0, 0, error

    def _run_transform(
        self, table: TableConfig, result: RunResult
    ) -> tuple[bool, int, Optional[str]]:
        try:
            success, rows, error = self.transformer.transform(table)
            result.add(
                table_name = f"silver.{table.target_table}",
                phase      = "TRANSFORM",
                success    = success,
                rows       = rows,
                error      = error,
            )
            return success, rows, error
        except Exception as e:
            error = str(e)
            log.error(f"  Transform exception: {error}")
            result.add(f"silver.{table.target_table}", "TRANSFORM", False, 0, error)
            return False, 0, error

    def _run_quality(self, table: TableConfig, result: RunResult):
        if not table.quality_rules:
            return
        try:
            passed, failed, warnings = self.quality.check(table)
            for rule_name, error in failed:
                result.add(
                    table_name = table.full_target,
                    phase      = "QUALITY",
                    success    = False,
                    error      = f"{rule_name}: {error}",
                )
            for rule_name, msg in warnings:
                log.warning(f"  Quality warning [{rule_name}]: {msg}")
        except Exception as e:
            log.error(f"  Quality check exception: {e}")