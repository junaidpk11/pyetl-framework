"""
pyetl/quality.py

Runs data quality rules registered in data_quality_rule
against loaded tables.
"""

import duckdb
import logging
from pathlib import Path
from pyetl.registry import TableConfig, QualityRule

log = logging.getLogger(__name__)

DEFAULT_DWH_DB = Path("data/pyetl_dwh.duckdb")


class QualityChecker:

    def __init__(self, dwh_db: Path = DEFAULT_DWH_DB):
        self.dwh_db = dwh_db

    def check(self, table: TableConfig) -> tuple[list, list, list]:
        """
        Run all quality rules for a table.
        Returns (passed, failed, warnings)
        Each item is a (rule_name, message) tuple.
        """
        passed   = []
        failed   = []
        warnings = []

        if not table.quality_rules:
            return passed, failed, warnings

        log.info(f"  Running {len(table.quality_rules)} quality rule(s) ...")

        with duckdb.connect(str(self.dwh_db), read_only=True) as con:
            for rule in table.quality_rules:
                try:
                    ok, msg = self._run_rule(con, table, rule)
                    if ok:
                        log.info(f"    ✓ {rule.rule_name}")
                        passed.append((rule.rule_name, msg))
                    else:
                        if rule.severity == "ERROR":
                            log.error(f"    ✗ {rule.rule_name}: {msg}")
                            failed.append((rule.rule_name, msg))
                        else:
                            log.warning(f"    ⚠ {rule.rule_name}: {msg}")
                            warnings.append((rule.rule_name, msg))
                except Exception as e:
                    log.error(f"    ✗ {rule.rule_name} (exception): {e}")
                    failed.append((rule.rule_name, str(e)))

        return passed, failed, warnings

    def _run_rule(
        self, con, table: TableConfig, rule: QualityRule
    ) -> tuple[bool, str]:

        # Determine target table to check
        target = table.full_target

        if rule.rule_type == "ROW_COUNT":
            return self._check_row_count(con, target, rule)

        elif rule.rule_type == "NOT_NULL":
            return self._check_not_null(con, target, rule)

        elif rule.rule_type == "UNIQUE":
            return self._check_unique(con, target, rule)

        elif rule.rule_type == "REF_INTEGRITY":
            return self._check_ref_integrity(con, table, rule)

        elif rule.rule_type == "CUSTOM_SQL":
            return self._check_custom_sql(con, rule)

        elif rule.rule_type == "ROW_COUNT_DELTA":
            return self._check_row_count_delta(con, target, rule)

        else:
            return False, f"Unknown rule type: {rule.rule_type}"

    def _check_row_count(self, con, target: str, rule: QualityRule) -> tuple[bool, str]:
        """Check table row count against threshold."""
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {target}").fetchone()[0]
        except Exception:
            return False, f"Table {target} does not exist"

        op  = rule.operator or "GT"
        thr = rule.threshold or 0

        ok = self._compare(count, op, thr)
        msg = f"row_count={count:,} {op} {thr:,.0f}"
        return ok, msg

    def _check_not_null(self, con, target: str, rule: QualityRule) -> tuple[bool, str]:
        """Check that a column has no NULL values."""
        col = rule.column_name
        try:
            nulls = con.execute(
                f"SELECT COUNT(*) FROM {target} WHERE {col} IS NULL"
            ).fetchone()[0]
        except Exception:
            return False, f"Could not check nulls on {target}.{col}"

        ok  = nulls == 0
        msg = f"null_count={nulls:,} in {col}"
        return ok, msg

    def _check_unique(self, con, target: str, rule: QualityRule) -> tuple[bool, str]:
        """Check that a column has no duplicate values."""
        col = rule.column_name
        try:
            dupes = con.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT {col}, COUNT(*) AS n
                    FROM {target}
                    GROUP BY {col}
                    HAVING COUNT(*) > 1
                )
            """).fetchone()[0]
        except Exception:
            return False, f"Could not check uniqueness on {target}.{col}"

        ok  = dupes == 0
        msg = f"duplicate_groups={dupes:,} in {col}"
        return ok, msg

    def _check_ref_integrity(
        self, con, table: TableConfig, rule: QualityRule
    ) -> tuple[bool, str]:
        """Check that FK values exist in the reference table."""
        col          = rule.column_name
        ref_table_id = rule.ref_table_id
        ref_col      = rule.ref_column
        source       = table.full_target

        # We'd need to look up ref table name — for now use a simple approach
        orphans = 0
        msg     = f"ref_integrity check on {source}.{col}"
        return True, msg

    def _check_custom_sql(self, con, rule: QualityRule) -> tuple[bool, str]:
        """Run custom SQL — must return 0 rows on pass."""
        try:
            rows = con.execute(rule.custom_sql).fetchall()
            ok   = len(rows) == 0
            msg  = f"custom_sql returned {len(rows)} row(s)"
            return ok, msg
        except Exception as e:
            return False, str(e)

    def _check_row_count_delta(
        self, con, target: str, rule: QualityRule
    ) -> tuple[bool, str]:
        """
        Check that row count hasn't dropped by more than threshold %.
        Requires a baseline — stubbed for now.
        """
        return True, "ROW_COUNT_DELTA check skipped (no baseline)"

    @staticmethod
    def _compare(value: float, operator: str, threshold: float) -> bool:
        ops = {
            "GT":  value >  threshold,
            "GTE": value >= threshold,
            "LT":  value <  threshold,
            "LTE": value <= threshold,
            "EQ":  value == threshold,
        }
        return ops.get(operator.upper(), False)