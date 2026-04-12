"""
tests/test_quality.py

Unit tests for the QualityChecker.
"""

import pytest
import duckdb
from pathlib import Path
from pyetl.registry import TableConfig, QualityRule
from pyetl.quality  import QualityChecker


TEST_DWH = Path("data/test_quality.duckdb")


@pytest.fixture(scope="module")
def test_db():
    TEST_DWH.parent.mkdir(parents=True, exist_ok=True)
    if TEST_DWH.exists():
        TEST_DWH.unlink()

    con = duckdb.connect(str(TEST_DWH))
    con.execute("CREATE SCHEMA test")
    con.execute("""
        CREATE TABLE test.sample AS
        SELECT * FROM (VALUES
            (1, 'Alice',   'active'),
            (2, 'Bob',     'inactive'),
            (3, 'Charlie', 'active'),
            (4, NULL,      'active')
        ) t(id, name, status)
    """)
    con.close()
    yield TEST_DWH

    if TEST_DWH.exists():
        TEST_DWH.unlink()


def make_table(quality_rules):
    return TableConfig(
        table_id      = 1,
        source_id     = 1,
        source_name   = "TEST",
        source_type   = "CSV",
        connection    = "",
        source_table  = "sample",
        target_schema = "test",
        target_table  = "sample",
        table_type    = "STAGING",
        load_order    = 1,
        columns       = [],
        load_config   = None,
        quality_rules = quality_rules,
        relationships = [],
    )


def make_rule(rule_type, rule_name="test_rule", column_name=None,
              operator=None, threshold=None, custom_sql=None,
              severity="ERROR", ref_table_id=None, ref_column=None):
    return QualityRule(
        rule_id       = 1,
        rule_name     = rule_name,
        rule_type     = rule_type,
        column_name   = column_name,
        operator      = operator,
        threshold     = threshold,
        threshold_max = None,
        ref_table_id  = ref_table_id,
        ref_column    = ref_column,
        custom_sql    = custom_sql,
        severity      = severity,
    )


@pytest.fixture
def checker(test_db):
    return QualityChecker(test_db)


def test_row_count_pass(checker):
    rule  = make_rule("ROW_COUNT", operator="GT", threshold=0)
    table = make_table([rule])
    passed, failed, warnings = checker.check(table)
    assert len(passed) == 1
    assert len(failed) == 0


def test_row_count_fail(checker):
    rule  = make_rule("ROW_COUNT", operator="GT", threshold=1000)
    table = make_table([rule])
    passed, failed, warnings = checker.check(table)
    assert len(failed) == 1


def test_row_count_warning_not_error(checker):
    rule  = make_rule("ROW_COUNT", operator="GT", threshold=1000, severity="WARNING")
    table = make_table([rule])
    passed, failed, warnings = checker.check(table)
    assert len(failed) == 0
    assert len(warnings) == 1


def test_not_null_fail_when_nulls_exist(checker):
    rule  = make_rule("NOT_NULL", column_name="name")
    table = make_table([rule])
    passed, failed, warnings = checker.check(table)
    assert len(failed) == 1
    assert "null_count=1" in failed[0][1]


def test_not_null_pass_on_non_null_column(checker):
    rule  = make_rule("NOT_NULL", column_name="id")
    table = make_table([rule])
    passed, failed, warnings = checker.check(table)
    assert len(passed) == 1


def test_unique_pass(checker):
    rule  = make_rule("UNIQUE", column_name="id")
    table = make_table([rule])
    passed, failed, warnings = checker.check(table)
    assert len(passed) == 1


def test_unique_fail_on_duplicates(checker):
    rule  = make_rule("UNIQUE", column_name="status")
    table = make_table([rule])
    passed, failed, warnings = checker.check(table)
    assert len(failed) == 1


def test_custom_sql_pass(checker):
    rule  = make_rule("CUSTOM_SQL",
                      custom_sql="SELECT id FROM test.sample WHERE id < 0")
    table = make_table([rule])
    passed, failed, warnings = checker.check(table)
    assert len(passed) == 1


def test_custom_sql_fail(checker):
    rule  = make_rule("CUSTOM_SQL",
                      custom_sql="SELECT id FROM test.sample WHERE id IS NOT NULL")
    table = make_table([rule])
    passed, failed, warnings = checker.check(table)
    assert len(failed) == 1


def test_multiple_rules(checker):
    rules = [
        make_rule("ROW_COUNT",  "row_check",  operator="GT", threshold=0),
        make_rule("NOT_NULL",   "null_check", column_name="id"),
        make_rule("UNIQUE",     "uniq_check", column_name="id"),
    ]
    table = make_table(rules)
    passed, failed, warnings = checker.check(table)
    assert len(passed) == 3
    assert len(failed) == 0