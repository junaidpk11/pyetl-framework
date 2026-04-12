"""
tests/test_registry.py

Unit tests for PyETLRegistryReader.
"""

import pytest
import duckdb
import json
from pathlib import Path

from control.schema import create_control_schema
from control.seed   import PyETLRegistry
from pyetl.registry import PyETLRegistryReader, ColumnConfig


TEST_DB = Path("data/test_registry.duckdb")


@pytest.fixture(scope="module", autouse=True)
def setup_test_db():
    """Create a fresh test control DB and seed minimal data."""
    TEST_DB.parent.mkdir(parents=True, exist_ok=True)
    if TEST_DB.exists():
        TEST_DB.unlink()

    create_control_schema(TEST_DB)

    r = PyETLRegistry(TEST_DB)
    src = r.register_source("TEST_SRC", "CSV", "data/raw", description="Test source")
    tbl = r.register_table(src, "test.csv", "bronze", "test_table", "STAGING", load_order=1)
    r.register_columns_bulk(tbl, [
        {"source_column": "id",         "target_column": "id",         "target_data_type": "INTEGER", "is_primary_key": True,  "ordinal_position": 1},
        {"source_column": "name",       "target_column": "name",       "target_data_type": "VARCHAR", "ordinal_position": 2},
        {"source_column": "status",     "target_column": "status_desc","target_data_type": "VARCHAR",
         "transformation_type": "CASE_DECODE",
         "transformation_value": '{"1":"Active","2":"Inactive"}',                                     "ordinal_position": 3},
        {"source_column": "created_dt", "target_column": "created_date","target_data_type": "DATE",
         "transformation_type": "CAST", "transformation_value": "%d/%m/%Y",                           "ordinal_position": 4},
    ])
    r.register_load_config(tbl, load_type="FULL", truncate_before_load=True)
    r.register_quality_rule(tbl, "min_rows", "ROW_COUNT", operator="GT", threshold=0, severity="ERROR")

    yield TEST_DB

    if TEST_DB.exists():
        TEST_DB.unlink()


@pytest.fixture
def reader(setup_test_db):
    return PyETLRegistryReader(setup_test_db)


def test_get_active_tables_returns_results(reader):
    tables = reader.get_active_tables()
    assert len(tables) > 0


def test_filter_by_source_name(reader):
    tables = reader.get_active_tables(source_name="TEST_SRC")
    assert len(tables) == 1
    assert tables[0].source_name == "TEST_SRC"


def test_filter_by_schema(reader):
    tables = reader.get_active_tables(target_schema="bronze")
    assert all(t.target_schema == "bronze" for t in tables)


def test_table_has_columns(reader):
    tables = reader.get_active_tables()
    assert len(tables[0].columns) == 4


def test_primary_key_detected(reader):
    tables = reader.get_active_tables()
    pks = tables[0].primary_keys
    assert "id" in pks


def test_column_ordinal_order(reader):
    tables = reader.get_active_tables()
    positions = [c.ordinal_position for c in tables[0].active_columns]
    assert positions == sorted(positions)


def test_case_decode_map_parsed(reader):
    tables  = reader.get_active_tables()
    col     = next(c for c in tables[0].columns if c.transformation_type == "CASE_DECODE")
    decoded = col.decoded_map()
    assert decoded == {"1": "Active", "2": "Inactive"}


def test_load_config_present(reader):
    tables = reader.get_active_tables()
    lc     = tables[0].load_config
    assert lc is not None
    assert lc.load_type == "FULL"
    assert lc.truncate_before_load is True


def test_quality_rules_present(reader):
    tables = reader.get_active_tables()
    rules  = tables[0].quality_rules
    assert len(rules) == 1
    assert rules[0].rule_type == "ROW_COUNT"


def test_full_target_property(reader):
    tables = reader.get_active_tables()
    assert tables[0].full_target == "bronze.test_table"