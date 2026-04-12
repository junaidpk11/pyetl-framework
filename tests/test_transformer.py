"""
tests/test_transformer.py

Unit tests for the Transformer — verifies generated SQL
for all transformation types.
"""

import pytest
from unittest.mock import MagicMock
from pyetl.registry    import TableConfig, ColumnConfig, LoadConfig
from pyetl.transformer import Transformer


def make_col(
    source_column,
    target_column,
    source_data_type   = "VARCHAR",
    target_data_type   = "VARCHAR",
    transformation_type= "RENAME",
    transformation_value=None,
    is_primary_key     = False,
    is_foreign_key     = False,
    is_nullable        = True,
    ordinal_position   = 1,
):
    return ColumnConfig(
        column_id            = 1,
        source_column        = source_column,
        target_column        = target_column,
        source_data_type     = source_data_type,
        target_data_type     = target_data_type,
        transformation_type  = transformation_type,
        transformation_value = transformation_value,
        is_primary_key       = is_primary_key,
        is_foreign_key       = is_foreign_key,
        is_nullable          = is_nullable,
        ordinal_position     = ordinal_position,
        default_value        = None,
    )


def make_table(columns, source_table="test.csv", target_schema="silver", target_table="test"):
    return TableConfig(
        table_id      = 1,
        source_id     = 1,
        source_name   = "TEST",
        source_type   = "CSV",
        connection    = "data/raw",
        source_table  = source_table,
        target_schema = target_schema,
        target_table  = target_table,
        table_type    = "STAGING",
        load_order    = 1,
        columns       = columns,
        load_config   = None,
        quality_rules = [],
        relationships = [],
    )


@pytest.fixture
def transformer():
    return Transformer()


def test_rename_varchar_adds_trim(transformer):
    col = make_col("first_name", "first_name", target_data_type="VARCHAR")
    expr = transformer._expr_rename(col)
    assert "TRIM" in expr
    assert "first_name" in expr


def test_rename_integer_no_trim(transformer):
    col = make_col("age", "age", target_data_type="INTEGER", transformation_type="RENAME")
    expr = transformer._expr_rename(col)
    assert "TRIM" not in expr
    assert expr == "age"


def test_rename_different_names(transformer):
    col = make_col("src_name", "tgt_name", target_data_type="VARCHAR")
    expr = transformer._expr_rename(col)
    assert "AS tgt_name" in expr


def test_cast_date_with_format(transformer):
    col = make_col("date", "event_date",
                   target_data_type="DATE",
                   transformation_type="CAST",
                   transformation_value="%d/%m/%Y")
    expr = transformer._expr_cast(col)
    assert "STRPTIME" in expr
    assert "%d/%m/%Y" in expr
    assert "AS event_date" in expr


def test_cast_date_without_format(transformer):
    col = make_col("date", "event_date",
                   target_data_type="DATE",
                   transformation_type="CAST")
    expr = transformer._expr_cast(col)
    assert "STRPTIME" not in expr
    assert "TRY_CAST" in expr
    assert "DATE" in expr


def test_cast_integer(transformer):
    col = make_col("age_str", "age",
                   source_data_type="VARCHAR",
                   target_data_type="INTEGER",
                   transformation_type="CAST")
    expr = transformer._expr_cast(col)
    assert "TRY_CAST" in expr
    assert "INTEGER" in expr
    assert "TRIM" in expr


def test_case_decode_generates_case_when(transformer):
    col = make_col("severity_code", "severity",
                   target_data_type="VARCHAR",
                   transformation_type="CASE_DECODE",
                   transformation_value='{"1":"Fatal","2":"Serious","3":"Slight"}')
    expr = transformer._expr_case_decode(col)
    assert "CASE" in expr
    assert "'Fatal'" in expr
    assert "'Serious'" in expr
    assert "'Slight'" in expr
    assert "ELSE 'Unknown'" in expr
    assert "AS severity" in expr


def test_case_decode_trims_source(transformer):
    col = make_col("code", "label",
                   transformation_type="CASE_DECODE",
                   transformation_value='{"1":"Yes","0":"No"}')
    expr = transformer._expr_case_decode(col)
    assert "TRIM" in expr


def test_calculated_expression(transformer):
    col = make_col("first_name", "full_name",
                   transformation_type="CALCULATED",
                   transformation_value="TRIM(first_name) || ' ' || TRIM(last_name)")
    expr = transformer._expr_calculated(col)
    assert "TRIM(first_name) || ' ' || TRIM(last_name)" in expr
    assert "AS full_name" in expr


def test_constant_string(transformer):
    col = make_col("currency", "currency",
                   transformation_type="CONSTANT",
                   transformation_value="GBP")
    expr = transformer._expr_constant(col)
    assert "'GBP'" in expr
    assert "AS currency" in expr


def test_constant_null(transformer):
    col = make_col("unknown", "unknown",
                   transformation_type="CONSTANT",
                   transformation_value="NULL")
    expr = transformer._expr_constant(col)
    assert "NULL" in expr


def test_preview_sql_contains_silver_target(transformer):
    columns = [
        make_col("id",   "id",   target_data_type="INTEGER", ordinal_position=1),
        make_col("name", "name", target_data_type="VARCHAR", ordinal_position=2),
    ]
    table = make_table(columns, source_table="source.csv", target_schema="bronze", target_table="my_table")
    sql   = transformer.preview_sql(table)
    assert "silver.my_table" in sql
    assert "bronze.source" in sql
    assert "CREATE OR REPLACE TABLE" in sql


def test_preview_sql_no_columns_copies_source(transformer):
    table = make_table([], source_table="raw.csv", target_schema="bronze", target_table="raw")
    sql   = transformer.preview_sql(table)
    assert "SELECT *" in sql