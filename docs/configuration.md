# Configuring a new data source

This guide walks through registering a new source, tables, columns, relationships, load config, and quality rules in the pyetl control database.

---

## Overview

pyetl is metadata-driven. Everything about your pipeline — what to extract, how to transform it, how to load it, and what to validate — is registered in the control database (`data/pyetl_control.duckdb`). No code changes are needed to add a new source or table.

---

## Step 1 — Register the source

```python
from control.seed import PyETLRegistry

r = PyETLRegistry()

source_id = r.register_source(
    source_name       = "MY_SYSTEM",
    source_type       = "CSV",          # CSV | EXCEL | DUCKDB | SQL_SERVER | ORACLE | API
    connection_string = "data/raw",     # file path, DB path, or ENV:VAR_NAME
    load_priority     = 1,              # lower = runs first
    description       = "My data source"
)
```

---

## Step 2 — Register tables

Register each fact, dimension, bridge, or staging table:

```python
table_id = r.register_table(
    source_id     = source_id,
    source_table  = "customers.csv",    # file name or DB table name
    target_schema = "bronze",           # bronze | silver | gold
    target_table  = "customers",        # target table name
    table_type    = "DIMENSION",        # FACT | DIMENSION | BRIDGE | STAGING
    load_order    = 1,                  # within source — lower runs first
    description   = "Customer master"
)
```

---

## Step 3 — Register columns

Use `register_columns_bulk` for multiple columns at once:

```python
r.register_columns_bulk(table_id, [
    {
        "source_column":       "cust_id",
        "target_column":       "customer_id",
        "source_data_type":    "INTEGER",
        "target_data_type":    "INTEGER",
        "transformation_type": "RENAME",
        "is_primary_key":      True,
        "ordinal_position":    1,
    },
    {
        "source_column":       "cust_name",
        "target_column":       "customer_name",
        "target_data_type":    "VARCHAR",
        "transformation_type": "RENAME",     # trims automatically
        "ordinal_position":    2,
    },
    {
        "source_column":        "dob",
        "target_column":        "date_of_birth",
        "target_data_type":     "DATE",
        "transformation_type":  "CAST",
        "transformation_value": "%d/%m/%Y",  # date format
        "ordinal_position":     3,
    },
    {
        "source_column":         "status_code",
        "target_column":         "status",
        "target_data_type":      "VARCHAR",
        "transformation_type":   "CASE_DECODE",
        "transformation_value":  '{"1":"Active","2":"Inactive","3":"Pending"}',
        "ordinal_position":      4,
    },
    {
        "source_column":         "full_name",
        "target_column":         "full_name",
        "target_data_type":      "VARCHAR",
        "transformation_type":   "CALCULATED",
        "transformation_value":  "TRIM(first_name) || ' ' || TRIM(last_name)",
        "ordinal_position":      5,
    },
    {
        "source_column":         "currency",
        "target_column":         "currency",
        "target_data_type":      "VARCHAR",
        "transformation_type":   "CONSTANT",
        "transformation_value":  "GBP",
        "ordinal_position":      6,
    },
])
```

### Transformation types

| Type | What it does | transformation_value |
|---|---|---|
| `RENAME` | Rename column. VARCHAR columns are trimmed automatically. | Not required |
| `CAST` | Cast to target data type. | Date format string e.g. `%d/%m/%Y` (optional) |
| `CASE_DECODE` | Decode numeric/coded values to labels. | JSON map `{"code":"label"}` |
| `CALCULATED` | Arbitrary SQL expression. | SQL expression referencing source columns |
| `CONSTANT` | Fixed value for every row. | The constant value e.g. `GBP` or `NULL` |
| `LOOKUP` | Resolve FK to label via reference table. | `schema.table.key_col.value_col` |

---

## Step 4 — Register load config

```python
r.register_load_config(
    table_id             = table_id,
    load_type            = "FULL",      # FULL | INCREMENTAL | SCD1 | SCD2
    truncate_before_load = True,

    # For INCREMENTAL:
    watermark_column     = "updated_at",
    watermark_value      = None,        # populated automatically after first run
    batch_size           = 500000,

    # For SCD2:
    scd_key_columns      = ["customer_id"],
    scd_track_columns    = ["customer_name", "status"],
)
```

---

## Step 5 — Register relationships (gold layer)

Register FK relationships between fact and dimension tables:

```python
r.register_relationship(
    parent_table_id   = fact_table_id,
    parent_column     = "customer_key",
    child_table_id    = dim_customer_id,
    child_column      = "customer_key",
    relationship_type = "FACT_TO_DIM",  # FACT_TO_DIM | FACT_TO_BRIDGE | BRIDGE_TO_DIM
    join_type         = "LEFT",
)
```

---

## Step 6 — Register data quality rules

```python
# Row count must be above threshold
r.register_quality_rule(
    table_id    = table_id,
    rule_name   = "customers_min_rows",
    rule_type   = "ROW_COUNT",
    operator    = "GT",
    threshold   = 100,
    severity    = "ERROR",   # ERROR = stop pipeline | WARNING = log and continue
)

# Column must not have NULLs
r.register_quality_rule(
    table_id    = table_id,
    rule_name   = "customer_id_not_null",
    rule_type   = "NOT_NULL",
    column_name = "customer_id",
    severity    = "ERROR",
)

# Column values must be unique
r.register_quality_rule(
    table_id    = table_id,
    rule_name   = "customer_id_unique",
    rule_type   = "UNIQUE",
    column_name = "customer_id",
    severity    = "ERROR",
)

# Custom SQL — must return 0 rows on pass
r.register_quality_rule(
    table_id   = table_id,
    rule_name  = "no_future_dates",
    rule_type  = "CUSTOM_SQL",
    custom_sql = "SELECT id FROM bronze.customers WHERE date_of_birth > CURRENT_DATE",
    severity   = "WARNING",
)
```

---

## Step 7 — Run the pipeline

```python
from pyetl import PyETLEngine, Loader

engine = PyETLEngine()

# Extract + transform (bronze → silver)
engine.run_full(source_name="MY_SYSTEM")

# Load gold layer
loader = Loader()
loader.load_all_gold(source_name="MY_SYSTEM")
```

Or run via CLI:

```bash
python3 examples/road_safety/run.py
python3 examples/road_safety/run.py --bronze
python3 examples/road_safety/run.py --silver
python3 examples/road_safety/run.py --gold
```

---

## Disabling a table

To stop a table from loading without deleting its config:

```sql
UPDATE table_registry SET is_active = false WHERE target_table = 'customers';
```

To re-enable:

```sql
UPDATE table_registry SET is_active = true WHERE target_table = 'customers';
```

---

## Viewing registered config

```python
from pyetl import PyETLRegistryReader
r = PyETLRegistryReader()
r.print_summary()
```

Or query the control DB directly:

```bash
duckdb data/pyetl_control.duckdb "SELECT * FROM table_registry;"
duckdb data/pyetl_control.duckdb "SELECT * FROM column_registry WHERE table_id = 1;"
duckdb data/pyetl_control.duckdb "SELECT * FROM data_quality_rule;"
```