# pyetl-framework

> A metadata-driven ETL framework for building data warehouses in Python. Register your sources, tables, and columns in a control database — the engine handles extraction, transformation, loading, and quality checks automatically.

---

## What this is

pyetl is a generic, configurable ETL engine. It replaces hardcoded pipeline scripts with a control-database approach — similar to enterprise tools like Informatica or SSIS metadata repositories, but built entirely in Python and open source.

**Key principle:** adding a new source table means inserting rows into the control database — not writing new code.

---

## Architecture

```
pyetl_control.duckdb          pyetl_dwh.duckdb
─────────────────────         ────────────────────────────
source_registry               bronze.*   (raw extracts)
table_registry          →     silver.*   (cleaned, typed)
column_registry               gold.*     (star schema)
transformation_rule
table_relationship
load_config
data_quality_rule
```

---

## Quickstart

```bash
git clone https://github.com/junaidpk11/pyetl-framework.git
cd pyetl-framework
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt && pip install -e .
python3 examples/road_safety/run.py
```

---

## Transformation types

| Type | What it does |
|---|---|
| RENAME | Rename column. VARCHAR columns trimmed automatically. |
| CAST | Cast to target type. Supports date format strings. |
| CASE_DECODE | Decode codes to labels via JSON map. |
| CALCULATED | Arbitrary SQL expression. |
| CONSTANT | Fixed value for every row. |

See [docs/configuration.md](docs/configuration.md) for full configuration guide.

---

## License
MIT