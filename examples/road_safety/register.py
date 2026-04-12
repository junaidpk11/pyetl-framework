"""
examples/road_safety/register.py

Registers the UK Road Safety dataset into the pyetl control database.
This is a real-world example showing how to configure:
  - 3 source CSV files
  - 1 fact table, 4 dimension tables (all in gold)
  - Full column mappings with CAST, CASE_DECODE, and RENAME transforms
  - Table relationships (fact to dims)
  - Load config for each table
  - Data quality rules
"""

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from control.schema import create_control_schema
from control.seed import PyETLRegistry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s"
)
log = logging.getLogger(__name__)


def register():
    log.info("=== Registering Road Safety dataset ===\n")

    # Ensure schema exists
    create_control_schema()

    r = PyETLRegistry()

    # ── Source ────────────────────────────────────────────────────────────────
    log.info("Registering source ...")
    source_id = r.register_source(
        source_name       = "ROAD_SAFETY_DFT",
        source_type       = "CSV",
        connection_string = "data/raw",
        load_priority     = 1,
        description       = "UK Dept for Transport — Road Safety open data"
    )

    # ── Bronze tables ─────────────────────────────────────────────────────────
    log.info("\nRegistering bronze tables ...")

    t_accidents = r.register_table(
        source_id     = source_id,
        source_table  = "accidents.csv",
        target_schema = "bronze",
        target_table  = "accidents",
        table_type    = "STAGING",
        load_order    = 1,
        description   = "Raw collision records"
    )
    r.register_load_config(t_accidents, load_type="FULL",
                           truncate_before_load=True)

    t_casualties = r.register_table(
        source_id     = source_id,
        source_table  = "casualties.csv",
        target_schema = "bronze",
        target_table  = "casualties",
        table_type    = "STAGING",
        load_order    = 2,
        description   = "Raw casualty records"
    )
    r.register_load_config(t_casualties, load_type="FULL",
                           truncate_before_load=True)

    t_vehicles = r.register_table(
        source_id     = source_id,
        source_table  = "vehicles.csv",
        target_schema = "bronze",
        target_table  = "vehicles",
        table_type    = "STAGING",
        load_order    = 3,
        description   = "Raw vehicle records"
    )
    r.register_load_config(t_vehicles, load_type="FULL",
                           truncate_before_load=True)

    # ── Columns — bronze.accidents ────────────────────────────────────────────
    log.info("\nRegistering bronze.accidents columns ...")
    r.register_columns_bulk(t_accidents, [
        {"source_column": "collision_index",      "target_column": "collision_index",      "target_data_type": "VARCHAR",  "is_primary_key": True,  "ordinal_position": 1},
        {"source_column": "collision_year",       "target_column": "collision_year",       "target_data_type": "INTEGER",  "ordinal_position": 2},
        {"source_column": "date",                 "target_column": "collision_date",       "target_data_type": "DATE",
         "transformation_type": "CAST",           "transformation_value": "%d/%m/%Y",      "ordinal_position": 3},
        {"source_column": "time",                 "target_column": "collision_time",       "target_data_type": "TIME",
         "transformation_type": "CAST",           "ordinal_position": 4},
        {"source_column": "collision_severity",   "target_column": "collision_severity",   "target_data_type": "VARCHAR",
         "transformation_type": "CASE_DECODE",
         "transformation_value": '{"1":"Fatal","2":"Serious","3":"Slight"}',               "ordinal_position": 5},
        {"source_column": "number_of_vehicles",   "target_column": "number_of_vehicles",   "target_data_type": "INTEGER",  "ordinal_position": 6},
        {"source_column": "number_of_casualties", "target_column": "number_of_casualties", "target_data_type": "INTEGER",  "ordinal_position": 7},
        {"source_column": "weather_conditions",   "target_column": "weather_conditions",   "target_data_type": "VARCHAR",
         "transformation_type": "CASE_DECODE",
         "transformation_value": '{"1":"Fine no high winds","2":"Raining no high winds","3":"Snowing no high winds","4":"Fine high winds","5":"Raining high winds","7":"Fog or mist","9":"Other"}',
         "ordinal_position": 8},
        {"source_column": "road_type",            "target_column": "road_type",            "target_data_type": "VARCHAR",
         "transformation_type": "CASE_DECODE",
         "transformation_value": '{"1":"Roundabout","2":"One way street","3":"Dual carriageway","6":"Single carriageway","7":"Slip road"}',
         "ordinal_position": 9},
        {"source_column": "road_surface_conditions", "target_column": "road_surface_conditions", "target_data_type": "VARCHAR",
         "transformation_type": "CASE_DECODE",
         "transformation_value": '{"1":"Dry","2":"Wet or damp","3":"Snow","4":"Frost or ice","5":"Flood"}',
         "ordinal_position": 10},
        {"source_column": "light_conditions",     "target_column": "light_conditions",     "target_data_type": "VARCHAR",
         "transformation_type": "CASE_DECODE",
         "transformation_value": '{"1":"Daylight","4":"Darkness lights lit","5":"Darkness lights unlit","6":"Darkness no lighting"}',
         "ordinal_position": 11},
        {"source_column": "urban_or_rural_area",  "target_column": "urban_or_rural_area",  "target_data_type": "VARCHAR",
         "transformation_type": "CASE_DECODE",
         "transformation_value": '{"1":"Urban","2":"Rural"}',                             "ordinal_position": 12},
        {"source_column": "speed_limit",          "target_column": "speed_limit",          "target_data_type": "INTEGER",
         "transformation_type": "CAST",           "ordinal_position": 13},
        {"source_column": "longitude",            "target_column": "longitude",            "target_data_type": "DOUBLE",   "ordinal_position": 14},
        {"source_column": "latitude",             "target_column": "latitude",             "target_data_type": "DOUBLE",   "ordinal_position": 15},
        {"source_column": "local_authority_ons_district", "target_column": "local_authority_ons_district", "target_data_type": "VARCHAR", "ordinal_position": 16},
        {"source_column": "local_authority_highway",      "target_column": "local_authority_highway",      "target_data_type": "VARCHAR", "ordinal_position": 17},
        {"source_column": "lsoa_of_accident_location",    "target_column": "lsoa_of_accident_location",    "target_data_type": "VARCHAR", "ordinal_position": 18},
    ])

    # ── Columns — bronze.casualties ───────────────────────────────────────────
    log.info("\nRegistering bronze.casualties columns ...")
    r.register_columns_bulk(t_casualties, [
        {"source_column": "collision_index",    "target_column": "collision_index",    "target_data_type": "VARCHAR", "is_primary_key": False, "is_foreign_key": True,  "ordinal_position": 1},
        {"source_column": "casualty_reference", "target_column": "casualty_reference", "target_data_type": "INTEGER", "is_primary_key": True,  "ordinal_position": 2},
        {"source_column": "vehicle_reference",  "target_column": "vehicle_reference",  "target_data_type": "INTEGER", "ordinal_position": 3},
        {"source_column": "casualty_class",     "target_column": "casualty_class",     "target_data_type": "VARCHAR",
         "transformation_type": "CASE_DECODE",
         "transformation_value": '{"1":"Driver or rider","2":"Passenger","3":"Pedestrian"}', "ordinal_position": 4},
        {"source_column": "sex_of_casualty",    "target_column": "sex_of_casualty",    "target_data_type": "VARCHAR",
         "transformation_type": "CASE_DECODE",
         "transformation_value": '{"1":"Male","2":"Female"}',                              "ordinal_position": 5},
        {"source_column": "age_of_casualty",    "target_column": "age_of_casualty",    "target_data_type": "INTEGER",
         "transformation_type": "CAST",          "ordinal_position": 6},
        {"source_column": "casualty_severity",  "target_column": "casualty_severity",  "target_data_type": "VARCHAR",
         "transformation_type": "CASE_DECODE",
         "transformation_value": '{"1":"Fatal","2":"Serious","3":"Slight"}',               "ordinal_position": 7},
        {"source_column": "casualty_type",      "target_column": "casualty_type",      "target_data_type": "VARCHAR",
         "transformation_type": "CASE_DECODE",
         "transformation_value": '{"0":"Pedestrian","1":"Cyclist","9":"Car","11":"Bus or coach","19":"Van or goods vehicle"}',
         "ordinal_position": 8},
    ])

    # ── Columns — bronze.vehicles ─────────────────────────────────────────────
    log.info("\nRegistering bronze.vehicles columns ...")
    r.register_columns_bulk(t_vehicles, [
        {"source_column": "collision_index",  "target_column": "collision_index",  "target_data_type": "VARCHAR", "is_foreign_key": True,  "ordinal_position": 1},
        {"source_column": "vehicle_reference","target_column": "vehicle_reference","target_data_type": "INTEGER", "is_primary_key": True,  "ordinal_position": 2},
        {"source_column": "vehicle_type",     "target_column": "vehicle_type",     "target_data_type": "VARCHAR",
         "transformation_type": "CASE_DECODE",
         "transformation_value": '{"1":"Pedal cycle","9":"Car","11":"Bus or coach","19":"Van","20":"Goods over 3.5t"}',
         "ordinal_position": 3},
        {"source_column": "sex_of_driver",    "target_column": "sex_of_driver",    "target_data_type": "VARCHAR",
         "transformation_type": "CASE_DECODE",
         "transformation_value": '{"1":"Male","2":"Female","3":"Not known"}',              "ordinal_position": 4},
        {"source_column": "age_of_driver",    "target_column": "age_of_driver",    "target_data_type": "INTEGER",
         "transformation_type": "CAST",        "ordinal_position": 5},
        {"source_column": "age_of_vehicle",   "target_column": "age_of_vehicle",   "target_data_type": "INTEGER",
         "transformation_type": "CAST",        "ordinal_position": 6},
        {"source_column": "generic_make_model","target_column": "generic_make_model","target_data_type": "VARCHAR", "ordinal_position": 7},
    ])

    # ── Gold tables ───────────────────────────────────────────────────────────
    log.info("\nRegistering gold tables ...")

    t_fact = r.register_table(
        source_id     = source_id,
        source_table  = "bronze.accidents + bronze.casualties + bronze.vehicles",
        target_schema = "gold",
        target_table  = "fact_collisions",
        table_type    = "FACT",
        load_order    = 10,
        description   = "One row per collision with all measures"
    )
    r.register_load_config(t_fact, load_type="FULL",
                           truncate_before_load=True)

    t_dim_date = r.register_table(
        source_id     = source_id,
        source_table  = "silver.accidents",
        target_schema = "gold",
        target_table  = "dim_date",
        table_type    = "DIMENSION",
        load_order    = 5,
        description   = "Date dimension"
    )
    r.register_load_config(t_dim_date, load_type="FULL")

    t_dim_geo = r.register_table(
        source_id     = source_id,
        source_table  = "silver.accidents",
        target_schema = "gold",
        target_table  = "dim_geography",
        table_type    = "DIMENSION",
        load_order    = 6,
        description   = "Geography dimension"
    )
    r.register_load_config(t_dim_geo, load_type="FULL")

    t_dim_sev = r.register_table(
        source_id     = source_id,
        source_table  = "silver.accidents",
        target_schema = "gold",
        target_table  = "dim_severity",
        table_type    = "DIMENSION",
        load_order    = 7,
        description   = "Severity dimension"
    )
    r.register_load_config(t_dim_sev, load_type="FULL")

    t_dim_veh = r.register_table(
        source_id     = source_id,
        source_table  = "silver.vehicles",
        target_schema = "gold",
        target_table  = "dim_vehicle_type",
        table_type    = "DIMENSION",
        load_order    = 8,
        description   = "Vehicle type dimension"
    )
    r.register_load_config(t_dim_veh, load_type="FULL")

    # ── Relationships ─────────────────────────────────────────────────────────
    log.info("\nRegistering relationships ...")
    r.register_relationship(t_fact, "date_key",      t_dim_date, "date_key",      "FACT_TO_DIM")
    r.register_relationship(t_fact, "geography_key", t_dim_geo,  "geography_key", "FACT_TO_DIM")
    r.register_relationship(t_fact, "collision_severity", t_dim_sev, "severity_key", "FACT_TO_DIM")

    # ── Data quality rules ────────────────────────────────────────────────────
    log.info("\nRegistering data quality rules ...")
    r.register_quality_rule(t_accidents,  "accidents_min_rows",   "ROW_COUNT",    operator="GT", threshold=1000,  severity="ERROR",   description="Must have at least 1000 rows")
    r.register_quality_rule(t_casualties, "casualties_min_rows",  "ROW_COUNT",    operator="GT", threshold=1000,  severity="ERROR",   description="Must have at least 1000 rows")
    r.register_quality_rule(t_vehicles,   "vehicles_min_rows",    "ROW_COUNT",    operator="GT", threshold=1000,  severity="ERROR",   description="Must have at least 1000 rows")
    r.register_quality_rule(t_accidents,  "collision_index_notnull", "NOT_NULL",  column_name="collision_index",  severity="ERROR",   description="PK must not be null")
    r.register_quality_rule(t_fact,       "fact_min_rows",        "ROW_COUNT",    operator="GT", threshold=1000,  severity="ERROR",   description="Fact table must have rows")
    r.register_quality_rule(t_fact,       "fact_ref_date",        "REF_INTEGRITY",column_name="date_key", ref_table_id=t_dim_date, ref_column="date_key", severity="WARNING", description="All fact dates must exist in dim_date")

    # ── Summary ───────────────────────────────────────────────────────────────
    r.summary()
    log.info("\n=== Registration complete ===")


if __name__ == "__main__":
    register()