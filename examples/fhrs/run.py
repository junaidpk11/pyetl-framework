"""
examples/fhrs/run.py

FHRS (Food Hygiene Rating Scheme) pipeline demo.

Demonstrates:
  - Paginated REST API extraction
  - Data cleaning and chain filtering
  - PostgreSQL upsert with protected manual fields
  - Button-click style CLI run

Usage:
    python3 examples/fhrs/run.py
    python3 examples/fhrs/run.py --local-authority 197  # filter by area
    python3 examples/fhrs/run.py --max-pages 5          # limit for testing
"""

import sys
import logging
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyetl.adapters.api      import APIExtractor
from pyetl.adapters.postgres import PostgresLoader
from pyetl.config            import Config

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s  %(levelname)s  %(message)s"
)
log = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

FHRS_BASE_URL = "https://api.ratings.food.gov.uk"

FHRS_HEADERS  = {
    "Accept":        "application/json",
    "x-api-version": "2",
    "content-type":  "application/json",
}

# Business types to keep
KEEP_BUSINESS_TYPES = {
    "Restaurant/Cafe/Canteen",
    "Takeaway/sandwich shop",
    "Pub/bar/nightclub",
    "Mobile caterer",
    "Hotel/bed & breakfast/guest house",
}

# Chain keywords — any business matching these is excluded
CHAIN_KEYWORDS = [
    "mcdonald", "mcdonalds", "kfc", "burger king", "subway", "greggs",
    "costa", "starbucks", "pret", "nero", "caffe nero", "nando",
    "wagamama", "zizzi", "ask italian", "pizza hut", "dominos",
    "domino", "papa john", "five guys", "leon", "pho", "itsu",
    "yo sushi", "sushi", "wasabi", "spudulike", "wetherspoon",
    "harvester", "toby carvery", "brewers fayre", "beefeater",
    "frankie", "benny", "bella italia", "prezzo", "chiquito",
    "tgi friday", "tgi", "ihop", "hooters", "shake shack",
    "chicken cottage", "perfect fried chicken", "pfc", "dixy",
    "texas chicken", "popeyes", "krispy kreme", "dunkin",
    "tim hortons", "muffin break", "upper crust", "harry ramsden",
]

# PostgreSQL target config
PG_SCHEMA = "fhrs"
PG_TABLE  = "establishments"

# Primary key — FHRS unique identifier
PRIMARY_KEY = ["fhrsid"]

# Manual fields — NEVER overwritten by the pipeline
PROTECTED_COLUMNS = [
    "exclude",          # manual boolean — exclude from leads
    "notes",            # manual text notes
    "contacted_at",     # when sales team contacted this business
    "contact_status",   # e.g. 'interested', 'not interested', 'callback'
]


# ── Extraction ─────────────────────────────────────────────────────────────────

def get_all_authorities(api: APIExtractor) -> list:
    """Fetch all local authorities."""
    log.info("  Fetching local authorities ...")
    all_authorities = []
    page = 1

    while True:
        data = api.get("Authorities", {"pageNumber": page, "pageSize": 100})
        authorities = data.get("authorities", [])
        if not authorities:
            break
        all_authorities.extend(authorities)
        meta = data.get("meta", {})
        total_pages = meta.get("totalPages", 1)
        log.info(f"    Page {page}/{total_pages} — {len(authorities)} authorities")
        if page >= total_pages:
            break
        page += 1

    log.info(f"  Found {len(all_authorities)} local authorities")
    return all_authorities


def extract_establishments(
    local_authority_id: int = None,
    max_pages: int = None,
    max_authorities: int = None,
) -> pd.DataFrame:
    """
    Fetch establishments from FHRS API.
    Loops through local authorities since the API requires a filter.
    """
    api = APIExtractor(
        base_url   = FHRS_BASE_URL,
        headers    = FHRS_HEADERS,
        rate_limit = 0.5,
    )

    # Get authorities to loop through
    if local_authority_id:
        authorities = [{"LocalAuthorityId": local_authority_id, "Name": "Specified"}]
    else:
        authorities = get_all_authorities(api)
        if max_authorities:
            authorities = authorities[:max_authorities]
            log.info(f"  Limited to first {max_authorities} authorities for testing")

    all_records = []

    for i, auth in enumerate(authorities, 1):
        auth_id   = auth["LocalAuthorityId"]
        auth_name = auth.get("Name", auth_id)
        est_count = auth.get("EstablishmentCount", "?")

        log.info(f"  [{i}/{len(authorities)}] {auth_name} (~{est_count} establishments)")

        df = api.get_paginated(
            endpoint        = "Establishments",
            params          = {"localAuthorityId": auth_id},
            data_key        = "establishments",
            page_param      = "pageNumber",
            page_size_param = "pageSize",
            page_size       = 500,
            max_pages       = max_pages,
        )

        if not df.empty:
            all_records.append(df)

    if not all_records:
        return pd.DataFrame()

    combined = pd.concat(all_records, ignore_index=True)
    log.info(f"  Extracted {len(combined):,} total raw establishments")
    return combined


# ── Transformation ─────────────────────────────────────────────────────────────

def transform_establishments(df: pd.DataFrame) -> pd.DataFrame:
    """Clean, filter, and structure the raw FHRS data."""
    if df.empty:
        return df

    log.info(f"  Transforming {len(df):,} records ...")
    original_count = len(df)

    # ── Extract key fields ────────────────────────────────────────────────────
    df = pd.DataFrame({
        "fhrsid":           df.get("FHRSID"),
        "business_name":    df.get("BusinessName"),
        "business_type":    df.get("BusinessType"),
        "business_type_id": df.get("BusinessTypeID"),
        "address_line1":    df.get("AddressLine1"),
        "address_line2":    df.get("AddressLine2"),
        "address_line3":    df.get("AddressLine3"),
        "address_line4":    df.get("AddressLine4"),
        "postcode":         df.get("PostCode"),
        "rating_value":     df.get("RatingValue"),
        "rating_date":      pd.to_datetime(df.get("RatingDate"), errors="coerce"),
        "local_authority":  df.get("LocalAuthorityName"),
        "local_authority_id": df.get("LocalAuthorityId"),
        "phone":            df.get("Phone"),
        "longitude":        pd.to_numeric(df.get("geocode", {}).apply(
                                lambda x: x.get("longitude") if isinstance(x, dict) else None
                            ), errors="coerce") if "geocode" in df.columns else None,
        "latitude":         pd.to_numeric(df.get("geocode", {}).apply(
                                lambda x: x.get("latitude") if isinstance(x, dict) else None
                            ), errors="coerce") if "geocode" in df.columns else None,
        "extracted_at":     datetime.now(),
    })

    # ── Clean strings ─────────────────────────────────────────────────────────
    str_cols = ["business_name", "address_line1", "address_line2",
                "address_line3", "address_line4", "postcode", "local_authority"]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace("None", None).replace("nan", None)

    # ── Filter business types ─────────────────────────────────────────────────
    before = len(df)
    df = df[df["business_type"].isin(KEEP_BUSINESS_TYPES)]
    log.info(f"    Business type filter: {before:,} → {len(df):,} rows")

    # ── Filter out chains ─────────────────────────────────────────────────────
    before = len(df)
    name_lower = df["business_name"].str.lower().fillna("")
    is_chain   = name_lower.apply(
        lambda name: any(kw in name for kw in CHAIN_KEYWORDS)
    )
    df = df[~is_chain]
    log.info(f"    Chain filter: {before:,} → {len(df):,} rows (removed {before - len(df):,} chains)")

    # ── Drop rows without a valid FHRSID ─────────────────────────────────────
    df = df.dropna(subset=["fhrsid"])
    df["fhrsid"] = df["fhrsid"].astype(int)

    log.info(f"  Transformation complete: {original_count:,} → {len(df):,} records")
    return df.reset_index(drop=True)


# ── Loading ────────────────────────────────────────────────────────────────────

def setup_postgres(loader: PostgresLoader, df: pd.DataFrame):
    """Create schema and table if they don't exist."""
    loader.create_schema(PG_SCHEMA)
    loader.create_table_from_df(
        df            = df,
        schema        = PG_SCHEMA,
        table         = PG_TABLE,
        primary_keys  = PRIMARY_KEY,
        extra_columns = {
            "exclude":        "BOOLEAN DEFAULT FALSE",
            "notes":          "TEXT",
            "contacted_at":   "TIMESTAMP",
            "contact_status": "VARCHAR(50)",
        }
    )


def load_to_postgres(loader: PostgresLoader, df: pd.DataFrame) -> int:
    """Upsert data into PostgreSQL preserving manual fields."""
    return loader.load_upsert_fast(
        df                = df,
        schema            = PG_SCHEMA,
        table             = PG_TABLE,
        primary_keys      = PRIMARY_KEY,
        protected_columns = PROTECTED_COLUMNS,
    )


# ── Main ───────────────────────────────────────────────────────────────────────

def run(local_authority_id: int = None, max_pages: int = None, max_authorities: int = None):
    log.info("=" * 60)
    log.info("  FHRS Pipeline — Food Hygiene Rating Scheme")
    log.info(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60 + "\n")

    # ── Connect to PostgreSQL ─────────────────────────────────────────────────
    log.info("Connecting to PostgreSQL ...")
    loader = PostgresLoader(Config.postgres_url())
    if not loader.test_connection():
        log.error("Cannot connect to PostgreSQL — aborting")
        sys.exit(1)

    # ── Extract ───────────────────────────────────────────────────────────────
    log.info("\nExtracting from FHRS API ...")
    raw_df = extract_establishments(
        local_authority_id = local_authority_id,
        max_pages          = max_pages,
        max_authorities    = max_authorities,
    )

    if raw_df.empty:
        log.warning("No data returned from API — aborting")
        sys.exit(1)

    # ── Transform ─────────────────────────────────────────────────────────────
    log.info("\nTransforming data ...")
    clean_df = transform_establishments(raw_df)

    if clean_df.empty:
        log.warning("No records after filtering — aborting")
        sys.exit(1)

    # ── Setup and load ────────────────────────────────────────────────────────
    log.info("\nSetting up PostgreSQL target ...")
    setup_postgres(loader, clean_df)

    log.info("\nLoading to PostgreSQL ...")
    total = load_to_postgres(loader, clean_df)

    # ── Summary ───────────────────────────────────────────────────────────────
    count = loader.row_count(PG_SCHEMA, PG_TABLE)
    log.info("\n" + "=" * 60)
    log.info("  Pipeline complete")
    log.info(f"  Records processed : {total:,}")
    log.info(f"  Total in database : {count:,}")
    log.info(f"  Protected fields  : {', '.join(PROTECTED_COLUMNS)}")
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FHRS Food Business Pipeline")
    parser.add_argument("--local-authority", type=int, help="Filter by local authority ID")
    parser.add_argument("--max-pages",        type=int, help="Limit pages per authority for testing")
    parser.add_argument("--max-authorities",  type=int, help="Limit number of authorities for testing")
    args = parser.parse_args()

    run(
        local_authority_id = args.local_authority,
        max_pages          = args.max_pages,
        max_authorities    = getattr(args, "max_authorities", None),
    )