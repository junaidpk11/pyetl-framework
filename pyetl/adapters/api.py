"""
pyetl/adapters/api.py

Generic paginated REST API extractor.
Handles pagination, retries, and rate limiting.
Returns a pandas DataFrame.
"""

import time
import logging
import requests
import pandas as pd
from typing import Optional, Callable

log = logging.getLogger(__name__)


class APIExtractor:

    def __init__(
        self,
        base_url:    str,
        headers:     dict = None,
        rate_limit:  float = 0.5,   # seconds between requests
        max_retries: int   = 3,
        timeout:     int   = 30,
    ):
        self.base_url    = base_url.rstrip("/")
        self.headers     = headers or {"Accept": "application/json"}
        self.rate_limit  = rate_limit
        self.max_retries = max_retries
        self.timeout     = timeout
        self.session     = requests.Session()
        self.session.headers.update(self.headers)

    def get(self, endpoint: str, params: dict = None) -> dict:
        """
        Make a single GET request with retry logic.
        Returns parsed JSON response.
        """
        url    = f"{self.base_url}/{endpoint.lstrip('/')}"
        params = params or {}

        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(
                    url, params=params, timeout=self.timeout
                )
                response.raise_for_status()
                return response.json()

            except requests.exceptions.HTTPError as e:
                log.warning(f"    HTTP error (attempt {attempt}): {e}")
                if attempt == self.max_retries:
                    raise
                time.sleep(2 ** attempt)

            except requests.exceptions.ConnectionError as e:
                log.warning(f"    Connection error (attempt {attempt}): {e}")
                if attempt == self.max_retries:
                    raise
                time.sleep(2 ** attempt)

            except requests.exceptions.Timeout:
                log.warning(f"    Timeout (attempt {attempt})")
                if attempt == self.max_retries:
                    raise
                time.sleep(2 ** attempt)

        return {}

    def get_paginated(
        self,
        endpoint:        str,
        params:          dict            = None,
        data_key:        str             = None,
        page_param:      str             = "pageNumber",
        page_size_param: str             = "pageSize",
        page_size:       int             = 100,
        max_pages:       Optional[int]   = None,
        transform_fn:    Optional[Callable] = None,
    ) -> pd.DataFrame:
        """
        Fetch all pages from a paginated endpoint.

        data_key:        JSON key containing the list of records
        page_param:      query param name for page number
        page_size_param: query param name for page size
        transform_fn:    optional function to transform each page's records
        """
        params    = params or {}
        all_rows  = []
        page      = 1
        total_pages = None

        log.info(f"  Fetching {endpoint} (page_size={page_size}) ...")

        while True:
            page_params = {
                **params,
                page_param:      page,
                page_size_param: page_size,
            }

            data = self.get(endpoint, page_params)

            if not data:
                break

            # Extract records from response
            if data_key and data_key in data:
                records = data[data_key]
            elif isinstance(data, list):
                records = data
            else:
                records = [data]

            if not records:
                break

            # Apply transform if provided
            if transform_fn:
                records = transform_fn(records)

            all_rows.extend(records)

            # Detect total pages from response metadata
            if total_pages is None:
                meta = data.get("meta", {})
                total_count = (
                    meta.get("totalCount") or
                    data.get("totalCount") or
                    data.get("total") or
                    None
                )
                if total_count:
                    total_pages = -(-int(total_count) // page_size)
                    log.info(f"    Total records: {total_count:,} ({total_pages} pages)")

            log.info(
                f"    Page {page}"
                + (f"/{total_pages}" if total_pages else "")
                + f" — {len(records)} records (running total: {len(all_rows):,})"
            )

            # Check if we've reached the last page
            if len(records) < page_size:
                break
            if max_pages and page >= max_pages:
                log.info(f"    Reached max_pages={max_pages} — stopping")
                break

            page += 1
            time.sleep(self.rate_limit)

        log.info(f"  Fetched {len(all_rows):,} total records from {endpoint}")
        return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()