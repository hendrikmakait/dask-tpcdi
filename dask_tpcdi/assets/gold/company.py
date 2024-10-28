from dagster import AssetKey, asset

import dask.dataframe as dd

from dask_tpcdi.assets.silver.constants import COMPANY_PATH as INPUT_PATH, INDUSTRY_PATH, STATUS_TYPE_PATH
from dask_tpcdi.assets.gold.constants import COMPANY_PATH as OUTPUT_PATH

import pandas as pd

@asset(key_prefix="gold", deps=[AssetKey(["silver", "company"]), AssetKey(["silver", "date"]), AssetKey(["silver", "industry"])])
def dim_company():
    companies = dd.read_parquet(INPUT_PATH)
    status_types = dd.read_parquet(STATUS_TYPE_PATH)
    industries = dd.read_parquet(INDUSTRY_PATH)
    dim = companies.merge(industries, on="industry_id", how="left").merge(status_types, on="status_type_id", how="left")
    dim.to_parquet(OUTPUT_PATH)
