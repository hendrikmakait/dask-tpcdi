
from dagster import AssetKey, asset
import dask.dataframe as dd
from dask_tpcdi.assets.bronze.constants import INDUSTRY_PATH as INPUT_PATH
from dask_tpcdi.assets.silver.constants import INDUSTRY_PATH as OUTPUT_PATH


@asset(key_prefix=["silver"], deps=AssetKey(["bronze", "industry"]))
def industry() -> None:
    dd.read_parquet(INPUT_PATH).rename({
        "IN_ID": "industry_id",
        "IN_NAME": "industry_name",
        "IN_SC_ID": "industry_sector_id",
    }).to_parquet(OUTPUT_PATH)
