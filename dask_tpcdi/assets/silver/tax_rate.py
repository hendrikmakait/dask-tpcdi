
from dagster import AssetKey, asset
import dask.dataframe as dd
from dask_tpcdi.assets.bronze.constants import TAX_RATE_PATH as INPUT_PATH
from dask_tpcdi.assets.silver.constants import TAX_RATE_PATH as OUTPUT_PATH


@asset(key_prefix=["silver"], deps=AssetKey(["bronze", "tax_rate"]))
def tax_rate() -> None:
    dd.read_parquet(INPUT_PATH).rename({
        "TX_ID": "tax_rate_id",
        "TX_NAME": "tax_rate_name",
        "TX_RATE": "tax_rate",
    }).to_parquet(OUTPUT_PATH)
