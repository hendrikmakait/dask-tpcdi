
from dagster import AssetKey, asset
import dask.dataframe as dd
from dask_tpcdi.assets.bronze.constants import STATUS_TYPE_PATH as INPUT_PATH
from dask_tpcdi.assets.silver.constants import STATUS_TYPE_PATH as OUTPUT_PATH


@asset(key_prefix=["silver"], deps=AssetKey(["bronze", "status_type"]))
def status_type() -> None:
    dd.read_parquet(INPUT_PATH).rename({
        "ST_ID": "status_type_id",
        "ST_NAME": "status_type_name",
    }).to_parquet(OUTPUT_PATH)
