from dagster import AssetKey, asset

from dask_tpcdi.assets.silver.constants import DATE_PATH as INPUT_PATH
from dask_tpcdi.assets.gold.constants import DATE_PATH as OUTPUT_PATH
import dask.dataframe as dd


@asset(key_prefix="gold", deps=[AssetKey(["silver", "date"])])
def date() -> None:
    dd.read_parquet(INPUT_PATH).repartition(npartitions=1).to_parquet(OUTPUT_PATH)
