from dagster import asset
import dask.dataframe as dd

from dask_tpcdi.assets.staging.constants import STATUS_TYPE_FILE_PATH as INPUT_PATH
from dask_tpcdi.assets.bronze.constants import STATUS_TYPE_PATH as OUTPUT_PATH


@asset(key_prefix=["bronze"])
def status_type() -> None:
    dd.read_csv(  # pyright: ignore[reportPrivateImportUsage]
        INPUT_PATH,
        sep="|",
        names=(
            "ST_ID",
            "ST_NAME",
        ),
        dtype={
            "ST_ID": "string",
            "ST_NAME": "string",
        },
    ).to_parquet(OUTPUT_PATH)
