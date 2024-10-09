from dagster import asset
import dask.dataframe as dd

from dask_tpcdi.assets.landing.constants import STATUS_TYPE_FILE_PATH as INPUT_PATH
from dask_tpcdi.assets.raw.constants import STATUS_TYPE_PATH as OUTPUT_PATH


@asset
def status_type() -> None:
    dd.read_csv(  # pyright: ignore[reportPrivateImportUsage]
        INPUT_PATH,
        sep=",",
        names=(
            "ST_ID",
            "ST_NAME",
        ),
        dtype={
            "ST_ID": "int",
            "ST_NAME": "string",
        },
    ).to_parquet(OUTPUT_PATH)
