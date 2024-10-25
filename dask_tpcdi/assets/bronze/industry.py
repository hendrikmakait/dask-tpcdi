
from dagster import asset
import dask.dataframe as dd
from dask_tpcdi.assets.staging.constants import INDUSTRY_FILE_PATH as INPUT_PATH
from dask_tpcdi.assets.bronze.constants import INDUSTRY_PATH as OUTPUT_PATH


@asset
def industry() -> None:
    dd.read_csv(  # pyright: ignore[reportPrivateImportUsage]
        INPUT_PATH,
        sep="|",
        names=(
            "IN_ID",
            "IN_NAME",
            "IN_SC_ID",
        ),
        dtype={
            "IN_ID": "string",
            "IN_NAME" :"string",
            "IN_SC_ID": "string",
        },
    ).to_parquet(OUTPUT_PATH)
