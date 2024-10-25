from dagster import asset
import dask.dataframe as dd

from dask_tpcdi.assets.staging.constants import WATCH_HISTORY_FILE_PATH as INPUT_PATH
from dask_tpcdi.assets.bronze.constants import WATCH_HISTORY_PATH as OUTPUT_PATH


@asset
def watch_history() -> None:
    dd.read_csv(  # pyright: ignore[reportPrivateImportUsage]
        INPUT_PATH,
        sep="|",
        names=(
            "W_C_ID",
            "W_S_SYMB",
            "W_DTS",
            "W_ACTION",
        ),
        dtype={
            "W_C_ID": "int",
            "W_S_SYMB": "string",
            "W_ACTION": "string",
        },
        date_format="%Y-%m-%d %H:%M:%S",
        parse_dates=["W_DTS"],
    ).to_parquet(OUTPUT_PATH)
