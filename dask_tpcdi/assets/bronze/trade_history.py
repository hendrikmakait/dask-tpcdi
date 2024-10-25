from dagster import asset
import dask.dataframe as dd

from dask_tpcdi.assets.staging.constants import TRADE_HISTORY_FILE_PATH as INPUT_PATH
from dask_tpcdi.assets.bronze.constants import TRADE_HISTORY_PATH as OUTPUT_PATH


@asset
def trade_history() -> None:
    dd.read_csv(  # pyright: ignore[reportPrivateImportUsage]
        INPUT_PATH,
        sep="|",
        names=(
            "TH_T_ID",
            "TH_DTS",
            "TH_ST_ID",
        ),
        dtype={
            "TH_T_ID": "int",
            "TH_ST_ID": "string",
        },
        date_format="%Y-%m-%d %H:%M:%S",
        parse_dates=["TH_DTS"],
    ).to_parquet(OUTPUT_PATH)
