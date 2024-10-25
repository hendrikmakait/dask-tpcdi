from dagster import asset
import dask.dataframe as dd
from dask_tpcdi.assets.staging.constants import DAILY_MARKET_FILE_PATH as INPUT_PATH
from dask_tpcdi.assets.bronze.constants import DAILY_MARKET_PATH as OUTPUT_PATH


@asset
def daily_market() -> None:
    dd.read_csv(  # pyright: ignore[reportPrivateImportUsage]
        INPUT_PATH,
        sep="|",
        names=(
            "DM_DATE",
            "DM_S_SYMB",
            "DM_CLOSE",
            "DM_HIGH",
            "DM_LOW",
            "DM_VOL",
        ),
        dtype={
            "DM_S_SYMB": "string",
            "DM_CLOSE": "float",
            "DM_HIGH": "float",
            "DM_LOW": "float",
            "DM_VOL": "int",
        },
        parse_dates=["DM_DATE"],
        date_format=["%Y-%m-%d"],
    ).to_parquet(OUTPUT_PATH)
