from dagster import asset
import dask.dataframe as dd

from dask_tpcdi.assets.landing.constants import TRADE_TYPE_FILE_PATH as INPUT_PATH
from dask_tpcdi.assets.raw.constants import TRADE_TYPE_PATH as OUTPUT_PATH


@asset
def trade_history() -> None:
    dd.read_csv(  # pyright: ignore[reportPrivateImportUsage]
        INPUT_PATH,
        sep=",",
        names=(
            "TT_ID",
            "TT_NAME",
            "TT_IS_SELL",
            "TT_IS_MRKT",
        ),
        dtype={
            "TT_ID": "int",
            "TT_NAME": "int",
            "TT_IS_SELL": "int",
            "TT_IS_MRKT": "int",
        },
    ).to_parquet(OUTPUT_PATH)
