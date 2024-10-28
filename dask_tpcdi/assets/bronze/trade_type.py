from dagster import asset
import dask.dataframe as dd

from dask_tpcdi.assets.staging.constants import TRADE_TYPE_FILE_PATH as INPUT_PATH
from dask_tpcdi.assets.bronze.constants import TRADE_TYPE_PATH as OUTPUT_PATH


@asset(key_prefix=["bronze"])
def trade_type() -> None:
    dd.read_csv(  # pyright: ignore[reportPrivateImportUsage]
        INPUT_PATH,
        sep="|",
        names=(
            "TT_ID",
            "TT_NAME",
            "TT_IS_SELL",
            "TT_IS_MRKT",
        ),
        dtype={
            "TT_ID": "string",
            "TT_NAME": "string",
            "TT_IS_SELL": "int",
            "TT_IS_MRKT": "int",
        },
    ).to_parquet(OUTPUT_PATH)
