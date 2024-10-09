from dagster import asset
import dask.dataframe as dd

from dask_tpcdi.assets.landing.constants import TAX_RATE_FILE_PATH as INPUT_PATH
from dask_tpcdi.assets.raw.constants import TAX_RATE_PATH as OUTPUT_PATH


@asset
def tax_rate() -> None:
    dd.read_csv(  # pyright: ignore[reportPrivateImportUsage]
        INPUT_PATH,
        sep=",",
        names=(
            "TX_ID",
            "TX_NAME",
            "TX_RATE",
        ),
        dtype={
            "TX_ID": "int",
            "TX_NAME": "string",
            "TX_RATE": "float",
        },
    ).to_parquet(OUTPUT_PATH)
