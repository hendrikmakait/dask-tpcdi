from dagster import asset
import dask.dataframe as dd

from dask_tpcdi.assets.landing.constants import CASH_TRANSACTION_FILE_PATH as INPUT_PATH
from dask_tpcdi.assets.raw.constants import CASH_TRANSACTION_PATH as OUTPUT_PATH


@asset
def cash_transaction() -> None:
    dd.read_csv(  # pyright: ignore[reportPrivateImportUsage]
        INPUT_PATH,
        sep=",",
        names=(
            "CT_CA_ID",
            "CT_DTS",
            "CT_AMT",
            "CT_NAME",
        ),
        dtype={
            "CT_CA_ID": "int",
            "CT_DTS": "datetime",
            "CT_AMT": "float",
            "CT_NAME": "string",
        },
    ).to_parquet(OUTPUT_PATH)
