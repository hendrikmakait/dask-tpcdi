from dagster import asset
import dask.dataframe as dd

from dask_tpcdi.assets.staging.constants import CASH_TRANSACTION_FILE_PATH as INPUT_PATH
from dask_tpcdi.assets.bronze.constants import CASH_TRANSACTION_PATH as OUTPUT_PATH


@asset(key_prefix=["bronze"])
def cash_transaction() -> None:
    dd.read_csv(  # pyright: ignore[reportPrivateImportUsage]
        INPUT_PATH,
        sep="|",
        names=(
            "CT_CA_ID",
            "CT_DTS",
            "CT_AMT",
            "CT_NAME",
        ),
        dtype={
            "CT_CA_ID": "int",
            "CT_AMT": "float",
            "CT_NAME": "string",
        },
        date_format="%Y-%m-%d %H:%M:%S",
        parse_dates=["CT_DTS"],
        
    ).to_parquet(OUTPUT_PATH)
