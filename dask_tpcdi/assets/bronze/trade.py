from dagster import asset
import dask.dataframe as dd

from dask_tpcdi.assets.staging.constants import TRADE_FILE_PATH as INPUT_PATH
from dask_tpcdi.assets.bronze.constants import TRADE_PATH as OUTPUT_PATH


@asset(key_prefix=["bronze"])
def trade() -> None:
    dd.read_csv(  # pyright: ignore[reportPrivateImportUsage]
        INPUT_PATH,
        sep="|",
        names=(
            "T_ID",
            "T_DTS",
            "T_ST_ID",
            "T_TT_ID",
            "T_IS_CASH",
            "T_S_SYMB",
            "T_QTY",
            "T_BID_PRICE",
            "T_CA_ID",
            "T_EXEC_NAME",
            "T_TRADE_PRICE",
            "T_CHRG",
            "T_COMM",
            "T_TAX",
        ),
        dtype={
            "T_ID": "int",
            "T_ST_ID": "string",
            "T_TT_ID": "string",
            "T_IS_CASH": "bool",
            "T_S_SYMB": "string",
            "T_QTY": "int",
            "T_BID_PRICE": "float",
            "T_CA_ID": "int",
            "T_EXEC_NAME": "string",
            "T_TRADE_PRICE": "float",
            "T_CHRG": "float",
            "T_COMM": "float",
            "T_TAX": "float",
        },
        date_format="%Y-%m-%d %H:%M:%S",
        parse_dates=["T_DTS"],
    ).to_parquet(OUTPUT_PATH)
