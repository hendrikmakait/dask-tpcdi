from dagster import asset
import dask.dataframe as dd
from dask_tpcdi.assets.staging.constants import HOLDING_HISTORY_FILE_PATH as INPUT_PATH
from dask_tpcdi.assets.bronze.constants import HOLDING_HISTORY_PATH as OUTPUT_PATH


@asset(key_prefix=["bronze"])
def holding_history() -> None:
    dd.read_csv(  # pyright: ignore[reportPrivateImportUsage]
        INPUT_PATH,
        sep="|",
        names=(
            "HH_H_T_ID",
            "HH_T_ID",
            "HH_BEFORE_QTY",
            "HH_AFTER_QTY",
        ),
        dtype={
            "HH_H_T_ID": "int",
            "HH_T_ID": "int",
            "HH_BEFORE_QTY": "int",
            "HH_AFTER_QTY": "int",
        },
    ).to_parquet(OUTPUT_PATH)