from dagster import asset
import dask.dataframe as dd

from dask_tpcdi.assets.staging.constants import TIME_FILE_PATH as INPUT_PATH
from dask_tpcdi.assets.bronze.constants import TIME_PATH as OUTPUT_PATH


@asset
def time() -> None:
    dd.read_csv(  # pyright: ignore[reportPrivateImportUsage]
        INPUT_PATH,
        sep="|",
        names=(
            "SK_TimeID",
            "TimeValue",
            "HourID",
            "HourDesc",
            "MinuteID",
            "MinuteDesc",
            "SecondID",
            "SecondDesc",
            "MarketHoursFlag",
            "OfficeHoursFlag",
        ),
        dtype={
            "SK_TimeID": "string", # FIXME: What's the type?
            "TimeValue": "string",
            "HourID": "int",
            "HourDesc": "string",
            "MinuteID": "int",
            "MinuteDesc": "string",
            "SecondID": "int",
            "SecondDesc": "string",
            "MarketHoursFlag": "bool",
            "OfficeHoursFlag": "bool",
        },
    ).to_parquet(OUTPUT_PATH)
