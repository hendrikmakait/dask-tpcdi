from dagster import AssetKey, asset

from dask_tpcdi.assets.bronze.constants import TIME_PATH as INPUT_PATH
from dask_tpcdi.assets.silver.constants import TIME_PATH as OUTPUT_PATH
import dask.dataframe as dd


@asset(key_prefix="silver", deps=[AssetKey(["bronze", "time"])])
def time() -> None:
    dd.read_parquet(INPUT_PATH).rename({
            "SK_TimeID": "sk_time_id",
            "TimeValue": "time_value",
            "HourID": "hour_id",
            "HourDesc": "hour_desc",
            "MinuteID": "minute_id",
            "MinuteDesc": "minute_desc",
            "SecondID": "second_id",
            "SecondDesc": "second_desc",
            "MarketHoursFlag": "market_hours_flag",
            "OfficeHoursFlag": "office_hours_flag",
            }
        ).to_parquet(OUTPUT_PATH)
