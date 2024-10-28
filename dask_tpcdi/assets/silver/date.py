from dagster import AssetKey, asset

from dask_tpcdi.assets.bronze.constants import DATE_PATH as INPUT_PATH
from dask_tpcdi.assets.silver.constants import DATE_PATH as OUTPUT_PATH
import dask.dataframe as dd

@asset(key_prefix="silver", deps=[AssetKey(["bronze", "date"])])
def date() -> None:
    dd.read_parquet(INPUT_PATH).rename({
            "SK_DateID": "sk_date_id",
            "DateValue": "date_value",
            "DateDesc": "date_desc",
            "CalendarYearID": "calendar_year_id",
            "CalendarYearDesc": "calendar_year_desc",
            "CalendarQtrID": "calendar_qtr_id",
            "CalendarQtrDesc": "calendar_qtr_desc",
            "CalendarMonthID": "calendar_month_id",
            "CalendarMonthDesc": "calendar_month_desc",
            "CalendarWeekID": "calendar_week_id",
            "CalendarWeekDesc": "calendar_week_desc",
            "DayOfWeekNum": "day_of_week_num",
            "DayOfWeekDesc": "day_of_week_desc",
            "FiscalYearID": "fiscal_year_id",
            "FiscalYearDesc": "fiscal_year_desc",
            "FiscalQtrID": "fiscal_qtr_id",
            "FiscalQtrDesc": "fiscal_qtr_desc",
            "HolidayFlag": "holiday_flag",
            }
        ).to_parquet(OUTPUT_PATH)
