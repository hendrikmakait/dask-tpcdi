from dagster import asset
import dask.dataframe as dd
from dask_tpcdi.assets.staging.constants import DATE_FILE_PATH as INPUT_PATH
from dask_tpcdi.assets.bronze.constants import DATE_PATH as OUTPUT_PATH


@asset(key_prefix=["bronze"])
def date() -> None:
    dd.read_csv(  # pyright: ignore[reportPrivateImportUsage]
        INPUT_PATH,
        sep="|",
        names=(
            "SK_DateID",
            "DateValue",
            "DateDesc",
            "CalendarYearID",
            "CalendarYearDesc",
            "CalendarQtrID",
            "CalendarQtrDesc",
            "CalendarMonthID",
            "CalendarMonthDesc",
            "CalendarWeekID",
            "CalendarWeekDesc",
            "DayOfWeekNum",
            "DayOfWeekDesc",
            "FiscalYearID",
            "FiscalYearDesc",
            "FiscalQtrID",
            "FiscalQtrDesc",
            "HolidayFlag",
        ),
        dtype={
            "SK_DateID": "int",
            "DateValue": "string",
            "DateDesc": "string",
            "CalendarYearID": "int",
            "CalendarYearDesc": "string",
            "CalendarQtrID": "int",
            "CalendarQtrDesc": "string",
            "CalendarMonthID": "int",
            "CalendarMonthDesc": "string",
            "CalendarWeekID": "int",
            "CalendarWeekDesc": "string",
            "DayOfWeekNum": "int",
            "DayOfWeekDesc": "string",
            "FiscalYearID": "int",
            "FiscalYearDesc": "string",
            "FiscalQtrID": "int",
            "FiscalQtrDesc": "string",
            "HolidayFlag": "bool",
        },
    ).to_parquet(OUTPUT_PATH)
