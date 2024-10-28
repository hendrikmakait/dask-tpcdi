from dagster import asset
import dask.dataframe as dd

from dask_tpcdi.assets.staging.constants import HR_FILE_PATH as INPUT_PATH
from dask_tpcdi.assets.bronze.constants import HR_PATH as OUTPUT_PATH


@asset(key_prefix=["bronze"])
def hr() -> None:
    dd.read_csv(  # pyright: ignore[reportPrivateImportUsage]
        INPUT_PATH,
        sep=",",
        names=(
            "EmployeeID",
            "ManagerID",
            "EmployeeFirstName",
            "EmployeeLastName",
            "EmployeeMI",
            "EmployeeJobCode",
            "EmployeeBranch",
            "EmployeeOffice",
            "EmployeePhone",
        ),
        dtype={
            "EmployeeID": "int",
            "ManagerID": "int",
            "EmployeeFirstName": "string",
            "EmployeeLastName": "string",
            "EmployeeMI": "string",
            "EmployeeJobCode": "Int16",
            "EmployeeBranch": "string",
            "EmployeeOffice": "string",
            "EmployeePhone": "string",
        },
    ).to_parquet(OUTPUT_PATH)
