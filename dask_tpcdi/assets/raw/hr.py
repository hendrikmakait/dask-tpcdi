from dagster import asset
import dask.dataframe as dd

from dask_tpcdi.assets.landing.constants import HR_FILE_PATH as INPUT_PATH
from dask_tpcdi.assets.raw.constants import HR_PATH as OUTPUT_PATH


@asset
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
        index_col=0,
        dtype={
            "EmployeeID": "int",
            "ManagerID": "int",
            "EmployeeFirstName": "string",
            "EmployeeLastName": "string",
            "EmployeeMI": "string",
            "EmployeeJobCode": "int",
            "EmployeeBranch": "string",
            "EmployeeOffice": "string",
            "EmployeePhone": "string",
        },
    ).to_parquet(OUTPUT_PATH)
