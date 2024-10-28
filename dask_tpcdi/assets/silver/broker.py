from datetime import datetime
from dask_tpcdi.assets.bronze.constants import HR_PATH as INPUT_PATH

from dagster import asset
import dask.dataframe as dd
import pandas as pd


@asset(key_prefix=["silver"])
def broker() -> None:
    employees = dd.read_parquet(INPUT_PATH)  # pyright: ignore[reportPrivateImportUsage]
    brokers = employees[employees["EmployeeJobCode"] == 314]
    brokers = brokers.rename(
        {
            "EmployeeID": "broker_id",
            "EmployeeFirstName": "first_name",
            "EmployeeLastName": "last_name",
            "EmployeeMI": "middle_initial",
            "EmployeeBranch": "branch",
            "EmployeeOffice": "office",
            "EmployeePhone": "phone",
        }
    )
    brokers = brokers[
        [
            "broker_id",
            "first_name",
            "last_name",
            "middle_initial",
            "branch",
            "office",
            "phone",
        ]
    ]
    #brokers["is_current"] = True
    #brokers["effective_date"] = ...
    #brokers["end_date"] = datetime.fromisoformat("9999-12-31")
    brokers["batch_id"] = ...
    brokers["sk_broker_id"] = brokers.map_partitions(lambda df: pd.util.hash_pandas_object(df[["broker_id"]]))
