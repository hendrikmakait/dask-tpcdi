
from dagster import asset
import dask.dataframe as dd
from dask_tpcdi.assets.staging.constants import PROSPECT_FILE_PATH as INPUT_PATH
from dask_tpcdi.assets.bronze.constants import PROSPECT_PATH as OUTPUT_PATH


@asset
def prospect() -> None:
    dd.read_csv(  # pyright: ignore[reportPrivateImportUsage]
        INPUT_PATH,
        sep=",",
        names=(
            "AgencyID",
            "LastName",
            "FirstName",
            "MiddleInitial",
            "Gender",
            "AddressLine1",
            "AddressLine2",
            "PostalCode",
            "City",
            "State",
            "Country",
            "Phone",
            "Income",
            "NumberCars",
            "NumberChildren",
            "MaritalStatus",
            "Age",
            "CreditRating",
            "OwnOrRentFlag",
            "Employer",
            "NumberCreditCards",
            "NetWorth",
        ),
        dtype={
            "AgencyID": "string",
            "LastName": "string",
            "FirstName": "string",
            "MiddleInitial": "string",
            "Gender": "string",
            "AddressLine1": "string",
            "AddressLine2": "string",
            "PostalCode": "string",
            "City": "string",
            "State": "string",
            "Country": "string",
            "Phone": "string",
            "Income": "Int32",
            "NumberCars": "Int8",
            "NumberChildren": "Int8",
            "MaritalStatus": "string",
            "Age": "Int16",
            "CreditRating": "Int16",
            "OwnOrRentFlag": "string",
            "Employer": "string",
            "NumberCreditCards": "Int8",
            "NetWorth": "Int64",
        },
    ).to_parquet(OUTPUT_PATH)
