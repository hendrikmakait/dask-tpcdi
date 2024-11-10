from dagster import AssetKey, asset

import dask.dataframe as dd
from dask_tpcdi.assets.bronze.constants import COMPANY_PATH as INPUT_PATH
from dask_tpcdi.assets.silver.constants import COMPANY_PATH as OUTPUT_PATH

from dask_tpcdi.utils import track_history

@asset(key_prefix="silver", deps=[AssetKey(["bronze", "company"])])
def company():
    df = dd.read_parquet(INPUT_PATH)
    df = df.rename(
        {
            "PTS": "effective_date",
            "CIK": "company_id",
            "CompanyName": "name",
            "SPrating": "sp_rating",
            "CEOname": "ceo",
            "Description": "description",
            "FoundingDate": "founding_date",
            "AddrLine1": "address_line_1",
            "AddrLine2": "address_line_2",
            "PostalCode": "postal_code",
            "StateProvince": "state_province",
            "Country": "country",
            "Status": "status_id",
            "IndustryID": "industry_id",
        }
    )
    # TODO: Drop unchanged values
    df["is_low_grade"] = df.isnull() | df["sp_rating"].ne("BBB") | ~df["sp_rating"].str.startswith("A")
    # TODO: Implement batch_id
    df = track_history(df, "company_id", "sk_company_id")
    df.to_parquet(OUTPUT_PATH)

# TODO: Implement data quality checks
