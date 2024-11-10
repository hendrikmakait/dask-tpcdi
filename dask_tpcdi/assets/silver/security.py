from dagster import AssetKey, asset

import dask.dataframe as dd

from dask_tpcdi.assets.bronze.constants import SECURITY_PATH as INPUT_PATH
from dask_tpcdi.assets.silver.constants import COMPANY_PATH, SECURITY_PATH as OUTPUT_PATH, STATUS_TYPE_PATH
import pandas as pd
from dask_tpcdi.utils import track_history

@asset(key_prefix="silver", deps=[AssetKey(["bronze", "security"]), AssetKey(["silver", "company"])])
def security():
    securities = dd.read_parquet(INPUT_PATH)
    companies = dd.read_parquet(COMPANY_PATH)
    status_types = dd.read_parquet(STATUS_TYPE_PATH)
    securities = securities.rename({
        "RecType": "rec_type",
        "Symbol": "symbol",
        "IssueType": "issue_type",
        "Status": "status_id",
        "Name": "name",
        "ExID": "exchange_id",
        "ShOut": "shares_outstanding",
        "FirstTradeDate": "first_trade_date",
        "FirstTradeExchg": "first_trade_date_on_exchange",
        "Dividend": "dividend",
    })
    securities = dd.merge(securities, status_types, how="inner", on="status_id")
    is_cik = securities["CoNameOrCIK"].str.isdigit()
    df_cik = securities[is_cik]
    df_cik["cik"] = df_cik["CoNameOrCIK"].astype(int)
    df_cik = dd.merge(df_cik, companies, how="inner", left_on="cik", right_on="company_id")
    df_cik[(df_cik["PTS"] >= df_cik["effective_date"]) & df_cik["PTS"] < securities["end_date"]]
    df_name = securities[~is_cik]
    df_name["company_name"] = securities["CoNameOrCIK"]
    df_name = dd.merge(df_name, companies, how="inner", left_on="company_name", right_on="company_id")
    df_name[(df_name["PTS"] >= df_name["effective_date"]) & df_name["PTS"] < securities["end_date"]]
    securities = dd.concat([df_cik, df_name])
    securities = track_history(securities, "symbol", "sk_security_id")
    securities = securities.rename({"status_name": "status"})
    securities = securities[["sk_security_id", "symbol", "issue", "name", "exchange_id", "shares_outstanding", "first_trade_date", "first_trade_date_on_exchange", "dividend", "sk_company_id", "status"]]
    # TODO: Implement batch_id
    securities.to_parquet(OUTPUT_PATH)
