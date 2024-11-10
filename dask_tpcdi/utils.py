from datetime import datetime
import pandas as pd
import dask.dataframe as dd


def track_history_per_key(df: pd.DataFrame, natural_key: str, surrogate_key: str) -> pd.DataFrame:
    df = df.sort_values(by="effective_date", ascending=False)
    df["end_date"] = df["effective_date"].shift(periods=1, fill_value=datetime.fromisoformat("9999-12-31"))
    df["is_current"] = False
    df["is_current"].iloc[0] = True
    df[surrogate_key] = pd.util.hash_pandas_object(df[[natural_key, "effective_date"]])
    return df

def track_history(df: dd.DataFrame, natural_key: str, surrogate_key: str) -> dd.DataFrame:    
    return df.groupby(natural_key).apply(track_history_per_key, natural_key=natural_key, surrogate_key=surrogate_key, include_groups=False)
