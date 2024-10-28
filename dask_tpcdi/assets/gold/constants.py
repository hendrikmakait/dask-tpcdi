from pathlib import Path

from dask_tpcdi.assets.bronze.constants import COMPANY_PATH


DATA_PATH = Path("data") / "silver"

COMPANY_PATH = DATA_PATH / "dim_company"
DATE_PATH = DATA_PATH / "dim_date"
TIME_PATH = DATA_PATH / "dim_time"