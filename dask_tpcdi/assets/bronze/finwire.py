from collections.abc import Iterable
from enum import StrEnum
from dagster import asset
import dask.bag as db
import dask.dataframe as dd
from dask_tpcdi.assets.staging.constants import FINWIRE_FILES_PATH as INPUT_PATH
from dask_tpcdi.assets.bronze.constants import COMPANY_PATH, FINANCIAL_PATH, SECURITY_PATH
import dask


class RecordType(StrEnum):
    Company = "CMP"
    Financial = "FIN"
    Security = "SEC"


@asset
def company() -> None:
    with dask.config.set({"dataframe.convert-string": True}):  # pyright: ignore[reportPrivateImportUsage]
        records = load_records(RecordType.Company)
        indices = [15, 3, 60, 10, 4, 2, 4, 8, 80, 80, 12, 25, 20, 24, 46, 150]
        records = records.map(split_by_lengths, indices, variable_end=False)
        records = records.map(strip_and_add_nulls)
        company = records.to_dataframe(
            columns=(
                "PTS",
                "RecType",
                "CompanyName",
                "CIK",
                "Status",
                "IndustryID",
                "SPrating",
                "FoundingDate",
                "AddrLine1",
                "AddrLine2",
                "PostalCode",
                "City",
                "StateProvince",
                "Country",
                "CEOname",
                "Description",
            )
        )
        company["PTS"] = dd.to_datetime(company["PTS"], format="%Y%m%d-%H%M%S")  # pyright: ignore[reportPrivateImportUsage]
        company["FoundingDate"] = dd.to_datetime(company["FoundingDate"], "%Y%m%d")  # pyright: ignore[reportPrivateImportUsage]
        company["year"] = company["PTS"].dt.year
        company["quarter"] = company["PTS"].dt.quarter
        company.to_parquet(COMPANY_PATH, partition_on=["year", "quarter"])


@asset
def financial() -> None:
    with dask.config.set({"dataframe.convert-string": True}):  # pyright: ignore[reportPrivateImportUsage]
        records = load_records(RecordType.Financial)
        indices = [15, 3, 4, 1, 8, 8, 17, 17, 12, 12, 12, 17, 17, 17, 13, 13]
        records = records.map(split_by_lengths, indices, variable_end=True)
        records = records.map(strip_and_add_nulls)
        financial = records.to_dataframe(
            columns=(
                "PTS",
                "RecType",
                "Year",
                "Quarter",
                "QtrStartDate",
                "PostingDate",
                "Revenue",
                "Earnings",
                "EPS",
                "DilutedEPS",
                "Margin",
                "Inventory",
                "Assets",
                "Liabilities",
                "ShOut",
                "DilutedShOut",
                "CoNameOrCIK",
            )
        )
        financial = financial.astype(
            {
                "Year": "int",
                "Quarter": "int",
                "Revenue": "float",
                "Earnings": "float",
                "EPS": "float",
                "DilutedEPS": "float",
                "Margin": "float",
                "Inventory": "float",
                "Assets": "float",
                "Liabilities": "float",
                "ShOut": "int",
                "DilutedShOut": "int",
            }
        )
        financial["PTS"] = dd.to_datetime(financial["PTS"], format="%Y%m%d-%H%M%S")  # pyright: ignore[reportPrivateImportUsage]
        financial["QtrStartDate"] = dd.to_datetime(financial["QtrStartDate"], "%Y%m%d")  # pyright: ignore[reportPrivateImportUsage]
        financial["PostingDate"] = dd.to_datetime(financial["PostingDate"], "%Y%m%d")  # pyright: ignore[reportPrivateImportUsage]
        financial["year"] = financial["PTS"].dt.year
        financial["quarter"] = financial["PTS"].dt.quarter
        financial.to_parquet(FINANCIAL_PATH, partition_on=["year", "quarter"])


@asset
def security() -> None:
    with dask.config.set({"dataframe.convert-string": True}):  # pyright: ignore[reportPrivateImportUsage]
        records = load_records(RecordType.Security)
        indices = [15, 3, 15, 6, 4, 70, 6, 13, 8, 8, 12]
        records = records.map(split_by_lengths, indices, variable_end=True)
        records = records.map(strip_and_add_nulls)
        security = records.to_dataframe(
            columns=(
                "PTS",
                "RecType",
                "Symbol",
                "IssueType",
                "Status",
                "Name",
                "ExID",
                "ShOut",
                "FirstTradeDate",
                "FirstTradeExchg",
                "Dividend",
                "CoNameOrCIK",
            )
        )
        security = security.astype({"ShOut": "int", "Dividend": "float"})
        security["PTS"] = dd.to_datetime(security["PTS"], format="%Y%m%d-%H%M%S")  # pyright: ignore[reportPrivateImportUsage]
        security["FirstTradeDate"] = dd.to_datetime(
            security["FirstTradeDate"], "%Y%m%d"
        )  # pyright: ignore[reportPrivateImportUsage]
        security["FirstTradeExchg"] = dd.to_datetime(
            security["FirstTradeExchg"], "%Y%m%d"
        )  # pyright: ignore[reportPrivateImportUsage]
        security["year"] = security["PTS"].dt.year
        security["quarter"] = security["PTS"].dt.quarter
        security.to_parquet(SECURITY_PATH, partition_on=["year", "quarter"])


def load_records(type: RecordType) -> db.Bag:  # pyright: ignore[reportPrivateImportUsage]
    return db.read_text(INPUT_PATH).filter(lambda row: row[15:18] == type)  # pyright: ignore[reportPrivateImportUsage]


def split_by_lengths(
    input: str, lengths: Iterable[int], variable_end: bool = False
) -> list[str]:
    start = 0
    output = []
    for length in lengths:
        output.append(input[start : start + length])
        start += length
    if variable_end:
        output.append(input[start:])
    return output


def strip_and_add_nulls(inputs: Iterable[str]) -> tuple[str | None, ...]:
    inputs = (input.strip() for input in inputs)
    return tuple(input if input else None for input in inputs)