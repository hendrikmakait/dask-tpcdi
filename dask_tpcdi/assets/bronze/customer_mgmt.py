from datetime import datetime
from typing import TypedDict
from dagster import AssetSpec, asset, multi_asset

from dask_tpcdi.assets.staging.constants import CUSTOMER_MGMT_FILE_PATH
from dask_tpcdi.assets.bronze.constants import ACCOUNT_PATH, CUSTOMER_PATH
import lxml.etree



class PhoneNumber(TypedDict):
    C_CTRY_CODE: str
    C_AREA_CODE: str
    C_LOCAL: str
    C_EXT: str | None


class Action(TypedDict):
    ActionType: str
    ActionTS: str

class Name(TypedDict):
    C_L_NAME: str | None
    C_F_NAME: str | None
    C_M_NAME: str | None

class Address(TypedDict):
    C_ADLINE1: str | None
    C_ADLINE2: str | None
    C_ZIPCODE: str | None
    C_CITY: str | None
    C_STATE_PROV : str | None
    C_CTRY: str | None


class ContactInfo(TypedDict):
    C_PRIM_EMAIL: str | None
    C_ALT_EMAIL: str | None
    C_PHONE_1: PhoneNumber | None
    C_PHONE_2: PhoneNumber | None
    C_PHONE_3: PhoneNumber | None

class TaxInfo(TypedDict):
    C_LCL_TX_ID: str | None
    C_NAT_TX_ID: str | None

class Customer(Action, Name, Address, ContactInfo, TaxInfo):
    # Attributes
    C_ID: str
    C_TAX_ID: str | None
    C_GNDR: str | None
    C_TIER: int | None
    C_DOB: datetime | None


class Account(Action):
    C_ID: str
    CA_ID: str
    CA_B_ID: str | None
    CA_NAME: str | None
    CA_TAX_ST: str | None


import pandas as pd
import pyarrow as pa


phone_number_dtype = pd.ArrowDtype(
    pa.struct(
        [
            ("C_CTRY_CODE", pa.string()),
            ("C_AREA_CODE", pa.string()),
            ("C_LOCAL", pa.string()),
            ("C_EXT", pa.string()),
        ]
    )
)

def extract_action(action: lxml.etree.Element) -> Action:
    return Action(
        ActionType=action.get("ActionType"),
        ActionTS=action.get("ActionTS"),
    )

def extract_phone_number(contact_info: lxml.etree.Element, id: int) -> PhoneNumber | None:
    phone = contact_info.find(f"C_PHONE_{id}")
    if phone is None:
        return None

    return PhoneNumber(
        C_CTRY_CODE=phone.findtext("C_CTRY_CODE"),
        C_AREA_CODE=phone.findtext("C_AREA_CODE"),
        C_LOCAL=phone.findtext("C_LOCAL"),
        C_EXT=phone.findtext("C_EXT"),
    )

def extract_name(customer: lxml.etree.Element) -> Name:
    name = customer.find("Name")
    if name is None:
        return Name(
            C_F_NAME=None,
            C_L_NAME=None,
            C_M_NAME=None,
        )

    return Name(
        C_F_NAME=name.findtext("C_F_NAME"),
        C_L_NAME=name.findtext("C_L_NAME"),
        C_M_NAME=name.findtext("C_M_NAME"),
    )

def extract_address(customer: lxml.etree.Element) -> Address:
    address = customer.find("Address")
    if address is None:
        return Address(
            C_ADLINE1=None,
            C_ADLINE2=None,
            C_ZIPCODE=None,
            C_CITY=None,
            C_STATE_PROV=None,
            C_CTRY=None,
        )

    return Address(
        C_ADLINE1=address.findtext("C_ADLINE1"),
        C_ADLINE2=address.findtext("C_ADLINE2"),
        C_ZIPCODE=address.findtext("C_ZIPCODE"),
        C_CITY=address.findtext("C_CITY"),
        C_STATE_PROV=address.findtext("C_STATE_PROV"),
        C_CTRY=address.findtext("C_CTRY"),
    )


def extract_contact_info(customer: lxml.etree.Element) -> ContactInfo:
    contact_info = customer.find("ContactInfo")
    if contact_info is None:
        return ContactInfo(
            C_PRIM_EMAIL=None,
            C_ALT_EMAIL=None,
            C_PHONE_1=None,
            C_PHONE_2=None,
            C_PHONE_3=None,
        )
    
    return ContactInfo(
        C_PRIM_EMAIL=contact_info.findtext("C_PRIM_EMAIL"),
        C_ALT_EMAIL=contact_info.findtext("C_ALT_EMAIL"),
        C_PHONE_1=extract_phone_number(contact_info, 1),
        C_PHONE_2=extract_phone_number(contact_info, 2),
        C_PHONE_3=extract_phone_number(contact_info, 3),
    )

def extract_tax_info(customer: lxml.etree.Element) -> TaxInfo:
    tax_info = customer.find("TaxInfo")
    if tax_info is None:
        return TaxInfo(
            C_LCL_TX_ID=None,
            C_NAT_TX_ID=None,
        )
    
    return TaxInfo(
        C_LCL_TX_ID=tax_info.findtext("C_LCL_TX_ID"),
        C_NAT_TX_ID=tax_info.findtext("C_NAT_TX_ID"),
    )

def extract_customer(action: lxml.etree.Element) -> Customer:
    element = action.find("Customer")
    return {
        "C_ID": element.get("C_ID"),
        "C_TAX_ID": element.get("C_TAX_ID"),
        "C_GNDR": element.get("C_GNDR"),
        "C_TIER": element.get("C_TIER"),
        "C_DOB": element.get("C_DOB"),
        **extract_action(action),
        **extract_name(element),
        **extract_address(element),
        **extract_contact_info(element),
        **extract_tax_info(element),
    }

def extract_account(account: lxml.etree.Element, customer: Customer) -> Account:
    return Account(
        ActionType=customer["ActionType"],
        ActionTS=customer["ActionTS"],
        C_ID=customer["C_ID"],
        CA_ID=account.get("CA_ID"),
        CA_B_ID=account.findtext("CA_B_ID"),
        CA_NAME=account.findtext("CA_NAME"),
        CA_TAX_ST=account.get("CA_TAX_ST"),
    )

def extract_accounts(action: lxml.etree.Element, customer: Customer) -> list[Account]:
    accounts = action.find("Customer").findall("Account")
    return [extract_account(account, customer) for account in accounts]


@multi_asset(specs=[AssetSpec("customer"), AssetSpec("account")])
def customer_mgmt():
    with open(CUSTOMER_MGMT_FILE_PATH) as f:
        tree = lxml.etree.parse(f)

    namespaces = tree.getroot().nsmap
    actions = tree.xpath("TPCDI:Action", namespaces=namespaces)
    customers = []
    accounts = []
    for action in actions:
        customer = extract_customer(action)
        customers.append(customer)
        accounts.extend(extract_accounts(action, customer))
    ACCOUNT_PATH.mkdir()
    CUSTOMER_PATH.mkdir()
    pd.DataFrame(data=accounts).to_parquet(ACCOUNT_PATH / "account.parquet")
    pd.DataFrame(data=customers).to_parquet(CUSTOMER_PATH / "customer.parquet")
