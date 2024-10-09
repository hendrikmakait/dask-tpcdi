from dagster import asset


@asset
def customer_mgmt() -> None:
    raise NotImplementedError()
