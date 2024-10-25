from dagster import load_assets_from_package_module

from dask_tpcdi.assets import bronze

bronze_assets = load_assets_from_package_module(package_module=bronze, group_name="bronze")
