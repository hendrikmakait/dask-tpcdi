from dagster import load_assets_from_package_module

from dask_tpcdi.assets import bronze, silver, gold

bronze_assets = load_assets_from_package_module(package_module=bronze, group_name="bronze")
silver_assets = load_assets_from_package_module(package_module=silver, group_name="silver")
gold_assets = load_assets_from_package_module(package_module=gold, group_name="gold")
