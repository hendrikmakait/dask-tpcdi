from setuptools import find_packages, setup

setup(
    name="dask_tpcdi",
    packages=find_packages(exclude=["dask_tpcdi_tests"]),
    install_requires=["dagster", "dagster-cloud"],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
