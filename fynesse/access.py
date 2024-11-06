from typing import Optional
import requests
from .config import *

"""These are the types of import we might expect in this file
import httplib2
import oauth2
import tables
import mongodb
import sqlite"""

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """


def hello_world():
    print("Hello from the data science library!")


def download_price_paid_data(year_from: int, year_to: int) -> None:
    # Base URL where the dataset is stored
    base_url = (
        "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com"
    )
    """Download UK house price data for given year range"""
    # File name with placeholders
    file_name = "/pp-<year>-part<part>.csv"
    for year in range(year_from, (year_to + 1)):
        print(f"Downloading data for year: {year}")
        for part in range(1, 3):
            url = base_url + file_name.replace("<year>", str(year)).replace(
                "<part>", str(part)
            )
            response = requests.get(url)
            if response.status_code == 200:
                with open(
                    "."
                    + file_name.replace("<year>", str(year)).replace(
                        "<part>", str(part)
                    ),
                    "wb",
                ) as file:
                    file.write(response.content)


def download_open_postcode_geo_data(
    url: str | None = None, file_name: str | None = None
) -> str:
    url = url or "https://www.getthedata.com/downloads/open_postcode_geo.csv.zip"
    file_name = file_name if file_name is not None else "./opg.csv"

    return file_name


def data():
    """Read the data from the web or local file, returning structured format such as a data frame"""
    raise NotImplementedError
