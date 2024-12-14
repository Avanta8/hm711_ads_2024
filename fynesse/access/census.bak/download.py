import os

import pandas as pd

from fynesse import access
from fynesse.access.utils import UploadCsvConfig, get_download_path


def get_census_2021_download_directory(code: str) -> str:
    path = f"census/census2021-{code.lower()}"
    return get_download_path(path)


def get_census_2021_download_csv(code: str, level: str) -> str:
    return os.path.join(
        get_census_2021_download_directory(code),
        f"census2021-{code.lower()}-{level}.csv",
    )


def download_census_data_2021(code) -> str:
    url = f"https://www.nomisweb.co.uk/output/census/2021/census2021-{code.lower()}.zip"
    # extract_dir = os.path.join(base_dir, os.path.splitext(os.path.basename(url))[0])
    # extract_dir = os.path.splitext(os.path.basename(url))[0]
    extract_dir = get_census_2021_download_directory(code)

    return access.utils.download_zip(url, extract_dir)


def load_census_data_2021(code, level="msoa"):
    # return pd.read_csv(
    #     f"census2021-{code.lower()}/census2021-{code.lower()}-{level}.csv"
    # )
    return pd.read_csv(get_census_2021_download_csv(code, level))


def upload_nssec(conn, level: str):
    code = "TS062"
    download_census_data_2021(code)
    path = get_census_2021_download_csv(code, level)

    config = UploadCsvConfig(
        name=f"nssec_{level}_2021",
        path=path,
        columns=[
            ("date", "tinytext NOT NULL"),
            ("geography", "tinytext NOT NULL"),
            ("geography_code", "tinytext NOT NULL"),
            ("all", "int(10) unsigned NOT NULL"),
            ("L1-L3", "int(10) unsigned NOT NULL"),
            ("L4-L6", "int(10) unsigned NOT NULL"),
            ("L7", "int(10) unsigned NOT NULL"),
            ("L8-L9", "int(10) unsigned NOT NULL"),
            ("L10-L11", "int(10) unsigned NOT NULL"),
            ("L12", "int(10) unsigned NOT NULL"),
            ("L13", "int(10) unsigned NOT NULL"),
            ("L14", "int(10) unsigned NOT NULL"),
            ("L15", "int(10) unsigned NOT NULL"),
        ],
        primary_key="id",
        order=None,
        recreate=True,
        ignore_lines=1,
    )
    config.upload(conn)
