import os

import pandas as pd
import pymysql

from fynesse import access
from fynesse.access.utils import UploadCsvConfig, get_download_path, normalise_df


COLUMNS_MAP = {
    "ts062": [
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
    ]
}


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


def load_raw_census_data_2021(code, level="msoa"):
    # return pd.read_csv(
    #     f"census2021-{code.lower()}/census2021-{code.lower()}-{level}.csv"
    # )
    return pd.read_csv(get_census_2021_download_csv(code, level))


def upload_census_data_2021(conn, code: str, level: str):
    download_census_data_2021(code)
    path = get_census_2021_download_csv(code, level)

    config = UploadCsvConfig(
        name=f"{code}_{level}_2021",
        path=path,
        columns=COLUMNS_MAP[code],
        primary_key="id",
        order=None,
        recreate=True,
        ignore_lines=1,
    )
    config.upload(conn)


def upload_nssec(conn, level: str):
    code = "ts062"
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


def load_census_2021_for_constituency(
    conn: pymysql.Connection,
    code: str,
    normalise: bool = False,
) -> pd.DataFrame:
    """
    Sum the values for each column over all MSOAs in the constituency
    """
    table = f"`{code}_msoa_2021`"

    statement = f"""
    SELECT *
    FROM {table}
    INNER JOIN msoa_2021_to_constituency_2024 ON {table}.geography_code = msoa_2021_to_constituency_2024.MSOA21CD
    """
    df = pd.read_sql(statement, conn)
    df.drop(
        columns=["date", "geography", "geography_code", "id", "MSOA21CD"], inplace=True
    )

    df: pd.DataFrame = df.groupby("PCON25CD").sum()
    if normalise:
        normalise_df(df, list(df.columns)[1:], target="all", keep=False, in_place=True)
    return df
