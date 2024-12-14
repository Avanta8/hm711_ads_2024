import pandas as pd
from pymysql import Connection

from fynesse.access.utils import (
    UploadCsvConfig,
    download_file,
    get_download_path,
    load_table_df,
    normalise_df,
)


ALL_PARTIES = [
    "Con",
    "Lab",
    "LD",
    "RUK",
    "Green",
    "SNP",
    "PC",
    "DUP",
    "SF",
    "SDLP",
    "UUP",
    "APNI",
    "Other",
]


def _create_columns(parties: list[str]):
    columns = [
        ("ONS_ID", "tinytext NOT NULL"),
        ("Constituency_name", "tinytext NOT NULL"),
        ("Country_name", "tinytext NOT NULL"),
        ("Result", "tinytext NOT NULL"),
        ("First_party", "tinytext NOT NULL"),
        ("Second_party", "tinytext NOT NULL"),
        ("Electorate", "int(10) NOT NULL"),
        ("Valid_votes", "int(10) NOT NULL"),
        ("Invalid_votes", "int(10) NOT NULL"),
        ("Majority", "int(10) NOT NULL"),
    ]

    for party in parties:
        columns.append((party, "int(10) NOT NULL"))

    return columns


def get_election_download_path(year: int):
    return get_download_path(f"election/election_{year}.csv")


def download_election(year: int) -> str:
    if year == 2024:
        url = "https://researchbriefings.files.parliament.uk/documents/CBP-10009/HoC-GE2024-results-by-constituency.csv"
    elif year == 2015:
        url = "https://researchbriefings.files.parliament.uk/documents/CBP-7186/HoC-GE2015-results-by-constituency.csv"
    else:
        raise ValueError()
    return download_file(url, get_election_download_path(year))


def load_raw_election(year: int) -> pd.DataFrame:
    return pd.read_csv(get_election_download_path(year))


def upload_election(conn: Connection, year: int, recreate=True):
    path = download_election(year)
    name = f"election_{year}"
    if year == 2024:
        size = 32
    elif year == 2015:
        size = 31
    else:
        raise ValueError

    # 2015 and 2024 have the same parties except UKIP is renamed to RUK

    UploadCsvConfig(
        name=name,
        path=path,
        columns=_create_columns(ALL_PARTIES),
        order=([0, 2, 5] + list(range(11, 31)), size),
        primary_key="id",
        ignore_lines=1,
        recreate=recreate,
    ).upload(conn)


def load_election_df(conn: Connection, year: int):
    table = f"election_{year}"
    df = load_table_df(conn, table)
    return df


def normalise_election_df(df: pd.DataFrame, in_place: bool = False) -> pd.DataFrame:
    if not in_place:
        df = df.copy(deep=True)

    irrelavent_parties = ALL_PARTIES[7:-1]
    relavent_parties = ALL_PARTIES[:7] + ALL_PARTIES[-1:]

    for party in irrelavent_parties:
        df["Other"] += df[party]

    df.drop(columns=irrelavent_parties, inplace=True)

    normalise_df(df, relavent_parties, target="Valid_votes", in_place=True)
    # df2 = normalise_df(df, relavent_parties)
    # print((df == df2).all().all())

    df.drop(
        columns=[
            "Country_name",
            "First_party",
            "Second_party",
            "Electorate",
            "Valid_votes",
            "Invalid_votes",
            "Majority",
            "Result",
            "id",
        ],
        inplace=True,
    )

    # df.set_index("ONS_ID", inplace=True, verify_integrity=True)

    return df


def download_election_historical() -> str:
    url = "https://researchbriefings.files.parliament.uk/documents/CBP-8647/1918-2019election_results.csv"
    path = get_download_path("election/election_historical.csv")
    return download_file(url, path)


def load_election_historical() -> pd.DataFrame:
    path = get_download_path("election/election_historical.csv")
    return pd.read_csv(path, encoding="latin-1")


def get_download_msoa_2021_to_constituency_2024_path():
    path = get_download_path("msoa_2021_to_constituency_2024.csv")
    return path


def download_msoa_2021_to_constituency_2024():
    url = "https://hub.arcgis.com/api/v3/datasets/098360c460dd41beacbdfad83bc4fea2_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1"
    path = get_download_msoa_2021_to_constituency_2024_path()
    return download_file(url, path)


def upload_msoa_2021_to_constituency_2024(conn: Connection, recreate=True):
    path = download_msoa_2021_to_constituency_2024()

    UploadCsvConfig(
        name="msoa_2021_to_constituency_2024",
        path=path,
        columns=[
            ("MSOA21CD", "tinytext NOT NULL"),
            ("PCON25CD", "tinytext NOT NULL"),
        ],
        primary_key="id",
        order=([0, 3], 10),
        recreate=recreate,
        ignore_lines=1,
    ).upload(conn)


def load_join_msoa_to_election_2021(conn: Connection):
    statement = """
    SELECT *
    FROM election_2024
    INNER JOIN msoa_2021_to_constituency_2024 ON election_2024.ONS_ID = msoa_2021_to_constituency_2024.PCON25CD
    """
    return pd.read_sql(statement, conn)


def join_election_census_df(election_df: pd.DataFrame, census_df: pd.DataFrame):
    # return pd.merge(election_df, census_df, left_on="ONS_ID", right_on="PCON25CD")
    return pd.merge(election_df, census_df, left_index=True, right_on="PCON25CD")
