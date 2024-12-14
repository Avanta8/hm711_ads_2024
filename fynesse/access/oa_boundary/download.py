import pandas as pd
import pymysql
# from pypika import Column, Query, Table

from fynesse.access.utils import (
    UploadCsvConfig,
    check_table_exists,
    create_separate_table,
    download_file,
    get_download_path,
)


def download_2021_oa_boundaries() -> str:
    url = "https://open-geography-portalx-ons.hub.arcgis.com/api/download/v1/items/6beafcfd9b9c4c9993a06b6b199d7e6d/csv?layers=0"

    path = get_download_path("oa_boundaries_2021.csv")

    return download_file(url, path)


def load_2021_oa_boundaries() -> pd.DataFrame:
    path = get_download_path("oa_boundaries_2021.csv")
    return pd.read_csv(path)


def upload_2021_oa_boundaries(conn: pymysql.Connection, recreate=True):
    path = download_2021_oa_boundaries()

    config = UploadCsvConfig(
        name="oa_boundaries_2021",
        path=path,
        columns=[
            ("oa", "tinytext"),
            ("lat", "decimal(11,8)"),
            ("lon", "decimal(10,8)"),
            ("area", "decimal(20,5)"),
            ("length", "decimal(20,5)"),
        ],
        primary_key="id",
        order=([1, 7, 8, 9, 10], 11),
        recreate=recreate,
        ignore_lines=1,
    )
    config.upload(conn)


# def upload_2021_oa_boundaries(conn: pymysql.Connection, recreate=True):
#     path = download_2021_oa_boundaries()
#
#     table_name = "oa_boundaries_2021"
#     table = Table(table_name)
#
#     if recreate:
#         cur = conn.cursor()
#         cur.execute(Query.drop_table(table).if_exists().get_sql(quote_char="`"))
#
#     query = (
#         Query.create_table(table)
#         .if_not_exists()
#         .columns(
#             Column("oa", "tinytext"),
#             Column("lat", "decimal(11,8)"),
#             Column("lon", "decimal(10,8)"),
#             Column("area", "decimal(20,5)"),
#             Column("length", "decimal(20,5)"),
#             Column("id", "bigint(20) unsigned", nullable=False),
#         )
#         .primary_key("id")
#     )
#
#     statement = query.get_sql(quote_char="`")
#     print(statement)
#
#     cur = conn.cursor()
#     cur.execute(statement)
#     conn.commit()
