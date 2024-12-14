import pandas as pd
import math
import os
import osmium
import pymysql
import fynesse
from fynesse.access.utils import (
    UploadCsvConfig,
    add_index,
    add_primary_key,
    check_table_exists,
    create_separate_table,
    download_file,
    get_download_path,
    load_table_df,
)


def get_box_coords(latitude, longitude, distance=1.0):
    """Returns (min_lat, min_long), (max_lat, max_long)"""
    lat_rad = math.radians(latitude)
    long_rad = math.radians(longitude)
    mlat = 111132.92 - 559.82 * math.cos(2 * lat_rad) + 1.175 * math.cos(4 * lat_rad)
    mlong = (
        111412.84 * math.cos(lat_rad)
        - 93.5 * math.cos(3 * lat_rad)
        - 0.11 * math.cos(5 * lat_rad)
    )
    # mlat = meters per degree
    # we want degree per meter
    dlat = 1 / mlat * 1000 / 2 * distance
    dlong = 1 / mlong * 1000 / 2 * distance
    return (latitude - dlat, longitude - dlong, latitude + dlat, longitude + dlong)


def download_osm() -> str:
    return download_file(
        "https://download.openstreetmap.fr/extracts/europe/united_kingdom-latest.osm.pbf"
    )


def osm_to_csv():
    osm_filepath = download_osm()
    basepath = get_download_path("osm/")
    try:
        os.makedirs(basepath)
    except OSError:
        return basepath

    target_batch_size = 1_000_000
    batch_no = 0
    batch = []

    # PERF:
    # This is terrible - doing this just to find how many entries there are
    entries_size = sum(
        1
        for _ in osmium.FileProcessor(osm_filepath, osmium.osm.NODE).with_filter(
            osmium.filter.EmptyTagFilter()
        )
    )

    for i, obj in enumerate(
        osmium.FileProcessor(osm_filepath, osmium.osm.NODE).with_filter(
            osmium.filter.EmptyTagFilter()
        )
    ):
        base_data = [
            obj.id,
            obj.lat,
            obj.lon,
            obj.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
        ]
        for tag in obj.tags:
            if tag.k == "source" or tag.k == "created_by":
                continue
            data = base_data + [tag.k, tag.v]
            batch.append(data)

        if len(batch) >= target_batch_size or i + 1 == entries_size:
            print(
                f"writing batch {batch_no}. {i}/{entries_size}. {round(i/entries_size*100,2)}%"
            )
            filepath = os.path.join(basepath, f"batch_{batch_no}.csv")

            text = "\n".join(",".join(map(str, row)) for row in batch)

            with open(filepath, "w") as f:
                f.write(text)

            batch.clear()
            batch_no += 1

    return basepath


def upload_osm(conn: pymysql.Connection, recreate=True):
    osm_basepath = osm_to_csv()

    paths = []
    for filename in sorted(os.listdir(osm_basepath)):
        filepath = os.path.join(osm_basepath, filename)
        paths.append(filepath)

    print(paths)
    config = UploadCsvConfig(
        name="osm",
        path=paths,
        columns=[
            ("osm_id", "bigint(20) unsigned NOT NULL"),
            ("lat", "decimal(11,8) NOT NULL"),
            ("lon", "decimal(10,8) NOT NULL"),
            ("timestamp", "date NOT NULL"),
            ("key", "tinytext NOT NULL"),
            ("value", "tinytext NOT NULL"),
        ],
        primary_key="id",
        recreate=recreate,
    )
    config.upload(conn)


def create_subtables(
    conn: pymysql.Connection,
    key: str | None = None,
    value: str | None = None,
):
    """
    Creates a table containing all entries from the `osm` table that
    match key = `key` and value = `value`.
    """
    if key and value is None:
        table = get_table_name(key, value)
        if not check_table_exists(conn, table):
            create_separate_table(conn, "osm", table, {"key": key})
            add_index(conn, table, ["lat", "lon"], "coordinate")
            add_primary_key(conn, table, "id")

    elif key and value:
        base_table = get_table_name(key, None)
        table = get_table_name(key, value)
        if not check_table_exists(conn, table):
            # if not check_table_exists(conn, base_table):

            # Make sure the parent table exists
            create_subtables(conn, key=key)

            create_separate_table(conn, base_table, table, {"value": value})
            add_index(conn, table, ["lat", "lon"], "coordinate")
            add_primary_key(conn, table, "id")

    else:
        raise ValueError(f"key: {key}, value: {value}")

    return table


def get_table_name(key, value):
    if key is not None and value is not None:
        return f"osm_{key}_{value}"
    elif key is not None:
        return f"osm_{key}_"
    else:
        raise ValueError(f"key: {key}, value: {value} is not valid")


def get_osm_counts(
    conn: pymysql.Connection,
    key: str | None = None,
    value: str | None = None,
    coords: tuple[float, float, float, float] | None = None,
):
    table = create_subtables(conn, key, value)
    # table = get_table_name(key, value)

    if coords is not None:
        n, w, s, e = coords

        statement = f"""
        SELECT count(*) FROM `{table}`
        WHERE lat BETWEEN {n} and {s}
            AND lon BETWEEN {w} and {e}
        """
    else:
        statement = f"""
        SELECT count(*) FROM `{table}`
        """

    cur = conn.cursor()
    cur.execute(statement)
    return cur.fetchone()[0]


def load_subtable_df(conn, key, value) -> pd.DataFrame:
    table = get_table_name(key, value)
    # # statement = f"""
    # # SELECT * FROM {table}
    # # """
    # return pd.read_sql(statement, conn)
    return load_table_df(conn, table)


# def create_separate_osm_table(conn, key=None, value=None):
#     if key is None and value is None:
#         raise ValueError
#
#     table_name = f"osm_{key or ''}_{value or ''}"
#     where = (
#         f"value = '{value}'"
#         if key is None
#         else f"`key` = '{key}'"
#         if value is None
#         else f"`key` = '{key}' AND value = '{value}'"
#     )
#
#     statement = f"""
#     CREATE TABLE `{table_name}` AS
#     SELECT *
#     FROM osm
#     WHERE {where};
#     """
#     print(statement)
#
#     cur = conn.cursor()
#     cur.execute(statement)
#     conn.commit()
