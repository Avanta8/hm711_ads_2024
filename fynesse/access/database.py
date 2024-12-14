import pandas as pd
import numpy as np
import haversine
import typing
import enum

from fynesse.access.osm.download import get_box_coords, get_osm_counts


def get_nssec_oa_boundary_2021(conn, oa: str):
    statement = f"""
    SELECT * FROM (
        SELECT * FROM nssec_oa_2021
        WHERE geography = "{oa}"
    ) as ns
    INNER JOIN (
        SELECT * FROM oa_boundaries_2021
        WHERE oa = "{oa}"
    ) AS b ON ns.geography = b.oa
    """

    return pd.read_sql(statement, con=conn)


def nearest_entry(df, lat, lon):
    """The distance in km"""
    return min(
        haversine.haversine((lat, lon), (row.lat, row.lon)) for _, row in df.iterrows()
    )


class Feature(enum.Enum):
    Count = enum.auto()
    Distance = enum.auto()


def get_features(conn, oas, features: list[tuple[Feature, typing.Any]]):
    res = []
    for oa in oas:
        nssec_boundary = get_nssec_oa_boundary_2021(conn, oa)
        lat, lon = nssec_boundary.lat[0], nssec_boundary.lon[0]

        arr = []
        for feature_type, feature_val in features:
            if feature_type == Feature.Count:
                dist, key, value = feature_val
                coords = get_box_coords(lat, lon, dist)
                count = get_osm_counts(conn, key, value, coords)
                arr.append(count)
            else:
                value = nearest_entry(feature_val, lat, lon)
                arr.append(value)

        res.append(np.array(arr))

    return np.array(res)


def get_students(conn, oas):
    statement = f"""
    SELECT `all`, L15 FROM nssec_oa_2021
    WHERE geography in ({", ".join(f"'{oa}'" for oa in oas)})
    """

    # statement = """
    # SELECT `all`, L15
    # FROM nssec_oa_2021
    # WHERE geography = "E00000002"
    # """

    df = pd.read_sql(statement, conn)
    return np.array(df.L15 / df["all"])
