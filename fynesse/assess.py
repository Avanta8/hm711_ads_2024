import pandas as pd
import osmnx as ox
import matplotlib.pyplot as plt
import math
from sklearn.cluster import KMeans
from scipy.spatial.distance import pdist, squareform

from .config import *
from . import access

"""These are the types of import we might expect in this file
import pandas
import bokeh
import seaborn
import matplotlib.pyplot as plt
import sklearn.decomposition as decomposition
import sklearn.feature_extraction"""

"""Place commands in this file to assess the data you have downloaded. How are missing values encoded, how are outliers encoded? What do columns represent, makes rure they are correctly labeled. How is the data indexed. Crete visualisation routines to assess the data (e.g. in bokeh). Ensure that date formats are correct and correctly timezoned."""


def data():
    """Load the data from access and ensure missing values are correctly encoded as well as indices correct, column names informative, date and times correctly formatted. Return a structured data structure such as a data frame."""
    df = access.data()
    raise NotImplementedError


def query(data):
    """Request user input for some aspect of the data."""
    raise NotImplementedError


def view(data):
    """Provide a view of the data that allows the user to verify some aspect of its quality."""
    raise NotImplementedError


def labelled(data):
    """Provide a labelled set of data ready for supervised learning."""
    raise NotImplementedError


def count_pois_near_coordinates(
    latitude: float, longitude: float, tags: dict, distance_km: float = 1.0
) -> dict:
    """
    Count Points of Interest (POIs) near a given pair of coordinates within a specified distance.
    Args:
        latitude (float): Latitude of the location.
        longitude (float): Longitude of the location.
        tags (dict): A dictionary of OSM tags to filter the POIs (e.g., {'amenity': True, 'tourism': True}).
        distance_km (float): The distance around the location in kilometers. Default is 1 km.
    Returns:
        dict: A dictionary where keys are the OSM tags and values are the counts of POIs for each tag.
    """
    dist = distance_km * 1000
    pois = ox.features_from_point((latitude, longitude), tags, dist)

    pois_df = pd.DataFrame(pois)
    pois_df["latitude"] = pois_df.apply(lambda row: row.geometry.centroid.y, axis=1)
    pois_df["longitude"] = pois_df.apply(lambda row: row.geometry.centroid.x, axis=1)

    poi_counts = {}
    for tag, values in tags.items():
        column = pois_df.get(tag)
        if column is not None:
            if values is True:
                indices = column.notnull()
            else:
                indices = column.isin(values)
            count = indices.sum()
        else:
            count = 0
        poi_counts[tag] = count

    return poi_counts


def box_coords(latitude, longitude, distance=1.0):
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
    return (latitude - dlat, longitude - dlong), (latitude + dlat, longitude + dlong)


def get_feature_counts(
    locations_dict: dict[str, tuple[int, int]], tags: dict[str, bool | list[str]]
):
    feature_counts = []
    for location, coordinates in locations_dict.items():
        poi_counts = count_pois_near_coordinates(*coordinates, tags, 1)
        feature_counts.append({**poi_counts, "location": location})
    return pd.DataFrame(feature_counts)


def drop_location(df):
    return feature_counts_df.drop(columns=["location"])


def kmeans_features(feature_counts_df, n_clusters: int):
    kmeans = KMeans(n_clusters=n_clusters)
    labels = kmeans.fit(drop_location(feature_counts_df))
    groups = {}
    for i, label in enumerate(labels.labels_):
        groups.setdefault(label, []).append(feature_counts_df.loc[i]["location"])
    return groups


def normalize_feature_counts(feature_counts_df, drop=False):
    if drop:
        feature_counts_dropped_df = drop_location(feature_counts_df)
    else:
        feature_counts_dropped_df = feature_counts_df
    normalized_feature_counts_df = (
        feature_counts_dropped_df - feature_counts_dropped_df.mean()
    ) / feature_counts_dropped_df.std()
    if drop:
        normalized_feature_counts_df["location"] = feature_counts_df["location"]
    return normalized_feature_counts_df


def get_distance_matrix(feature_counts_df):
    distance_vector = pdist(drop_location(feature_counts_df))
    distance_matrix = squareform(distance_vector)
    return pd.DataFrame(
        distance_matrix,
        columns=feature_counts_df.location.unique(),
        index=feature_counts_df.location.unique(),
    )


def filter_nan_columns(df, columns):
    """For any column in `columns`, it is removed from `df` iff any of the rows contain a
    NaN value for that column"""
    df = df.copy(deep=True)
    for column in columns:
        df = df[df[column].notnull()]
    return df


def merge_dfs(df_left, df_right, left_on, right_on):
    """Will return a dataframe with all entries in the merged column casefolded.
    Does not mutate the input."""
    df_left = df_left.copy(deep=True)
    df_right = df_right.copy(deep=True)

    for field in left_on:
        df_left[field] = df_left[field].str.casefold()
    for field in right_on:
        df_right[field] = df_right[field].str.casefold()

    return df_left.merge(df_right, left_on=left_on, right_on=right_on)


def plot_correlation(feature_counts_df):
    correlations = feature_counts_df.corr(numeric_only=True)

    fig, ax = plt.subplots()
    im = ax.matshow(correlations)

    fig.colorbar(im, ax=ax)
    count = len(feature_counts_df.columns)
    ax.set_xticks(range(count), feature_counts_df.columns, fontsize=14, rotation=45)
    ax.set_yticks(range(count), feature_counts_df.columns, fontsize=14)


def plot_buildings(max_lat, min_lat, max_long, min_long, place_name=None):
    all_buildings_df = ox.geometries_from_bbox(
        max_lat, min_lat, max_long, min_long, {"building": True}
    )
    address_columns = ["addr:street", "addr:housenumber", "addr:postcode"]
    all_buildings_filtered_df = filter_nan_columns(all_buildings_df, address_columns)

    fig, ax = plt.subplots()

    if place_name is not None:
        graph = ox.graph_from_bbox(max_lat, min_lat, max_long, min_long)
        # Retrieve nodes and edges
        nodes, edges = ox.graph_to_gdfs(graph)
        # Plot street edges
        edges.plot(ax=ax, linewidth=1, edgecolor="dimgray")

    ax.set_xlim([min_long, max_long])
    ax.set_ylim([min_lat, max_lat])
    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")

    # Plot all POIs
    all_buildings_df.plot(ax=ax, color="green", alpha=0.7, markersize=10)
    all_buildings_filtered_df.plot(ax=ax, color="blue", alpha=0.7, markersize=10)
    plt.tight_layout()
