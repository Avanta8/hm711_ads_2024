import os
import osmium
import pymysql
from fynesse.access.utils import (
    UploadCsvConfig,
    download_file,
    get_download_path,
    upload_csv,
)


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
    for filename in sorted(os.listdir(osm_basepath))[:1]:
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

    upload_csv(conn, config)
