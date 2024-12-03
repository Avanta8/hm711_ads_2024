import io
import os
import zipfile
from dataclasses import dataclass

import pymysql
import pymysql.cursors
import requests


"""
TODO:
    Create function for creating index
"""


def get_download_path(relative_path: str) -> str:
    return os.path.join("./downloads/", relative_path)


def create_connection(user, password, host, database, port=3306):
    """Create a database connection to the MariaDB database
        specified by the host url and database name.
    :param user: username
    :param password: password
    :param host: host url
    :param database: database name
    :param port: port number
    :return: Connection object or None
    """
    try:
        conn = pymysql.connect(
            user=user,
            passwd=password,
            host=host,
            port=port,
            local_infile=1,
            db=database,
        )
        print("Connection established!")
    except Exception as e:
        print(f"Error connecting to the MariaDB Server: {e}")
        raise e
    return conn


@dataclass
class UploadCsvConfig:
    name: str
    path: str | list[str]
    columns: list[tuple[str, str]]
    # primary_key: str | None = None
    primary_key: str | None = None
    order: tuple[list[int], int] | None = None
    recreate: bool = True
    ignore_lines: int = 0


def _create_table(conn: pymysql.Connection, config: UploadCsvConfig):
    lines = []
    if config.recreate:
        cur = conn.cursor()
        # lines.append(f"DROP TABLE IF EXISTS `{config.name}`;")
        cur.execute(f"DROP TABLE IF EXISTS `{config.name}`;")

    lines.append(f"CREATE TABLE IF NOT EXISTS `{config.name}` (")

    columns = [f"`{key}` {rem}" for key, rem in config.columns]
    if config.primary_key is not None:
        columns.append(
            f"`{config.primary_key}` bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY"
        )
        # columns.append("PRIMARY KEY (`id`)")

    lines.append(",\n".join(columns))
    lines.append(") DEFAULT CHARSET=utf8 COLLATE=utf8_bin;")

    statement = "\n".join(lines)
    print(statement)

    cur = conn.cursor()
    cur.execute(statement)
    conn.commit()


def _load_data_infile(conn: pymysql.Connection, config: UploadCsvConfig):
    paths = config.path if isinstance(config.path, list) else [config.path]
    for path in paths:
        statement = rf"""
        LOAD DATA LOCAL INFILE "{path}"
        INTO TABLE `{config.name}`
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED by '"'
        LINES STARTING BY ''
        TERMINATED BY '\n'
        IGNORE {config.ignore_lines or 0} lines
        """
        if config.order is not None:
            order, size = config.order
            upload = ["@dummy"] * size
            for (key, _), idx in zip(config.columns, order):
                upload[idx] = f"`{key}`"

            statement = f"""
            {statement}
            ({", ".join(upload)})
            """

        print(statement)

        cur = conn.cursor()
        cur.execute(statement)
    conn.commit()


def upload_csv(conn: pymysql.Connection, config: UploadCsvConfig):
    # TODO:
    # recreate flag doesn't currently work corrently.
    # It will still reupload the csv even if the table is there.
    # This is not necessarily what we want?
    _create_table(conn, config)
    _load_data_infile(conn, config)


def download_file(url: str, path: str = "") -> str:
    if not path:
        path = get_download_path(os.path.basename(url))

    if os.path.exists(path):
        print(f"Files already exist at: {path}.")
        return path

    print(f"Downloading to {path}")
    response = requests.get(url)
    if response.status_code == 200:
        with open(path, "wb") as file:
            file.write(response.content)
        return path
    else:
        raise Exception(f"Unable to download: {url}")


def download_zip(url: str, path: str) -> str:
    if os.path.exists(path) and os.listdir(path):
        print(f"Files already exist at: {path}.")
        return path

    os.makedirs(path, exist_ok=True)
    response = requests.get(url)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
        zip_ref.extractall(path)

    print(f"Files extracted to: {path}")
    return path
