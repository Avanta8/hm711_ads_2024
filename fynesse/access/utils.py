import io
import pandas as pd
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

    def _create_table(self, conn: pymysql.Connection):
        lines = []
        if self.recreate:
            cur = conn.cursor()
            # lines.append(f"DROP TABLE IF EXISTS `{self.name}`;")
            cur.execute(f"DROP TABLE IF EXISTS `{self.name}`;")

        lines.append(f"CREATE TABLE IF NOT EXISTS `{self.name}` (")

        columns = [f"`{key}` {rem}" for key, rem in self.columns]
        if self.primary_key is not None:
            columns.append(
                f"`{self.primary_key}` bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY"
            )
            # columns.append("PRIMARY KEY (`id`)")

        lines.append(",\n".join(columns))
        lines.append(") DEFAULT CHARSET=utf8 COLLATE=utf8_bin;")

        statement = "\n".join(lines)
        print(statement)

        cur = conn.cursor()
        cur.execute(statement)
        conn.commit()

    def _load_data_infile(self, conn: pymysql.Connection):
        paths = self.path if isinstance(self.path, list) else [self.path]
        for path in paths:
            statement = rf"""
            LOAD DATA LOCAL INFILE "{path}"
            INTO TABLE `{self.name}`
            FIELDS TERMINATED BY ','
            OPTIONALLY ENCLOSED by '"'
            LINES STARTING BY ''
            TERMINATED BY '\n'
            IGNORE {self.ignore_lines or 0} lines
            """
            if self.order is not None:
                order, size = self.order
                upload = ["@dummy"] * size
                for (key, _), idx in zip(self.columns, order):
                    upload[idx] = f"`{key}`"

                statement = f"""
                {statement}
                ({", ".join(upload)})
                """

            print(statement)

            cur = conn.cursor()
            cur.execute(statement)
        conn.commit()

    def upload(self, conn: pymysql.Connection):
        # TODO:
        # recreate flag doesn't currently work corrently.
        # It will still reupload the csv even if the table is there.
        # This is not necessarily what we want?
        # We could add another flag - reupload?
        self._create_table(conn)
        self._load_data_infile(conn)


def add_primary_key(conn: pymysql.Connection, table: str, field: str):
    statement = f"""
    ALTER TABLE `{table}`
    ADD PRIMARY KEY (`{field}`)
    """
    print(statement)
    cur = conn.cursor()
    cur.execute(statement)
    conn.commit()


def add_index(
    conn: pymysql.Connection,
    table: str,
    index_column: str | list[str],
    index_name: str | None = None,
):
    if isinstance(index_column, str):
        index_column = [index_column]
    columns = ", ".join(f"`{c}`" for c in index_column)

    statement = f"""
    ALTER TABLE `{table}`
    ADD INDEX {f"`{index_name}`" if index_name else ""}({columns})
    """
    print(statement)
    cur = conn.cursor()
    cur.execute(statement)
    conn.commit()


def check_table_exists(conn, table):
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = "{table}" 
        """
    )
    if cur.fetchone()[0] == 1:
        cur.close()
        return True

    cur.close()
    return False


def create_separate_table(
    conn, source_table: str, new_name: str, select: dict[str, str]
):
    where = " AND ".join(f"`{k}` = '{v}'" for k, v in select.items())

    statement = f"""
    CREATE TABLE `{new_name}` AS
    SELECT *
    FROM `{source_table}`
    WHERE {where}
    """

    print(statement)

    cur = conn.cursor()
    cur.execute(statement)
    conn.commit()


def load_table_df(conn, table) -> pd.DataFrame:
    statement = f"SELECT * FROM {table}"
    return pd.read_sql(statement, conn)


def normalise_df(
    df: pd.DataFrame,
    columns: list[str],
    target: str | None = None,
    keep: bool = True,
    in_place: bool = False,
) -> pd.DataFrame:
    """keep: keep the column that you normalise by or not"""
    if not in_place:
        df = df.copy()

    if target is not None:
        total = df[target].copy()
    else:
        total = df[columns[0]].copy()
        for c in columns[1:]:
            total += df[c]

    for c in columns:
        df[c] = df[c] / total

    if target is not None and not keep:
        df.drop(columns=[target], inplace=True)

    return df
