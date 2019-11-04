from os import path
import schedule
import time
from datetime import datetime, timedelta
from typing import NamedTuple
import yaml

from psycopg2.extensions import AsIs

from ..hive_api import leaderboard
from .sql import Postgres

LEADERBOARD_LENGTH = 1000  # Number of players on the Hive leaderboard
API_MAX_CALL_SIZE = 200  # Max leaderboard entries retrievable per api call

SQL_NOW = "now()"  # Constant for the timestamp function used in postgres
UNIT_DICT = {  # Shortcode mapping for time units
    "s": timedelta(seconds=1),
    "m": timedelta(minutes=1),
    "h": timedelta(hours=1),
    "d": timedelta(days=1),
    "w": timedelta(weeks=1),
}

DIR_PATH = path.dirname(__file__)
TABLE_FILE = path.join(DIR_PATH, "tables.yaml")


class Table(NamedTuple):
    name: str
    columns: tuple
    types: tuple
    constraints: dict = None
    update_freq: str = None


def scheduled_update():
    """Starts the scheduled auto update of the cached hive leaderboards
    """
    database = Postgres()

    with open(TABLE_FILE, "r") as file:
        tables = yaml.safe_load(file)

    global LAST_UPDATED
    LAST_UPDATED = Table(**tables["last_updated"])

    for table in tables.values():
        setup_table(database, Table(**table))

    while True:
        schedule.run_pending()
        time.sleep(1)


def setup_table(database: Postgres, table: Table):
    """Takes a table object and runs all the required steps to create it and setup
    scheduled updates on the specified database

    Args:
        database (Postgres): interface to interact with the database
        table (Table): defines the structure of the table to upload data to
    """
    database.create_table(table.name, table.columns, table.types, raise_error=False)

    if table.constraints:
        for column, constraint in table.constraints.items():
            database.add_constraint(table.name, column, constraint, raise_error=False)

    if table.update_freq:
        check_outdated(database, table)
        schedule.every().minute.do(check_outdated, database, table)


def check_outdated(database, data_table):
    """Checks if a table is outdated based on it's update frequency and updates it

    Args:
        database (Postgres): interface to interact with the database
        data_table (Table): the table that needs to be checked
    """
    now = datetime.utcnow()
    period = data_table.update_freq
    length, unit = int(period[:-1]), period[-1:]

    database.cursor.execute(
        """
            select %(update_col)s from %(update_table)s
                where %(name_col)s = %(target)s
        """,
        {
            "update_table": AsIs(LAST_UPDATED.name),
            "name_col": AsIs(LAST_UPDATED.columns[0]),
            "update_col": AsIs(LAST_UPDATED.columns[1]),
            "target": data_table.name,
        },
    )

    updated = database.cursor.fetchone()

    if updated:
        updated = updated[0]

        if unit == "M":
            if (
                ((now.year - updated.year) * 12) + (now.month - updated.month)
            ) < length:
                return False
        else:
            if (now - updated) < (UNIT_DICT[unit] * length):
                return False

    update_leaderborad(database, data_table)


def update_leaderborad(database, data_table, game="bp"):
    """Retrieve full leaderboard for specified game and upload it to database table

    Args:
        database (Postgres): interface to interact with the database
        data_table (dict): defines the structure of the table to upload data to
        game (str, optional): identifier for game, defaults to bp

    Requires:
        data_table should define the table name, columns and specific column types if
        they are enforced
    """
    data = []

    for start in range(0, LEADERBOARD_LENGTH, API_MAX_CALL_SIZE):
        data += leaderboard(game, start, API_MAX_CALL_SIZE)

    data = tuple(tuple(row.values()) for row in data)

    database.insert(
        data_table.name, data_table.columns, data, conflict_key=data_table.columns[0]
    )

    database.insert(
        LAST_UPDATED.name,
        LAST_UPDATED.columns,
        ((data_table.name, SQL_NOW),),
        conflict_key=LAST_UPDATED.columns[0],
    )


def query_leaderboard(database: Postgres, start, length=1, game="bp"):
    """Returns leaderboard entries for specified game

    Args:
        database (Postgres): interface to interact with the database
        start (int): index for first entry of leaderboard to get
        length (int, optional): number of entries to get from start
                                defaults to 1
        game (str, optional): identifier for game, defaults to bp

    Requires:
        start is within [0, 1000]
        length <= 200

    Returns:
        Tuple(dict): tuple of leaderboard entries
    """
    database.cursor.execute(
        """
            select * from %(game)s_main
                where index >= %(start)s
                limit %(limit)s;
        """,
        {"game": AsIs(game), "start": AsIs(start), "limit": AsIs(length)},
    )

    return database.cursor.fetchall()
