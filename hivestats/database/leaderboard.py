import schedule
import time
from datetime import datetime, timedelta
from typing import NamedTuple

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


class Table(NamedTuple):
    table: str
    columns: tuple
    types: tuple
    update_freq: str = None


BP = Table(
    table="bp",
    columns=(
        "index",
        "human_index",
        "uuid",
        "wins",
        "points",
        "elims",
        "placings",
        "played",
        "username",
    ),
    types=(
        "int",
        "int",
        "varchar(32)",
        "int",
        "int",
        "int",
        "int",
        "int",
        "varchar(200)",
    ),
    update_freq="5m",
)

BP_MONTHLY = Table(
    table="bp_monthly", columns=BP.columns, types=BP.types, update_freq="1M"
)

LAST_UPDATED = Table(
    table="last_updated",
    columns=("name", "updated"),
    types=("varchar(200)", "timestamp"),
)


def scheduled_update():
    """Starts the scheduled auto update of the cached hive leaderboards
    """
    database = Postgres()

    database.create_table(
        LAST_UPDATED.table, LAST_UPDATED.columns, LAST_UPDATED.types, raise_error=False
    )
    database.add_constraint(
        LAST_UPDATED.table, LAST_UPDATED.columns[0], "unique", raise_error=False
    )

    database.create_table(BP.table, BP.columns, BP.types, raise_error=False)
    database.add_constraint(BP.table, BP.columns[0], "unique", raise_error=False)
    check_outdated(database, BP)

    database.create_table(
        BP_MONTHLY.table, BP_MONTHLY.columns, BP_MONTHLY.types, raise_error=False
    )
    database.add_constraint(
        BP_MONTHLY.table, BP_MONTHLY.columns[0], "unique", raise_error=False
    )
    check_outdated(database, BP_MONTHLY)

    schedule.every().minute.do(check_outdated, database, BP)
    schedule.every().minute.do(check_outdated, database, BP_MONTHLY)

    while True:
        schedule.run_pending()
        time.sleep(1)


def check_outdated(database, data_table):
    """Checks if a table is outdated based on it's update frequency and updates it

    Args:
        database (Postgres): interface to interact with the database
        data_table (dict): defines the structure of the table to upload data to
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
            "update_table": AsIs(LAST_UPDATED.table),
            "name_col": AsIs(LAST_UPDATED.columns[0]),
            "update_col": AsIs(LAST_UPDATED.columns[1]),
            "target": data_table.table,
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
        game (str): identifier for game

    Requires:
        data_table should define the table name, columns and specific column types if
        they are enforced
    """
    data = []

    for start in range(0, LEADERBOARD_LENGTH, API_MAX_CALL_SIZE):
        data += leaderboard(game, start, API_MAX_CALL_SIZE)

    data = tuple(tuple(row.values()) for row in data)

    database.insert(
        data_table.table, data_table.columns, data, conflict_key=data_table.columns[0]
    )

    database.insert(
        LAST_UPDATED.table,
        LAST_UPDATED.columns,
        ((data_table.table, SQL_NOW),),
        conflict_key=LAST_UPDATED.columns[0],
    )


def query_leaderboard(database: Postgres, game, start, length=1):
    """Returns leaderboard entries for specified game

    Args:
        database (Postgres): interface to interact with the database
        game (str): identifier for game
        start (int): index for first entry of leaderboard to get
        length (int, optional): number of entries to get from start
                                defaults to 1

    Requires:
        start is within [0, 1000]
        length <= 200

    Returns:
        Tuple(dict): tuple of leaderboard entries
    """
    database.cursor.execute(
        """
            select * from %(game)s
                where index >= %(start)s
                limit %(limit)s;
        """,
        {"game": AsIs(game), "start": AsIs(start), "limit": AsIs(length)},
    )

    return database.cursor.fetchall()
