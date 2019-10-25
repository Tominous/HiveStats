import schedule
import time

from psycopg2.extensions import AsIs

from ..hive_api import leaderboard
from .sql import Postgres

LEADERBOARD_LENGTH = 1000  # Number of players on the Hive leaderboard
API_MAX_CALL_SIZE = 200  # Max leaderboard entries retrievable per api call

UPDATE_FREQUENCY = 5  # in mins

BP_LEADERBOARD = {
    "table": "bp_leaderboard",
    "columns": (
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
    "types": (
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
}


def scheduled_update():
    """Starts the scheduled auto update of the cached hive leaderboards
    """
    database = Postgres()

    database.create_table(**BP_LEADERBOARD)
    update_leaderborad(database, BP_LEADERBOARD, "bp")

    schedule.every(UPDATE_FREQUENCY).minutes.do(update_leaderborad, database, BP_LEADERBOARD, "bp")

    while True:
        schedule.run_pending()
        time.sleep(60)


def update_leaderborad(database, data_table, game):
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

    database.replace_table(**data_table, values=data)


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
            select * from %(game)s_leaderboard
                where index >= %(start)s
                limit %(limit)s;
        """,
        {"game": AsIs(game), "start": AsIs(start), "limit": AsIs(length)}
    )

    return database.cursor.fetchall()
