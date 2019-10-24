import schedule
import time

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
    schedule.every(UPDATE_FREQUENCY).minutes.do(leaderboard_table, BP_LEADERBOARD, "bp")

    while True:
        schedule.run_pending()
        time.sleep(60)


def leaderboard_table(data_table, game):
    """Retrieve full leaderboard for specified game and upload it to database table

    Args:
        data_table (dict): defines the structure of the table to upload data to
        game (str): identifier for game

    Requires:
        data_table should define the table name, columns and specific column types if
        they are enforced
    """
    with Postgres() as db:
        data = []

        for start in range(0, LEADERBOARD_LENGTH, API_MAX_CALL_SIZE):
            data += leaderboard(game, start, API_MAX_CALL_SIZE)

        data = tuple(tuple(row.values()) for row in data)

        db.replace_table(**data_table, values=data)
