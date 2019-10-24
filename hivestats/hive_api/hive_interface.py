import requests


base = "http://api.hivemc.com/v1/{}".format


def player_data(uuid, game=""):
    """Returns data of specified player

    Args:
        uuid (str): id of player to retrieve data for
        game (str, optional): if provided, returns player stats for specified
            game else returns general Hive info on player

    Returns:
        dict or bool: serialized data for player or False if request failed
    """
    r = requests.get(base("player/{}/{}".format(uuid, game)))

    return r.json() if r.ok else False


def leaderboard(game, start, length=1):
    """Returns leaderboard entries for specified game

    Args:
        game (str): identifier for game
        start (int): index for first entry of leaderboard to get
        length (int, optional): number of entries to get from start
                                defaults to 1

    Requires:
        start is within [0, 1000]
        length <= 200

    Returns:
        list(dict) or dict: list of leaderboard entries
    """
    end = min(1000, start + length)
    r = requests.get(base("game/{}/leaderboard/{}/{}".format(game, start, end)))

    if length == 1:
        return r.json()["leaderboard"][0]

    return r.json()["leaderboard"]
