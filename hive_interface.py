import requests


base = 'http://api.hivemc.com/v1/{}'.format


def player_data(uuid, game=''):
    """Returns data of specified player

    Args:
        uuid (str): id of player to retrieve data for
        game (str, optional): if provided, returns player stats for specified
            game else returns general Hive info on player

    Returns:
        dict: serialized data for player
    """
    r = requests.get(base('player/{}/{}'.format(uuid, game)))

    return r.json()
