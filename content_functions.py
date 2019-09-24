from functools import reduce
from hive_interface import leaderboard


RANK_DICT = {
    "First Step": 0,
    "Party Animal": 100,
    "Ballerina": 500,
    "Raver": 1000,
    "Freestyler": 2500,
    "Breakdancer": 5000,
    "Star": 10000,
    "MC Hammer": 20000,
    "Carlton": 35000,
    "Destroyer": 50000,
    "Famous": 75000,
    "Dominator": 100000,
    "Fabulous": 150000,
    "King of Dance": 200000,
    "Choreographer": 300000,
    "Happy Feet": 400000,
    "Jackson": 500000,
    "Astaire": 625000,
    "Swayze": 750000,
    "Legendary": 1000000,
}
TOP_RANK = "Billy Elliot"


def get_next_rank(points):
    """Gets the next rank up from the points provided

    Args:
        points (int): total current points

    Returns:
        str: name of next rank
        int: total points required to reach next rank
    """
    if points > max(RANK_DICT.values()):
        top_rank_points = leaderboard("BP", 0)["total_points"]
        return TOP_RANK, top_rank_points - points

    next_rank = reduce(lambda x, y: y if points > RANK_DICT[x] else x, RANK_DICT.keys())
    return next_rank, RANK_DICT[next_rank] - points
