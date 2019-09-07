from functools import reduce
from hive_interface import leaderboard


RANK_DICT = {
    'First Step': 0,
    'Party Animal': 100,
    'Ballerina': 500,
    'Raver': 1000,
    'Freestyler': 2500,
    'Breakdancer': 5000,
    'Star': 10000,
    'MC Hammer': 20000,
    'Carlton': 35000,
    'Destroyer': 50000,
    'Famous': 75000,
    'Dominator': 100000,
    'Fabulous': 150000,
    'King of Dance': 200000,
    'Choreographer': 300000,
    'Happy Feet': 400000,
    'Jackson': 500000,
    'Astaire': 625000,
    'Swayze': 750000,
    'Legendary': 1000000
}
TOP_RANK = 'Billy Elliot'


def NextRank(points):
    """Gets the next rank up from the points provided

    Args:
        points (int): total current points

    Returns:
        str: name of next rank
        int: total points required to reach next rank
    """
    if points > max(RANK_DICT.values()):
        top_rank_points = leaderboard('BP', 0)['total_points']
        return TOP_RANK, top_rank_points - points

    next_rank = reduce(lambda x, y: y if points > RANK_DICT[x] else x,
                       RANK_DICT.keys())
    return next_rank, RANK_DICT[next_rank] - points


def BlockPartyStats(stats):
    """Formats an embed field using the provided stats

    Args:
        stats (dict): valid dictionary of player stats

    Returns:
        str: value to use within embed field
    """
    next_rank, diff = NextRank(stats['total_points'])

    content = f'''
    **Rank:** {stats['title']} ({diff} points to {next_rank})
    **Points:** {stats['total_points']}
    **Games Played:** {stats['games_played']}
    **Wins:** {stats['victories']}
    **Placings:** {stats['total_placing']}
    **Eliminations:** {stats['total_eliminations']}

    **W/L Ratio:** {stats['victories'] / (stats['games_played'] - stats['victories']):.2f}
    **Win Rate:** {stats['victories'] / stats['games_played']:.2%}
    '''

    return content
