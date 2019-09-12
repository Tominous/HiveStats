import os
import discord
from datetime import datetime

from discord.ext.commands import Bot
from mojang_api import get_uuid, is_valid_uuid, get_username_history

import hive_interface as hive
from content_functions import get_next_rank


BOT_PREFIX = '/'
TOKEN = os.environ['discordToken']


client = Bot(command_prefix=BOT_PREFIX, case_insensitive=True)


@client.event
async def on_ready():
    print('Logged in as {}: {}'.format(client.user.name, client.user.id))
    await client.change_presence(activity=discord.Game(name='The Hive'))


def player_head(uuid, size):
    """Returns link to thumbnail of player head

    Args:
        uuid (str): id fo player to retrieve data for
        size (int): widht and height in pixels of image

    Note:
        api only supports sizes that are multiples of 16, rounding
        will be done automatically

    Returns:
        str: url for the thumbnail image
    """
    return'https://visage.surgeplay.com/head/{}/{}'.format(size, uuid)


def resolve_username(username):
    """Resolves a username to a uuid if valid

    Args:
        username (str or None): username to be resolved

    Returns:
        bool: whether the username was valid
        str: the resolved uuid or error message
    """
    if not username:
        return False, 'Please provide a username.'

    if not is_valid_uuid(username):
        try:
            username = get_uuid(username).id
        except AttributeError:
            return False, 'Username or UUID was not found.'

    return True, username.replace('-', '')


def format_interval(seconds, granularity=2):
    intervals = (('years', 31536000), ('weeks', 604800), ('days', 86400),
                 ('hours', 3600), ('minutes', 60), ('seconds', 1))
    result = []

    for name, length in intervals:
        value = seconds // length

        if value:
            seconds %= length

            if value == 1:
                name = name[:-1]  # Remove 's' from the end

            result.append("{} {}".format(value, name))

    return result[:granularity]


def embed_header(uuid, head_size=96):
    """Creates an embed with the primary fields filled in as required

    Args:
        uuid (str): id of player to retrieve data for
        head_size (int, optional): width and heightin pixels of thumbnail image
                                   defaults to 96

    Returns:
        discord.Embed: an embed object formatted as required
    """
    info = hive.player_data(uuid)
    current_time = int(datetime.timestamp(datetime.now()))

    if info['lastLogout'] < info['lastLogin']:
        time_diff = current_time - info['lastLogin']
        description = '{} {}\nOnline for {}'.format(info['status']['description'],
                                                    info['status']['game'],
                                                    format_interval(time_diff, 1)[0])
        color = 0x00ff00
    else:
        time_diff = current_time - info['lastLogout']
        description = 'Last seen {} ago'.format(
            ', '.join(format_interval(time_diff)))
        color = 0x222222

    embed = discord.Embed(
        title='**{}** - {}'.format(info['username'],
                                   info['modernRank']['human']),
        description=description,
        color=color)

    embed.set_thumbnail(url=player_head(uuid, head_size))

    return embed


@client.command(name='seen')
async def seen(ctx, username):
    valid, resolved = resolve_username(username)

    if not valid:
        await ctx.send(resolved)

    uuid = resolved

    embed = embed_header(uuid, 64)
    await ctx.send(embed=embed)


@client.command(name='stats', aliases=['records', 'stat'])
async def get_stats(ctx, uuid=None, game='BP'):
    valid, resolved = resolve_username(uuid)

    if not valid:
        await ctx.send(resolved)

    uuid = resolved
    stats = hive.player_data(uuid, game)

    embed = embed_header(uuid)

    next_rank, diff = get_next_rank(stats['total_points'])
    content = f'''
**Rank:** {stats['title']} ({diff} points to {next_rank})
**Points:** {stats['total_points']}
**Games Played:** {stats['games_played']}
**Wins:** {stats['victories']}
**Placings:** {stats['total_placing']}
**Eliminations:** {stats['total_eliminations']}

**W/L Ratio:** {stats['victories'] / (stats['games_played'] - stats['victories']):.2f}
**Win Rate:** {stats['victories'] / stats['games_played']:.2%}'''

    if game == 'BP':
        embed.add_field(name='BlockParty Stats', value=content)

    await ctx.send(embed=embed)


@client.command(name='names', aliases=['history', 'namemc'])
async def get_names(ctx, uuid=None, count: int = None):
    if count and count <= 0:
        await ctx.send('Please input a number larger than 0.')

    valid, resolved = resolve_username(uuid)

    if not valid:
        await ctx.send(resolved)

    uuid = resolved
    response = get_username_history(uuid)
    count = len(response) if count is None else count

    names = [entry.name for entry in response[::-1]]
    # Java timestamps are returned which are in millisecs, so we divide by 1000
    times = [datetime.fromtimestamp(entry.changedToAt / 1000).strftime('%d %b, %Y %H:%M')
             for entry in response[:0:-1]]
    times.append('(Original Name)')

    # Trim down lists to requested size
    names, times = names[:count], times[:count]

    embed = discord.Embed(
        title='**{}\'s Name History**'.format(names[0]),
        color=0xffa500
    )

    embed.set_thumbnail(url=player_head(uuid, 96))

    batch_size = 20

    for i in range(0, len(names), batch_size):
        embed.add_field(name='\u200b',
                        value='\n'.join(['**{}** - {}'.format(name, time)
                                         for name, time in zip(names[i:i + batch_size],
                                                               times[i:i + batch_size])]),
                        inline=False)

    await ctx.send(embed=embed)


client.run(TOKEN)
