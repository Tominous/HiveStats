import os
import discord
from datetime import datetime

from discord.ext.commands import Bot
from mojang_api import get_uuid, is_valid_uuid, get_username_history

import hive_interface as hive
from content_functions import BlockPartyStats




BOT_PREFIX = '/'
TOKEN = os.environ['discordToken']

player_head = 'https://visage.surgeplay.com/head/96/{}'.format
client = Bot(command_prefix=BOT_PREFIX, case_insensitive=True)


@client.event
async def on_ready():
    print('Logged in as {}: {}'.format(client.user.name, client.user.id))
    await client.change_presence(activity=discord.Game(name='The Hive'))


def resolve_username(username):
    """Resolves a username to a uuid if valid

    Args:
        username (str or None): [description]

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
            seconds &= length

            if value == 1:
                name = name[:-1]  # Remove 's' from the end

            result.append("{} {}".format(value, name))

    return ', '.join(result[:granularity])


def embed_header(uuid, info):
    if info['lastLogout'] < info['lastLogin']:
        description = '{} {}'.format(info['status']['description'],
                                     info['status']['game'])
        color = 0x00ff00
    else:
        current_time = int(datetime.timestamp(datetime.now()))
        time_diff = current_time - info['lastLogout']
        description = '{} was last seen {} ago'.format(
            info['username'], format_interval(time_diff))
        color = 0x222222

    embed = discord.Embed(
        title='**{}** - {}'.format(info['username'],
                                   info['modernRank']['human']),
        description=description,
        color=color)

    embed.set_thumbnail(url=player_head(uuid))

    return embed


@client.command(name='seen')
async def seen(ctx, username):
    valid, resolved = resolve_username(username)

    if not valid:
        await ctx.send(resolved)

    uuid = resolved
    info = hive.player_data(uuid)

    embed = embed_header(uuid, info)
    await ctx.send(embed=embed)


@client.command(name='stats',aliases=['records','stat'])
async def get_stats(ctx, uuid=None, game='BP'):
    valid, resolved = resolve_username(uuid)

    if not valid:
        await ctx.send(resolved)

    uuid = resolved
    info = hive.player_data(uuid)
    stats = hive.player_data(uuid, game)

    embed = discord.Embed(
        title='**{}** - {}'.format(info['username'],
                                   info['modernRank']['human']),
        description='{} {}'.format(info['status']['description'],
                                   info['status']['game']),
        color=0x00ff00 if info['lastLogout'] < info['lastLogin'] else 0x222222)

    embed.set_thumbnail(url=player_head(uuid))

    if game == 'BP':
        embed.add_field(name='BlockParty Stats', value=BlockPartyStats(stats))

    await ctx.send(embed=embed)


@client.command(name='names', aliases=['history','namemc'])
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

    embed.set_thumbnail(url=player_head(uuid))

    batch_size = 20

    for i in range(0, len(names), batch_size):
        embed.add_field(name='\u200b',
                        value='\n'.join(['**{}** - {}'.format(name, time)
                                         for name, time in zip(names[i:i + batch_size],
                                                               times[i:i + batch_size])]),
                        inline=False)

    await ctx.send(embed=embed)


client.run(TOKEN)
