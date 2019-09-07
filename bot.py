import os
import discord

from discord.ext.commands import Bot
from mojang_api import get_uuid, is_valid_uuid

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


@client.command(name="stats")
async def get_stats(ctx, uuid=None, game='BP'):
    if not uuid:
        await ctx.send("Please provide a username.")

    if not is_valid_uuid(uuid):
        try:
            uuid = get_uuid(uuid).id
        except AttributeError:
            await ctx.sen("Username or UUID was not found.")

    info = hive.player_data(uuid)
    stats = hive.player_data(uuid, game)
    online = info['lastLogout'] < info['lastLogin']

    embed = discord.Embed(
        title='**{}** - {}'.format(info['username'],
                                   info['modernRank']['human']),
        description='{} {}'.format(info['status']['description'],
                                   info['status']['game']),
        color=0x00ff00 if online else 0x222222)

    embed.set_thumbnail(url=player_head(uuid))

    if game == 'BP':
        embed.add_field(name='BlockParty Stats', value=BlockPartyStats(stats))

    await ctx.send(embed=embed)


client.run(TOKEN)
