import os
import discord

from discord.ext.commands import Bot
from mojang_api import get_uuid, is_valid_uuid
import hive_interface as hive


BOT_PREFIX = '/'
TOKEN = os.environ['discordToken']

client = Bot(command_prefix=BOT_PREFIX)


@client.event
async def on_ready():
    print('Logged in as {}: {}'.format(client.user.name, client.user.id))
    await client.change_presence(activity=discord.Game(name='The Hive'))


@client.command(name="stats")
async def get_stats(ctx, uuid=None, game='BP'):
    """WIP Command
        TODO: - Include detailed stats
              - Output as embed
              - Error handling
    """
    if not is_valid_uuid(uuid):
        uuid = get_uuid(uuid).id

    await ctx.send(hive.player_data(uuid, game)['title'])


client.run(TOKEN)
