import os
import discord
from discord.ext.commands import Bot


BOT_PREFIX = '/'
TOKEN = os.environ['discordToken']

client = Bot(command_prefix=BOT_PREFIX)


@client.event
async def on_ready():
    print('Logged in as')
    print(client.user.name)
    print(client.user.id)
    print('------')
    await client.change_presence(activity=discord.Game(name='The Hive'))


@client.command(name="exCommand")
async def exampleCommand(ctx):
    await ctx.send('This is an example of a Command Funtion')


client.run(TOKEN)
