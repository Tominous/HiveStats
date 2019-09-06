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

@client.command(name="stats")
async def on_message(ctx, username, game):
    hiveUrl = 'http://api.hivemc.com/v1/player/{}/{}'.format(username,game)
    req = Request(hiveUrl, headers={'User-Agent': 'Mozilla/5.0'})
    webpage = urlopen(req).read()
    decodeJson = json.loads(webpage)
    finalString = decodeJson["title"]
    await ctx.send(finalString)
client.run(TOKEN)
