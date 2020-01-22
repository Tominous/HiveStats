import os
import discord
from datetime import datetime
from functools import partial
from asyncio import TimeoutError
from multiprocessing import Process

from discord.ext.commands import Bot
from mojang_api import get_uuid, is_valid_uuid, get_username_history

from hivestats import hive_api as hive
from hivestats.content_functions import get_next_rank
from hivestats.database import Postgres
import hivestats.database.leaderboard as db_lb


BOT_PREFIX = os.environ["BOT_PREFIX"]
TOKEN = os.environ["DISCORD_TOKEN"]


BATCH_SIZE = 20  # Number of rows returned for commands that return a batch
LEADERBOARD_LENGTH = 1000  # Number of players on the Hive leaderboard

REACTION_TIMEOUT = 600  # Timeout for reaction based interfaces
REACTION_POLLING_FREQ = 60  # Frequency at which reaction checks auto-timeout


client = Bot(command_prefix=BOT_PREFIX, case_insensitive=True)
database = Postgres()


def run_bot():
    """Packaged function for multiprocessing, starts bot
    """
    client.run(TOKEN)


def update_leaderboard():
    """Packaged function for multiprocessing, starts leaderboard caching
    """
    db_lb.scheduled_update()


@client.event
async def on_ready():
    print("Logged in as {}: {}".format(client.user.name, client.user.id))
    await client.change_presence(activity=discord.Game(name="The Hive"))


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
    return "https://visage.surgeplay.com/face/{}/{}".format(size, uuid)


def resolve_username(username):
    """Resolves a username to a uuid if valid

    Args:
        username (str or None): username to be resolved

    Returns:
        bool: whether the username was valid
        str: the resolved uuid or error message
    """
    if not username:
        return False, "Please provide a username."

    if not is_valid_uuid(username):
        try:
            username = get_uuid(username).id
        except AttributeError:
            return False, "Username or UUID was not found."

    return True, username.replace("-", "")


def uuid_to_username(uuid):
    """Resolves a uuid to current username if valid

    Args:
        uuid (str): uuid to be resolved

    Returns:
        str or None: current username or None if uuid is invalid
    """
    uuid = uuid.replace("-", "")

    if is_valid_uuid(uuid):
        return format_username(get_username_history(uuid)[-1].name)

    return None


def format_username(username):
    """Reformats usernames to avoid chars in name causing unwanted discord formatting

    Args:
        username (str): username to be reformatted

    Returns:
        str: name with certain characters replaced with appropriate escape sequences
        """
    return username.replace("_", "\\_")


def format_interval(seconds, granularity=2):
    intervals = (
        ("years", 31536000),
        ("weeks", 604800),
        ("days", 86400),
        ("hours", 3600),
        ("minutes", 60),
        ("seconds", 1),
    )
    result = []

    for name, length in intervals:
        value = seconds // length

        if value:
            seconds %= length

            if value == 1:
                name = name[:-1]  # Remove 's' from the end

            result.append("{} {}".format(value, name))

    return result[:granularity]


def embed_header(data, head_size=64):
    """Creates an embed with the primary fields filled in as required

    Args:
        data (dict): json data for player returned from hive api
        head_size (int, optional): width and heightin pixels of thumbnail image
                                   defaults to 64

    Returns:
        discord.Embed: an embed object formatted as required
    """
    current_time = int(datetime.timestamp(datetime.now()))

    if data["lastLogout"] < data["lastLogin"]:
        time_diff = current_time - data["lastLogin"]
        description = "{} {}\nOnline for {}".format(
            data["status"]["description"],
            data["status"]["game"],
            format_interval(time_diff, 1)[0],
        )
        color = 0x00FF00
    else:
        time_diff = current_time - data["lastLogout"]
        description = "Last seen {} ago".format(", ".join(format_interval(time_diff)))
        color = 0x222222

    embed = discord.Embed(
        title="**{}** - {}".format(
            uuid_to_username(data["UUID"]), data["modernRank"]["human"]
        ),
        description=description,
        color=color,
    )

    embed.set_thumbnail(url=player_head(data["UUID"], head_size))

    return embed


@client.command(name="seen")
async def seen(ctx, username):
    valid, resolved = resolve_username(username)

    if not valid:
        await ctx.send(resolved)
        return

    uuid = resolved
    data = hive.player_data(uuid)

    if not data:
        await ctx.send("This player has never played on The Hive.")
        return

    embed = embed_header(data, 48)
    await ctx.send(embed=embed)


@client.command(name="stats", aliases=["records", "stat"])
async def get_stats(ctx, uuid=None, period="all", game="BP"):
    valid, resolved = resolve_username(uuid)

    if not valid:
        await ctx.send(resolved)
        return

    uuid = resolved
    data = hive.player_data(uuid)
    stats = hive.player_data(uuid, game)

    if not data:
        await ctx.send("This player has never played on The Hive.")
        return

    if not stats:
        await ctx.send("This player has never played BlockParty.")
        return

    reactions = {
        "\U0001F1E9": "daily",
        "\U0001F1FC": "weekly",
        "\U0001F1F2": "monthly",
        "\U0001F1E6": "all",
    }

    def create_stats_embed(data, stats, uuid, game, period):
        game = game.upper()
        period = period.lower()

        embed = embed_header(data)
        cached_stats = db_lb.query_stats(database, uuid, game, period)
        if period != "all" and not cached_stats:
            embed.add_field(
                name="BlockParty Stats",
                value="This player does not have any stats available for this period.",
            )
            return embed
        elif not cached_stats:
            if stats["games_played"]:
                win_rate = stats["victories"] / stats["games_played"]
                placing_rate = stats["total_placing"] / stats["games_played"]
                points_per_game = stats["total_points"] / stats["games_played"]
            else:
                win_rate, placing_rate, points_per_game = 0, 0, 0

            cached_stats = {
                "win_rate": win_rate,
                "placing_rate": placing_rate,
                "points_per_game": points_per_game,
            }

        stats.update(cached_stats)

        if stats["games_played"] == 0:
            win_loss = "Undefined"
        elif stats["win_rate"] == 1:
            win_loss = "Infinity"
        else:
            win_loss = "{:.2f}".format(stats["win_rate"] / (1 - stats["win_rate"]))

        next_rank, diff = get_next_rank(stats["total_points"])
        next_rank_text = (
            f"**Next Rank:** {next_rank} ({diff} points away)"
            if period == "all"
            else ""
        )
        leaderboard_text = (
            f"**Leaderboard #**: {stats['position']}"
            if "position" in cached_stats
            else ""
        )

        if period != "all":
            embed_title = f"BlockParty {period.capitalize()} Stats"
        else:
            embed_title = "BlockParty Stats"
        embed.add_field(
            name=embed_title,
            value=(
                f"**Rank:** {stats['title']}\n"
                f"**Points:** {stats['total_points']}\n"
                f"**Games Played:** {stats['games_played']}\n"
                f"**Wins:** {stats['victories']}\n"
                f"**Placings:** {stats['total_placing']}\n"
                f"**Eliminations:** {stats['total_eliminations']}\n"
            ),
        )
        embed.add_field(
            name="\u200b",
            value=(
                f"{next_rank_text}\n"
                f"{leaderboard_text}\n"
                f"**W/L Ratio:** {win_loss}\n"
                f"**Win Rate:** {stats['win_rate']:.2%}\n"
                f"**Placing Rate:** {stats['placing_rate']:.2%}\n"
                f"**Points per Game**: {stats['points_per_game']:.2f}\n"
            ),
        )
        embed.set_footer(text="D: Daily, W: Weekly, M: Monthly, A: All-time")

        return embed

    msg = await ctx.send(embed=create_stats_embed(data, stats, uuid, game, period))

    if str(ctx.channel.type) != "text":
        await ctx.send("Warning: The emojis are not auto removed in DMs.")

    for reaction in reactions:
        await msg.add_reaction(reaction)

    async def stats_reaction_check():
        try:
            payload = await client.wait_for(
                "raw_reaction_add", timeout=REACTION_POLLING_FREQ
            )
        except TimeoutError:
            pass
        else:
            emoji = str(payload.emoji.name)

            if (
                payload.message_id == msg.id
                and payload.user_id == ctx.author.id
                and emoji in reactions
            ):
                stats_type = reactions[emoji]
                await msg.edit(
                    embed=create_stats_embed(data, stats, uuid, game, stats_type)
                )

                if str(ctx.channel.type) == "text":
                    await msg.remove_reaction(emoji, ctx.author)

    while (datetime.utcnow() - msg.created_at).total_seconds() < REACTION_TIMEOUT:
        await stats_reaction_check()

    await msg.clear_reactions()


@client.command(name="names", aliases=["history", "namemc"])
async def get_names(ctx, uuid=None, count: int = None):
    if count and count <= 0:
        await ctx.send("Please input a number larger than 0.")
        return

    valid, resolved = resolve_username(uuid)

    if not valid:
        await ctx.send(resolved)
        return

    uuid = resolved
    response = get_username_history(uuid)
    count = len(response) if count is None else count

    names = [format_username(entry.name) for entry in response[::-1]]
    # Java timestamps are returned which are in millisecs, so we divide by 1000
    times = [
        datetime.fromtimestamp(entry.changedToAt / 1000).strftime("%d %b, %Y %H:%M")
        for entry in response[:0:-1]
    ]
    times.append("(Original Name)")

    # Trim down lists to requested size
    names, times = names[:count], times[:count]

    embed = discord.Embed(
        title="**{}'s Name History**".format(names[0]), color=0xFFA500
    )

    embed.set_thumbnail(url=player_head(uuid, 64))

    batch_size = 20

    for i in range(0, len(names), batch_size):
        embed.add_field(
            name="\u200b",
            value="\n".join(
                [
                    "**{}** - {}".format(name, time)
                    for name, time in zip(
                        names[i : i + batch_size], times[i : i + batch_size]
                    )
                ]
            ),
            inline=False,
        )

    await ctx.send(embed=embed)


@client.command(name="compare")
async def compare(ctx, uuid_a=None, uuid_b=None, game="BP"):
    resolved_uuids = []

    for i, uuid in enumerate([uuid_a, uuid_b]):
        valid, resolved = resolve_username(uuid)

        if not valid:
            await ctx.send(resolved)
            return

        resolved_uuids.append(resolved)

    get_stats = partial(hive.player_data, game=game)
    stats = [get_stats(resolved_uuids[0]), get_stats(resolved_uuids[1])]

    for uuid, stat in zip(resolved_uuids, stats):
        if not stat:
            await ctx.send(
                "**{}** has never played BlockParty.".format(uuid_to_username(uuid))
            )
            return

        stat["username"] = uuid_to_username(uuid)
        stat["win_rate"] = stat["victories"] / stat["games_played"]
        stat["placing_rate"] = stat["total_placing"] / stat["games_played"]
        stat["points_per_game"] = stat["total_points"] / stat["games_played"]

    embed = discord.Embed(
        title="{} and {} Stats Comparison".format(
            stats[0]["username"], stats[1]["username"]
        ),
        description="",
    )

    embed.add_field(
        name="\u200b",
        value=(
            f"**Points:**\n"
            f"**Games:**\n"
            f"**Wins:**\n"
            f"**Placings:**\n"
            f"**Eliminations:**\n"
            f"\n"
            f"**Win Rate:**\n"
            f"**Placings Rate:**\n"
            f"**Points per Game:**\n"
        ),
    )

    for i, stat in enumerate(stats):
        other = stats[not i]

        abs_diff = []
        perc_diff = []
        abs_diff_dec = []

        for field in [
            "total_points",
            "games_played",
            "victories",
            "total_placing",
            "total_eliminations",
        ]:
            abs_diff.append(f"{stat[field]:,} ({stat[field] - other[field]:+,})\n")

        for field in ["win_rate", "placing_rate"]:
            perc_diff.append(
                f"{stat[field]:.2%} ({(stat[field] - other[field]) / (sum([stat[field], other[field]]) / 2):+.2%})\n"
            )

        for field in ["points_per_game"]:
            abs_diff_dec.append(
                f"{stat[field]:.2f} ({stat[field] - other[field]:+.2f})\n"
            )

        embed.add_field(
            name=stat["username"],
            value=(
                f"{''.join(abs_diff)}"
                f"\n"
                f"{''.join(perc_diff)}"
                f"{''.join(abs_diff_dec)}"
            ),
        )

    await ctx.send(embed=embed)


@client.command(name="leaderboard", aliases=["leaderboards", "lb"])
async def leaderboard(ctx, period="all", column="points", page=1, game="BP"):
    columns_dict = {
        "wins": "victories",
        "points": "total_points",
        "elims": "total_eliminations",
        "placings": "total_placing",
        "played": "games_played",
        "win%": "win_rate",
        "placing%": "placing_rate",
        "ppg": "points_per_game",
    }
    valid_periods = ["all", "monthly", "weekly", "daily"]
    usage = "**Invalid Parameters: ** Expected /lb [period] [column] [page]\n"

    if period not in valid_periods:
        await ctx.send(
            "{}Please use one of the following periods: ```{}```".format(
                usage, ", ".join(valid_periods)
            )
        )
        return

    if column not in columns_dict:
        await ctx.send(
            "{}Please use one of the following columns: ```{}```".format(
                usage, ", ".join(columns_dict.keys())
            )
        )
        return

    if not isinstance(page, int) or page < 1 or page > 50:
        await ctx.send("Please input a page number between 1-50.")
        return

    column = columns_dict[column]
    page -= 1
    reactions = {
        "\u23EA": -10,  # rewind
        "\u25C0": -1,  # left_arrow
        "\u25B6": 1,  # right_arrow
        "\u23E9": 10,  # fast_forward
    }

    def create_lb_embed(page, game, period):
        game = game.upper()
        period = period.lower()

        data = db_lb.query_leaderboard(
            database, BATCH_SIZE * page, BATCH_SIZE, sort_by=column, period=period
        )

        if column in [
            "victories",
            "total_points",
            "total_eliminations",
            "total_placing",
            "games_played",
        ]:
            format_string = ","
        elif column in ["win_rate", "placing_rate"]:
            format_string = ".2%"
        elif column in ["points_per_game"]:
            format_string = ".2f"
        else:
            format_string = ""

        if period != "all":
            embed_title = f"BlockParty {period.capitalize()} Leaderboard"
        else:
            embed_title = "BlockParty Leaderboard"

        embed = discord.Embed(title=embed_title, color=0xFFA500)
        embed.set_author(name=ctx.author, icon_url=ctx.author.avatar_url)
        embed.set_footer(text=f"Current page: {page + 1}")
        embed.add_field(
            name="#    Player",
            value="\n".join(
                [
                    f"{entry['row_num']}) **{format_username(entry['username'])}**"
                    for entry in data
                ]
            ),
        )

        embed.add_field(
            name=column.replace("_", " ").capitalize(),
            value="\n".join([f"{entry[column]:{format_string}}" for entry in data]),
        )
        return embed, True

    result, success = create_lb_embed(page, game, period)

    if not success:
        await ctx.send(result)
        return

    msg = await ctx.send(embed=result)

    for reaction in reactions:
        await msg.add_reaction(reaction)

    if str(msg.channel.type) != "text":
        await ctx.send("Warning: The emojis are not auto removed in DMs.")

    async def run_checks(page):
        try:
            payload = await client.wait_for(
                "raw_reaction_add", timeout=REACTION_POLLING_FREQ
            )
        except TimeoutError:
            pass
        else:
            emoji = str(payload.emoji.name)

            if (
                payload.message_id == msg.id
                and payload.user_id == ctx.author.id
                and emoji in reactions
            ):
                if str(msg.channel.type) == "text":
                    await msg.remove_reaction(emoji, ctx.author)

                page += reactions[emoji]
                page %= int(LEADERBOARD_LENGTH / BATCH_SIZE)

                result, _ = create_lb_embed(page, game, period)
                await msg.edit(embed=result)

        return page

    while (datetime.utcnow() - msg.created_at).total_seconds() < REACTION_TIMEOUT:
        page = await run_checks(page)

    await msg.clear_reactions()


if __name__ == "__main__":
    Process(target=run_bot).start()
    Process(target=update_leaderboard).start()
