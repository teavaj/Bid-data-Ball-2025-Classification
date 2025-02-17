"""Gether tables from NFL Big Data Bowl 2025 and other sources"""
import polars as pl

pl.Config.set_streaming_chunk_size(10000)
#pl.Config.set_tbl_rows(-1)


"""Official NFL tables"""

players_lf = pl.scan_csv("data/nfl-big-data-bowl-2025/players.csv", null_values="NA")
games_lf = pl.scan_csv(
    "data/nfl-big-data-bowl-2025/games.csv", null_values="NA", try_parse_dates=True
).with_columns(pl.col("gameDate").str.to_date(format="%m/%d/%Y"))
plays_lf = pl.scan_csv("data/nfl-big-data-bowl-2025/plays.csv", null_values="NA")
player_play_lf = pl.scan_csv(
    "data/nfl-big-data-bowl-2025/player_play.csv", null_values="NA"
)
tracking_week_df = pl.scan_csv(
    "data/nfl-big-data-bowl-2025/tracking_week_1.csv", null_values="NA"
)
# %%
nr_weeks = 9
tracking_week_lf = pl.LazyFrame(
    schema=[
        "gameId",
        "playId",
        "nflId",
        "displayName",
        "frameId",
        "frameType",
        "time",
        "jerseyNumber",
        "club",
        "playDirection",
        "x",
        "y",
        "s",
        "a",
        "dis",
        "o",
        "dir",
        "event",
    ]
)

for week in range(1, nr_weeks + 1):
    # print(f'NFL/nfl-big-data-bowl-2025/tracking_week_{week}.csv')
    temp_lf = pl.scan_csv(
        f"data/nfl-big-data-bowl-2025/tracking_week_{week}.csv",
        # schema_overrides={'nflId': pl.String}
        # use_pyarrow=True,
        null_values="NA",
        #  infer_schema_length=int(1e10)
    )
    tracking_week_lf = pl.concat([tracking_week_lf, temp_lf], how="vertical_relaxed")


"""play-by-play 2022
See pbp_2022.py for more details"""
# %%
from pbp_2022 import savant_df as savant_lf

# %%
savant_lf = savant_lf.select(
    [
        "GameId",
        "SeriesFirstDown",
        "Yards",
        "Formation",
        "PlayType",
        "NextPlay",
        "IsRush",
        "IsPass",
        "IsIncomplete",
        "IsTouchdown",
        "PassType",
        "IsSack",
        "IsChallenge",
        "IsChallengeReversed",
        "Challenger",
        "IsMeasurement",
        "IsInterception",
        "IsFumble",
        "IsPenalty",
        "IsTwoPointConversion",
        "IsTwoPointConversionSuccessful",
        "IsPenaltyAccepted",
        "PenaltyTeam",
        "IsNoPlay",
        "PenaltyType",
        "playId",
    ]
)

"""player_targets"""
# %%
from player_targets import targets_lf as player_targets_lf

# %% md
""" EPA
EPA = Expected Points After Play - Expected Points Before Play"""
# %%
plays_epa_lf = plays_lf.with_columns(
    (pl.col("expectedPoints") - pl.col("expectedPointsAdded")).alias("epa")
)

"""Adjusted Net Yards per Attempt (ANY/A)"""
# %%
from anya import anya_lf

anya_lf = anya_lf.select(
    (pl.selectors.contains("ANYA")), pl.col("GameId"), pl.col("playId")
)
"""Closed Stadiums
Thanks for perplexity for the data"""
# %%
stadium_data = [
    {"Team": "ARI", "Closed": 1},
    {"Team": "ATL", "Closed": 1},
    {"Team": "BAL", "Closed": 0},
    {"Team": "BUF", "Closed": 0},
    {"Team": "CAR", "Closed": 0},
    {"Team": "CHI", "Closed": 0},
    {"Team": "CIN", "Closed": 0},
    {"Team": "CLE", "Closed": 0},
    {"Team": "DAL", "Closed": 1},
    {"Team": "DEN", "Closed": 0},
    {"Team": "DET", "Closed": 1},
    {"Team": "GB", "Closed": 0},
    {"Team": "HOU", "Closed": 1},
    {"Team": "IND", "Closed": 1},
    {"Team": "JAX", "Closed": 0},
    {"Team": "KC", "Closed": 0},
    {"Team": "LAC", "Closed": 1},
    {"Team": "LA", "Closed": 1},
    {"Team": "LV", "Closed": 1},
    {"Team": "MIA", "Closed": 0},
    {"Team": "MIN", "Closed": 1},
    {"Team": "NE", "Closed": 0},
    {"Team": "NO", "Closed": 1},
    {"Team": "NYG", "Closed": 0},
    {"Team": "NYJ", "Closed": 0},
    {"Team": "PHI", "Closed": 0},
    {"Team": "PIT", "Closed": 0},
    {"Team": "SEA", "Closed": 0},
    {"Team": "SF", "Closed": 0},
    {"Team": "TB", "Closed": 0},
    {"Team": "TEN", "Closed": 0},
    {"Team": "WAS", "Closed": 0},
]

stadiums_df = pl.LazyFrame(stadium_data)

"""Weather Data
See weather_data.py for more details"""
# %%
# from weather_data import weather_df
# weather_lf=pl.LazyFrame(weather_df)
# weather_lf.head().collect()
# weather_lf.collect().write_csv('data/weather_data.csv')
weather_lf = pl.scan_csv("data/weather_data.csv").with_columns(
    pl.col("gameDate").str.to_date(format="%Y-%m-%d")
)

# %% md
# # The Big JOIN
# ## join chart
# %%
import base64
from IPython.display import Image, display


def mm(graph):
    """Flowchart for the JOIN statements"""
    graphbytes = graph.encode("ascii")
    base64_bytes = base64.b64encode(graphbytes)
    base64_string = base64_bytes.decode("ascii")
    url = "https://mermaid.ink/img/" + base64_string

    display(Image(url=url, width=800))


mm(
    """
graph LR

  
  subgraph Games
  weather(Weather Data)
 
  games[games_lf]
  
  stadiums(Closed Stadiums)
  stadiums .-> |Team| games
  weather .-> |date, Team| games
  end
  
  subgraph Plays
  play[plays_lf w/ epa]
  savant(PbP nflsavant.com)
  anya(ANY/A)
  savant .-> |GameId, playId| play
  anya .-> |gameId, playId| play 
  end
  
  subgraph Play-by-Play
  PBP[player_play_lf]  
  PT(Player Targets)  
  Players  
  PT .-> |gameId, playId, nflId| PBP
  Players  .-> |nflId| PBP
  end
  
  subgraph PlayerTracking
  tracking_week_lf
  end
  
  subgraph JOIN
      direction BT
          join(join_lf = tracking_week_lf)
          player_play_lf .-> |gameId, playId, nflId| join
          plays_lf .-> |gameId, playId| join
          games_lf .-> |gameId| join
      
  end
  
  PlayerTracking --> JOIN
  Play-by-Play --> JOIN
    Plays --> JOIN
    Games --> |gameId| Plays
 
"""
)

# %% md
# ## mini-joins
# %%
player_play_lf = player_play_lf.join(players_lf, on="nflId", how="left")
player_play_lf = player_play_lf.join(
    player_targets_lf, on=["gameId", "nflId", "playId"], how="left"
)

# player_play_lf.collect(streaming=True).write_parquet('data/player_play.parquet', compression='zstd', row_group_size=100000)

# (player_play_lf.collect(streaming=True).select(pl.all().exclude("^.*_right$")).write_csv('data/player_play.csv'))
# %%
# games_stadiums_lf=games_lf.join(stadiums_df, left_on='homeTeamAbbr',right_on='Team',  how='left')
games_weather_lf = games_lf.join(
    weather_lf, on=["gameDate", "homeTeamAbbr"], how="left"
)
games_join_lf = games_weather_lf.join(
    stadiums_df, left_on="homeTeamAbbr", right_on="Team", how="left"
)

# print({'join':games_join_lf.collect_schema().names(),"stad":games_stadiums_lf.collect_schema().names(), "weath":games_weather_lf.collect_schema().names()})

# games_join_lf.collect(streaming=True).write_parquet('data/games_join.parquet', compression='zstd', row_group_size=100000)
# (games_join_lf.collect(streaming=True).select(pl.all().exclude("^.*_right$")).write_csv('data/games_join.csv'))
# %%

plays_epa_lf = plays_epa_lf.join(
    savant_lf,
    left_on=[pl.col("gameId"), pl.col("playId")],
    right_on=[pl.col("GameId"), pl.col("playId")],
    how="left",
)
plays_epa_lf = plays_epa_lf.join(
    anya_lf,
    left_on=[pl.col("gameId"), pl.col("playId")],
    right_on=[pl.col("GameId"), pl.col("playId")],
    how="left",
)
plays_epa_lf = plays_epa_lf.join(games_lf, on="gameId", how="left")
# median_anya = plays_epa_lf.select(pl.col('ANYA')).median().collect().item()
# plays_epa_lf = plays_epa_lf.with_columns(pl.col('ANYA').fill_nan(median_anya).fill_null(median_anya))

# plays_epa_lf.collect(streaming=True).write_parquet('data/plays_epa.parquet', compression='zstd', row_group_size=100000)

# (plays_epa_lf.collect(streaming=True).select(pl.all().exclude("^.*_right$")).write_csv('data/plays_epa.csv'))
# %%
(plays_epa_lf.head(200).collect())
# %%
plays_epa_lf.select(pl.col("ANYA")).median().collect()

# %% md
# ## classification data

# %%
#class_lf=player_play_lf.join(plays_epa_lf, on=['gameId', 'playId'], how='left')
class_lf=plays_epa_lf.join(games_join_lf, on='gameId', how='left')
#class_lf.collect().write_parquet('data/classification.parquet', compression='zstd', row_group_size=100000)
# %% md
# ## join statement
# %%
# tracking_week_lf.collect(streaming=True).write_parquet('data/tracking_week.parquet', compression='zstd', row_group_size=100000)
# %%
#join_lf = pl.scan_parquet("data/tracking_week.parquet", low_memory=True)
join_lf=pl.scan_csv('data/nfl-big-data-bowl-2025/tracking_week_1.csv', null_values='NA', low_memory=True)
join_lf=pl.concat([join_lf, pl.scan_csv('data/nfl-big-data-bowl-2025/tracking_week_2.csv', null_values='NA', low_memory=True)], how='vertical_relaxed')
#join_lf=pl.concat([join_lf, pl.scan_csv('data/nfl-big-data-bowl-2025/tracking_week_3.csv', null_values='NA', low_memory=True)], how='vertical_relaxed')
join_lf = join_lf.join(player_play_lf, on=["gameId", "playId", "nflId"], how="left")
join_lf = join_lf.join(plays_epa_lf, on=["gameId", "playId"], how="left")
join_lf = join_lf.join(games_join_lf, on="gameId", how="left")
join_lf = join_lf.select(
    pl.all()
    #                #.exclude(pl.String)
    .shrink_dtype()
)


# %% raw
# (join_lf.select(pl.all()
#                #.exclude(pl.String)
#                .shrink_dtype()).collect(streaming=True).write_parquet('join.parquet', compression='zstd', row_group_size=10000), print('done'))
# %% raw
# ((pl.scan_parquet('data/tracking_week.parquet')).join((pl.scan_parquet('data/player_play.parquet')), on=['gameId', 'playId', 'nflId'], how='left').collect().write_parquet('data=join1.parquet', compression='zstd', row_group_size=10000), print('done'))
# %% raw
# (join_lf.collect(streaming=True).write_parquet('data/join_out.parquet', compression='zstd', row_group_size=10000), print('done'))

# %% raw
# # Set chunk size for streaming
# pl.Config.set_streaming_chunk_size(100000)
#
# # Use streaming collection to write
# (join_lf.select(pl.all().exclude("^.*_right$"))).limit(10000).collect(streaming=True).write_parquet(
# "output_test.parquet",
# compression="zstd",
# row_group_size=100000)
#
# %% raw
# print(join_lf.limit(1000).collect())
# %% raw
# print(join_lf.slice(1000, 5000).collect())
# %%
# %%
#join_lf.collect().write_parquet('data/join_week_1-2.parquet')

