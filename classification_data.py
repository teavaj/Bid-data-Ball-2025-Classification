"""Gather and write data to parquet for classification model.
See data_gathering.ipynb for more details.
"""
import polars as pl
players_lf = pl.scan_csv("data/nfl-big-data-bowl-2025/players.csv", null_values="NA")
games_lf = pl.scan_csv(
    "data/nfl-big-data-bowl-2025/games.csv", null_values="NA", try_parse_dates=True
).with_columns(pl.col("gameDate").str.to_date(format="%m/%d/%Y"))
plays_lf = pl.scan_csv("data/nfl-big-data-bowl-2025/plays.csv", null_values="NA")

from pbp_2022 import savant_df as savant_lf

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

from player_targets import targets_lf as player_targets_lf
plays_epa_lf = plays_lf.with_columns(
    (pl.col("expectedPoints") - pl.col("expectedPointsAdded")).alias("epa")
)
from anya import anya_lf

anya_lf = anya_lf.select(
    (pl.selectors.contains("ANYA")), pl.col("GameId"), pl.col("playId")
)
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

weather_lf = pl.scan_csv("data/weather_data.csv").with_columns(
    pl.col("gameDate").str.to_date(format="%Y-%m-%d")
)

games_weather_lf = games_lf.join(
    weather_lf, on=["gameDate", "homeTeamAbbr"], how="left"
)
games_join_lf = games_weather_lf.join(
    stadiums_df, left_on="homeTeamAbbr", right_on="Team", how="left"
)

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

class_lf=plays_epa_lf.join(games_join_lf, on='gameId', how='left')

class_lf=class_lf.drop('playDescription', "^.*_right$")
# class_lf=class_lf.with_columns(
#     (pl.col('NextPlay').fill_null('0'))
# )

class_lf.collect().write_parquet('data/classification.parquet', compression='zstd', row_group_size=100000)
