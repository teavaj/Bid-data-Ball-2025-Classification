"""Additional play-by-play data for the 2022 NFL season from https://nflsavant.com/about.php."""

import polars as pl

# %%
savant_df = pl.scan_csv('data/nfl-savant/NFL Stats 2022.csv').filter(
    (pl.col('PlayType') != 'KICK OFF') &
    (pl.col('PlayType') != 'TIMEOUT') &
    (pl.col('PlayType') != 'NO PLAY') &
    (pl.col('PlayType') != "EXTRA POINT") &
    (pl.col('PlayType') != "TWO-POINT CONVERSION") &
    (pl.col('PlayType') != "CLOCK STOP") &
    (pl.col('PlayType') != "PENALTY") &
    (pl.col('PlayType') != "EXCEPTION")
)
nfl_df = pl.scan_csv('data/nfl-big-data-bowl-2025/plays.csv', null_values=['NA'])


# %%

def get_joinId(savant_df: pl.DataFrame, nfl_df: pl.DataFrame) -> list[pl.DataFrame, pl.DataFrame]:
    """Create a joinId column in both savant and nfl DataFrames."""

    savant_df_processed = savant_df.with_columns(
        (pl.col('Minute').cast(pl.String).str.zfill(2)),
        (pl.col('Second').cast(pl.String).str.zfill(2)),

    )
    savant_df_processed = savant_df_processed.with_columns(
        (pl.concat_str(['Minute', 'Second'], separator=':')).alias('GameClock')
    )
    savant_df_processed = savant_df_processed.with_columns(
        (pl.concat_str(['GameId', 'Quarter', 'GameClock'], separator="-").alias('joinId'))
    )

    nfl_df_processed = nfl_df.with_columns(
        (pl.concat_str(['gameId', 'quarter', 'gameClock'], separator="-").alias('joinId'))
    )
    return savant_df_processed, nfl_df_processed


# %%
def get_playId(savant_df: pl.DataFrame, nfl_df: pl.DataFrame) -> pl.DataFrame:
    """Join savant and nfl DataFrames on joinId for playId."""
    savant_df_processed, nfl_df_processed = get_joinId(savant_df, nfl_df)
    nfl_df_join = nfl_df_processed.select(['joinId', 'playId'])
    savant_df_processed = savant_df_processed.join(nfl_df_join, on='joinId', how='inner')
    return savant_df_processed


savant_df = get_playId(savant_df, nfl_df)

savant_df=savant_df.with_columns(pl.col('PlayType').shift(n=-1).alias('NextPlay'))

# %%
