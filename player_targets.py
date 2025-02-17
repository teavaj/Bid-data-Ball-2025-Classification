"""Calculate cumulative sum of rushing/receiving targets for each player in each game"""
import polars as pl

# %%
player_play_lf = pl.scan_csv('data/nfl-big-data-bowl-2025/player_play.csv', null_values='NA')
id_columns = ["gameId", 'nflId', 'playId']
metrics_columns = ['hadRushAttempt',
                   'rushingYards', 'wasTargettedReceiver', 'hadPassReception', 'receivingYards']
player_play_lf = player_play_lf.select("gameId", 'nflId', 'playId', 'hadRushAttempt',
                                       'rushingYards', 'wasTargettedReceiver', 'hadPassReception', 'receivingYards'

                                       )

# %%
targets_lf = player_play_lf
for metric in metrics_columns:
    targets_lf = targets_lf.sort(by=id_columns, descending=False)
    targets_lf=targets_lf.with_columns(
        pl.col(metric).cum_sum().over(['gameId', 'nflId']).alias(f'{metric}_sum')
    )
#targets_lf.collect()
targets_lf=targets_lf.select(
    (pl.selectors.contains('Id')),
    (pl.selectors.contains('_sum'))
)
