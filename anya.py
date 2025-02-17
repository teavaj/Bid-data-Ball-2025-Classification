"""ANY/A (Adjusted Net Yards per Attempt)
(Passing Yards + 20 * Touchdowns - 45 * Interceptions - Sack Yards) / (Pass Attempts + Sacks)
"""
import polars as pl

pl.Config.set_tbl_rows(-1)
from pbp_2022 import savant_df as savant_lf

id_columns = ['GameId', 'playId']

#%%
def passing_yards(play_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Calculate passing yards for each play"""
    passing_lf = play_lf.filter(
        (pl.col('PlayType') == "PASS") &
        (pl.col('Yards') > 0)
    ).sort(id_columns, descending=False)
    passing_lf = passing_lf.with_columns(
        pl.col('Yards').cum_sum()
        .over('GameId', 'OffenseTeam')
        .alias('ANYA_PASSING_YARDS')
    ).select(['GameId', 'playId', 'ANYA_PASSING_YARDS', 'PlayType'])
    passing_lf = ((play_lf.join(passing_lf, on=['GameId', 'playId', 'PlayType'], how='left')
                   .sort(id_columns, descending=False)
                   .fill_null(strategy='forward')
                   )
        #.filter(pl.col('Yards')>0)
        ).fill_null(0)
    return passing_lf
#%%

def sac_yards(play_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Calculate sack yards for each play"""
    sac_lf = play_lf.filter(
        (pl.col('IsSack') == 1)
    ).sort(id_columns, descending=False)
    sac_lf = sac_lf.with_columns(
        pl.col('Yards').abs().cum_sum()
        .over('GameId', 'DefenseTeam')
        .alias('ANYA_SACK_YARDS')
    ).select(['GameId', 'playId', 'ANYA_SACK_YARDS'])
    sac_lf = ((play_lf.join(sac_lf, on=id_columns, how='left')
               .sort(id_columns, descending=False)
               .fill_null(strategy='forward')
               )
        #.filter(pl.col('IsSack')==1)
        )
    return sac_lf

#%%
def anya(play_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Calculate ANYA for each play"""
    join_lf = play_lf
    # add passing yards

    passing_lf = passing_yards(play_lf).select(['GameId', 'playId', 'ANYA_PASSING_YARDS'])
    join_lf = (join_lf.join(passing_lf, on=id_columns, how='left')
               .sort(id_columns, descending=False)
               #.fill_null(strategy='forward')
               )
    # add sack yards
    sac_lf = sac_yards(play_lf).select(['GameId', 'playId', 'ANYA_SACK_YARDS']).fill_null(0)
    join_lf = (join_lf.join(sac_lf, on=id_columns, how='left')
               .sort(id_columns, descending=False)
               #.fill_null(strategy='forward')
               )
 #   add some more sums
    join_lf=join_lf.with_columns(
        (pl.col('IsSack').cum_sum().over('GameId', 'DefenseTeam').alias('ANYA_SACKS')),
        (pl.col('IsTouchdown').cum_sum().over('GameId', 'OffenseTeam').alias('ANYA_TD_SUM')),
        (pl.col('IsInterception').cum_sum().over('GameId', 'OffenseTeam').alias('ANYA_IT_SUM')),
        (pl.col('IsPass').cum_sum().over('GameId', 'OffenseTeam').alias('ANYA_PASS_ATTEMPTS'))

    )
    
    # calculate ANYA
    over=(pl.col('ANYA_PASSING_YARDS')+(20*pl.col('ANYA_TD_SUM'))-(45*pl.col('ANYA_IT_SUM'))-pl.col('ANYA_SACK_YARDS'))
    under=(pl.col('ANYA_PASS_ATTEMPTS')+pl.col('ANYA_SACKS'))
    anya_lf = join_lf.with_columns(
        (pl.when(under>0).then(over/under).otherwise(6.0)


        )
        .alias('ANYA')
    )
    
    return anya_lf

#%%
anya_lf=anya(savant_lf)
#%%
