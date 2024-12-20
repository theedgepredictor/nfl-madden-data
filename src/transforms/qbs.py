import pandas as pd

from src.extracts.player_stats import collect_espn_player_stats


def calculate_raw_passer_value(df):
    ## takes a df, with properly named fields and returns a series w/ VALUE ##
    ## formula for reference ##
    ## https://fivethirtyeight.com/methodology/how-our-nfl-predictions-work/ ##
    ##      -2.2 * Pass Attempts +
    ##         3.7 * Completions +
    ##       (Passing Yards / 5) +
    ##        11.3 * Passing TDs –
    ##      14.1 * Interceptions –
    ##          8 * Times Sacked –
    ##       1.1 * Rush Attempts +
    ##       0.6 * Rushing Yards +
    ##        15.9 * Rushing TDs
    return (
        -2.2 * df['attempts'] +
        3.7 * df['completions'] +
        (df['passing_yards'] / 5) +
        11.3 * df['passing_tds'] -
        14.1 * df['interceptions'] -
        8 * df['sacks'] -
        1.1 * df['carries'] +
        0.6 * df['rushing_yards'] +
        15.9 * df['rushing_tds']
    )

def calculate_passer_rating(df):
    # Step 1: Calculate each component
    a = ((df['completions'] / df['attempts']) - 0.3) * 5
    b = ((df['passing_yards'] / df['attempts']) - 3) * 0.25
    c = (df['passing_tds'] / df['attempts']) * 20
    d = 2.375 - (df['interceptions'] / df['attempts']) * 25

    # Step 2: Cap each value between 0 and 2.375
    a = a.clip(0, 2.375)
    b = b.clip(0, 2.375)
    c = c.clip(0, 2.375)
    d = d.clip(0, 2.375)

    # Step 3: Calculate passer rating
    passer_rating = ((a + b + c + d) / 6) * 100

    return passer_rating

def quarterback_rating_stats(season):
    quarterback_attrs = [
        'player_id',
        'completions',
        'attempts',
        'passing_yards',
        'passing_tds',
        'interceptions',
        'sacks',
        'sack_yards',
        'passing_air_yards',
        'passing_yards_after_catch',
        'passing_first_downs',
        'passing_epa',
        'passing_2pt_conversions',
        'pacr',
        'dakota',
        'carries',
        'rushing_yards',
        'rushing_tds',
        'rushing_fumbles',
        'rushing_fumbles_lost',
        'rushing_first_downs',
        'rushing_epa',
        'rushing_2pt_conversions',
        #'fantasy_points',
        'fantasy_points_ppr'
    ]
    player_weekly_stats_df = collect_espn_player_stats(season-1)
    player_reg_season_stats_df = player_weekly_stats_df[((player_weekly_stats_df.position_group == 'quarterback') &(player_weekly_stats_df.season_type == 'REG'))].copy()[quarterback_attrs]
    player_reg_season_stats_df['VALUE_ELO'] = calculate_raw_passer_value(player_reg_season_stats_df)
    player_reg_season_stats_df['passer_rating'] = calculate_passer_rating(player_reg_season_stats_df)
    player_games_df = player_reg_season_stats_df[['player_id']].copy()
    player_games_df['regular_season_games'] = 1
    player_games_df = player_games_df.groupby('player_id', group_keys=False).sum().reset_index(drop=False)

    player_post_season_stats_df = player_weekly_stats_df[((player_weekly_stats_df.position_group == 'quarterback') &(player_weekly_stats_df.season_type == 'POST'))].copy()[quarterback_attrs]
    player_games_post_df = player_post_season_stats_df[['player_id']].copy()
    player_games_post_df['post_season_games'] = 1
    player_games_post_df = player_games_post_df.groupby('player_id', group_keys=False).sum().reset_index(drop=False)
    player_season_stats_df = player_reg_season_stats_df.groupby('player_id', group_keys=False).mean().reset_index(drop=False)

    player_season_stats_df = pd.merge(player_season_stats_df, player_games_df, on='player_id', how='left')
    player_season_stats_df = pd.merge(player_season_stats_df, player_games_post_df, on='player_id', how='left')
    return player_season_stats_df