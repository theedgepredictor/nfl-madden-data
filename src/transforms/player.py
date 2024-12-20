import pandas as pd

from src.extracts.event import get_event_infos
from src.extracts.player_stats import collect_players, collect_combine, collect_av, collect_roster
from src.formatters import team_id_repl
from src.utils import df_rename_fold


def get_static_players():
    """
    Pull from player and combine data
    """
    df = collect_players()

    ### Combine Extractor (First Come)
    combine_df = collect_combine()
    #combine_df = combine_df[combine_df.position_group == position_group].copy()
    valid_combine_df = combine_df[combine_df.pfr_id.notnull()].copy()[[
        'pfr_id',
        'forty',
        'bench',
        'vertical',
        'broad_jump',
        'cone',
        'shuttle'
    ]]
    valid_combine_df = pd.merge(valid_combine_df, df[['player_id', 'pfr_id']], on='pfr_id', how='left').drop(columns=['pfr_id'])
    invalid_combine_df = combine_df[combine_df.pfr_id.isnull()].copy()[[
        'name',
        'position_group',
        'forty',
        'bench',
        'vertical',
        'broad_jump',
        'cone',
        'shuttle'

    ]]
    invalid_combine_df = pd.merge(invalid_combine_df, df[['name', 'position_group', 'player_id']], on=['name', 'position_group'], how='left').drop(columns=['name', 'position_group'])
    combine_df = pd.concat([valid_combine_df, invalid_combine_df], axis=0).reset_index(drop=True)
    df = df.merge(combine_df, on='player_id', how='left')
    df = df[[
        'player_id',
        'name',
        'common_first_name',
        'first_name',
        'last_name',
        'short_name',
        'football_name',
        'suffix',
        'esb_id',
        'nfl_id',
        'pfr_id',
        'pff_id',
        'otc_id',
        'espn_id',
        'smart_id',
        'birth_date',
        #'high_pos_group',
        #'position_group',
        #'position',
        'height',
        'weight',
        'headshot',
        'college_name',
        'college_conference',
        #'jersey_number',
        'rookie_season',
        #'last_season',
        #'latest_team',
        #'status',
        #'status_abbr',
        #'ngs_status',
        #'ngs_status_short_description',
        #'years_of_experience',
        #'pff_position',
        #'pff_status',
        'draft_year',
        'draft_round',
        'draft_pick',
        'draft_team',
        'forty',
        'bench',
        'vertical',
        'broad_jump',
        'cone',
        'shuttle'
    ]]
    df['last_updated'] = pd.to_datetime('now')
    return df


def get_preseason_players(season):
    """
    Using info from the previous season create a preseason player. Rating information to be used
    as a base rating for the upcoming season
    :return:
    """
    df = collect_roster(season)
    df = df[df.week == 1].copy()
    df = pd.merge(df, get_static_players(), on='player_id', how='left')
    df['is_rookie'] = (df['rookie_season'] == season) & (df.years_exp == 0)

    ### AV Extractor (Previous Season)
    av_df = collect_av(season - 1)[[
        'player_id',
        'approximate_value'
    ]].rename(columns={'player_id': 'pfr_id', 'approximate_value': 'last_season_av'})
    df = pd.merge(df, av_df, on='pfr_id', how='left').drop(columns=['week'])

    ### Games played last year
    last_year_roster_df = collect_roster(season - 1)
    last_year_roster_df = last_year_roster_df.drop_duplicates(subset=['player_id', 'week'], keep='last')
    player_games_df = last_year_roster_df[((last_year_roster_df.week <= (18 if season - 1 >= 2021 else 17)) & (last_year_roster_df.status_abbr == 'A'))][['player_id']].copy()
    player_games_df['last_year_regular_season_games_active'] = 1
    player_games_df = player_games_df.groupby('player_id', group_keys=False).sum().reset_index(drop=False)

    player_post_games_df = last_year_roster_df[((last_year_roster_df.week > (18 if season - 1 >= 2021 else 17)) & (last_year_roster_df.status_abbr == 'A'))][['player_id']].copy()
    player_post_games_df['last_year_post_season_games_active'] = 1
    player_post_games_df = player_post_games_df.groupby('player_id', group_keys=False).sum().reset_index(drop=False)
    player_games_df = pd.merge(last_year_roster_df[['player_id', 'season']].drop_duplicates(), player_games_df, on='player_id', how='left').drop(columns=['season'])
    player_games_df = pd.merge(player_games_df, player_post_games_df, on='player_id', how='left')
    df = df.merge(player_games_df, on='player_id', how='left')
    return df



def get_pre_event_players(season):
    """
    Get Weekly Roster information
    :return:
    """
    df = get_event_infos(season)
