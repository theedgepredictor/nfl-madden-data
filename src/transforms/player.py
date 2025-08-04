import pandas as pd

from src.extracts.event import get_event_infos
from src.extracts.madden import get_approximate_value, get_madden_ratings
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
        'pfr_id',
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

def apply_rookie_av(df):
    if df['draft_pick'] == 1:
        df['last_season_av'] = 12
    elif df['draft_pick'] == 2:
        df['last_season_av'] = 11
    elif df['draft_pick'] == 3:
        df['last_season_av'] = 10.5
    elif df['draft_pick'] == 4:
        df['last_season_av'] = 9
    elif df['draft_pick'] == 5:
        df['last_season_av'] = 8.5
    else:
        df['last_season_av'] = (9 - df['draft_round']) * 0.5
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
    av_df = get_approximate_value(season - 1)[[
        'player_id',
        'approximate_value'
    ]].rename(columns={'player_id': 'pfr_id', 'approximate_value': 'last_season_av'})
    df = pd.merge(df, av_df, on='pfr_id', how='left')

    processed_madden_df = pd.concat([
        get_madden_ratings(season),
        get_madden_ratings(season-1),
    ]).drop_duplicates(subset=['player_id'], keep=('first' if season != 2002 else 'last'))

    processed_madden_df['season'] = season
    df = pd.merge(df, processed_madden_df.drop(columns=['position_group']), on=['player_id','season'], how='left')

    rookie_approx_value_df = df[df['is_rookie']==True].copy()
    rookie_approx_value_df.draft_round = rookie_approx_value_df.draft_round.fillna(8)
    rookie_approx_value_df.draft_pick = rookie_approx_value_df.draft_pick.fillna(rookie_approx_value_df.draft_pick.max() + 1)
    rookie_approx_value_df = rookie_approx_value_df.apply(apply_rookie_av, axis=1)

    df = df[df['is_rookie']==False].copy()
    df = pd.concat([df, rookie_approx_value_df], ignore_index=True)

    df = df.drop_duplicates(subset=['player_id'], keep='first').drop(columns=[
        'team',
        'week',
        'position',
        'jersey_number',
        'status_abbr',
        'position_group',
        'high_pos_group',
    ])
    df = df[[
        'season',
        'player_id',
        'madden_id',
        'years_exp',
        'is_rookie', 'last_season_av',  'overallrating', 'agility',
         'acceleration', 'speed', 'stamina', 'strength', 'toughness', 'injury',
         'awareness', 'jumping', 'trucking', 'throwpower', 'throwaccuracyshort',
         'throwaccuracymid', 'throwaccuracydeep', 'playaction', 'throwonrun',
         'carrying', 'ballcarriervision', 'stiffarm', 'spinmove', 'jukemove',
         'catching', 'shortrouterunning', 'midrouterunning', 'deeprouterunning',
         'spectacularcatch', 'catchintraffic', 'release', 'runblocking',
         'passblocking', 'impactblocking', 'mancoverage', 'zonecoverage',
         'tackle', 'hitpower', 'press', 'pursuit', 'kickaccuracy', 'kickpower',
         'return']]
    return df



def get_pre_event_players(season):
    """
    Get Weekly Roster information
    :return:
    """
    df = get_event_infos(season)
