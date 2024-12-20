import pandas as pd

from src.consts import POSITION_MAPPER, HIGH_POSITION_MAPPER
from src.formatters import team_id_repl


def collect_espn_player_stats(season, group=''):
    """
    Grab whole season stats for players
    :param season: 2000 - current
    :param group: empty | def | kicking
    :return:
    """
    if group in ['def', 'kicking']:
        group = '_' + group
    data = pd.read_parquet(f'https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats{group}_{season}.parquet')
    data = team_id_repl(data)
    data['position_group'] = data.position
    data.position_group = data.position_group.map(POSITION_MAPPER)
    return data

def collect_depth_chart(season):
    data = pd.read_parquet(f'https://github.com/nflverse/nflverse-data/releases/download/depth_charts/depth_charts_{season}.parquet')
    data = team_id_repl(data)
    data['position_group'] = data.position
    data.position_group = data.position_group.map(POSITION_MAPPER)
    data['depth_team'] = data['depth_team'].astype(int)
    return data

def collect_injuries(season):
    data = pd.read_parquet(f'https://github.com/nflverse/nflverse-data/releases/download/injuries/injuries_{season}.parquet')
    data = team_id_repl(data)
    data['position_group'] = data.position
    data.position_group = data.position_group.map(POSITION_MAPPER)
    return data.rename(columns={'gsis_id': 'player_id'})

def collect_combine():
    data = pd.read_parquet("https://github.com/nflverse/nflverse-data/releases/download/combine/combine.parquet")
    data = team_id_repl(data)
    data['position_group'] = data.pos
    data.position_group = data.position_group.map(POSITION_MAPPER)
    return data.rename(columns={'player_name': 'name'})

def collect_players():
    data = pd.read_parquet("https://github.com/nflverse/nflverse-data/releases/download/players_components/players.parquet")
    data = team_id_repl(data)
    data['position_group'] = data.position
    data.position_group = data.position_group.map(POSITION_MAPPER)
    data['high_pos_group'] = data.position_group
    data.high_pos_group = data.high_pos_group.map(HIGH_POSITION_MAPPER)
    data['status_abbr'] = data.status
    data.status_abbr = data.status_abbr.fillna('N')
    data.status_abbr = data.status_abbr.apply(lambda x: x[0])
    data.status_abbr = data.status_abbr.replace(['W', 'E', 'I', 'N'], ['N', 'N', 'N', 'N'])
    return data.rename(columns={'display_name': 'name', 'gsis_id': 'player_id'})

def collect_roster(year):
    try:
        player_nfld_df = pd.read_parquet(f'https://github.com/nflverse/nflverse-data/releases/download/weekly_rosters/roster_weekly_{year}.parquet')
    except Exception as e:
        print(f'Cant get latest rosters for {year}...using latest player pull as week 1 data')
        player_nfld_df = collect_players()[['player_id', 'birth_date','position', 'latest_team','status_abbr', 'years_of_experience','jersey_number']]
        player_nfld_df = player_nfld_df.rename(
            columns={
                'latest_team': 'team',
                'years_of_experience': 'years_exp',
                'status_abbr': 'status_description_abbr',
                'player_id': 'gsis_id'
            })
        player_nfld_df['season'] = year
        player_nfld_df['week'] = 1

    player_nfld_df = team_id_repl(player_nfld_df)
    player_nfld_df = player_nfld_df[[
        'season',
        'week',
        'team',
        'position',
        #'depth_chart_position',
        'jersey_number',
        'birth_date',
        # 'status',
        'status_description_abbr',
        'gsis_id',
        # 'sportradar_id',
        # 'yahoo_id',
        # 'rotowire_id',
        # 'pff_id',
        # 'fantasy_data_id',
        # 'sleeper_id',
        'years_exp',
        # 'headshot_url',
        # 'ngs_position',
        # 'game_type',

        # 'football_name',
        # 'esb_id',
        # 'gsis_it_id',
        # 'smart_id',
    ]].rename(columns={'full_name': 'name', 'gsis_id': 'player_id'})
    player_nfld_df = player_nfld_df.loc[(
            (player_nfld_df.player_id.notnull()) & (player_nfld_df.birth_date.notnull()))].copy()
    player_nfld_df = player_nfld_df.drop(columns=['birth_date'])
    player_nfld_df = player_nfld_df.loc[player_nfld_df.player_id != ''].copy()
    player_nfld_df = player_nfld_df.rename(columns={'status_description_abbr': 'status_abbr'})
    player_nfld_df.status_abbr = player_nfld_df.status_abbr.fillna('N')
    player_nfld_df.status_abbr = player_nfld_df.status_abbr.apply(lambda x: x[0])
    player_nfld_df.status_abbr = player_nfld_df.status_abbr.replace(['W', 'E', 'I', 'N'], ['N', 'N', 'N', 'N'])
    player_nfld_df = player_nfld_df.reset_index().drop(columns='index')
    #player_nfld_df = player_nfld_df[player_nfld_df.week == 1].copy()
    player_nfld_df['position_group'] = player_nfld_df.position
    player_nfld_df.position_group = player_nfld_df.position_group.map(POSITION_MAPPER)
    player_nfld_df['high_pos_group'] = player_nfld_df.position_group
    player_nfld_df.high_pos_group = player_nfld_df.high_pos_group.map(HIGH_POSITION_MAPPER)
    return player_nfld_df

def collect_av(season):
    return pd.read_csv(f"../../data/pfr/approximate_value/{season}.csv")

def collect_raw_madden(season):
    return pd.read_csv(f"../../data/madden/raw/{season}.csv")
