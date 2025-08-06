import numpy as np
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

def get_player_regular_season_game_fs(season, group='off'):
    try:
        df = pd.read_parquet(f'https://github.com/theedgepredictor/nfl-feature-store/raw/main/data/feature_store/player/{group}/regular_season_game/{season}.parquet')
        return df
    except:
        return pd.DataFrame()

def collect_combine():
    data = pd.read_parquet("https://github.com/nflverse/nflverse-data/releases/download/combine/combine.parquet")
    data = team_id_repl(data)
    data['position_group'] = data.pos
    data.position_group = data.position_group.map(POSITION_MAPPER)
    return data.rename(columns={'player_name': 'name'})


def _fill_pre_2002_roster(year):
    r_data = pd.read_parquet(f"https://github.com/nflverse/nflverse-data/releases/download/rosters/roster_{year}.parquet")
    r_data = r_data[[
        'gsis_id',
        'season',
        'team',
        'depth_chart_position',
        'position',
        'jersey_number',
        'status',
        'years_exp',
        'birth_date',
        'full_name'
    ]]
    rosters = []
    for week in range(1, 18 + 4):
        snapshot = r_data.copy()
        snapshot['week'] = week
        snapshot['status_description_abbr'] = snapshot['status']
        rosters.append(snapshot)
    return pd.concat(rosters, axis=0).reset_index(drop=True)

def collect_roster(year):
    try:
        if year < 2002:
            player_nfld_df = _fill_pre_2002_roster(year)
        else:
            player_nfld_df = pd.read_parquet(f'https://github.com/nflverse/nflverse-data/releases/download/weekly_rosters/roster_weekly_{year}.parquet')
    except Exception as e:
        if year < 2024:
            return pd.DataFrame()
        print(f'Cant get latest rosters for {year}...using latest player pull as week 1 data')
        player_nfld_df = collect_players()[['player_id', 'birth_date','position', 'latest_team','status_abbr', 'status','years_of_experience','jersey_number','full_name']]
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
        'depth_chart_position',
        'jersey_number',
        'birth_date',
        'status',
        'status_description_abbr',
        'gsis_id',
        'full_name',
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
    player_nfld_df['jersey_number'] = player_nfld_df['jersey_number'].astype(str)
    player_nfld_df['jersey_number'] = player_nfld_df['jersey_number'].str.extract('(\d+)') # Only numeric jersey numbers
    player_nfld_df['jersey_number'] = player_nfld_df['jersey_number'].fillna(-1).astype(int) # Fill with -1 to avoid convert to float
    player_nfld_df['jersey_number'] = player_nfld_df['jersey_number'].astype(str) # Convert to string
    player_nfld_df['jersey_number'] = player_nfld_df['jersey_number'].replace("-1", np.nan) # Convert -1 to NaN

    player_nfld_df = player_nfld_df.loc[(
            (player_nfld_df.player_id.notnull()) & (player_nfld_df.birth_date.notnull()))].copy()
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
    data = data.rename(columns={'display_name': 'name', 'gsis_id': 'player_id'})

    def add_missing_draft_data(df):
        ## load missing draft data ##
        missing_draft = pd.read_csv(
            'https://github.com/greerreNFL/nfeloqb/raw/refs/heads/main/nfeloqb/Manual%20Data/missing_draft_data.csv',
        )
        ## groupby id to ensure no dupes ##
        missing_draft = missing_draft.groupby(['player_id']).head(1)
        ## rename the cols, which will fill if main in NA ##
        missing_draft = missing_draft.rename(columns={
            'rookie_year': 'rookie_season_fill',
            'draft_number': 'draft_pick_fill',
            'entry_year': 'draft_year_fill',
            'birth_date': 'birth_date_fill',
        })
        ## add to data ##
        df = pd.merge(
            df,
            missing_draft[[
                'player_id', 'rookie_season_fill', 'draft_pick_fill',
                'draft_year_fill', 'birth_date_fill'
            ]],
            on=['player_id'],
            how='left'
        )
        ## fill in missing data ##
        for col in [
            'rookie_season', 'draft_pick', 'draft_year', 'birth_date'
        ]:
            ## fill in missing data ##
            df[col] = df[col].combine_first(df[col + '_fill'])
            ## and then drop fill col ##
            df = df.drop(columns=[col + '_fill'])
        ## return ##
        return df
    data = add_missing_draft_data(data)

    return data



def collect_av(season):
    return pd.read_csv(f'https://github.com/theedgepredictor/nfl-madden-data/raw/main/data/pfr/approximate_value/{season}.csv')

def collect_raw_madden(season):
    return pd.read_csv(f"../../data/madden/raw/{season}.csv")
