## assignment functions that replaces fastr team names with legacy team names ##
repl = {
    'OAK': 'LV',  # Oakland Raiders became Las Vegas Raiders
    'ARZ': 'ARI',  # Arizona Cardinals abbreviation changed
    'HST': 'HOU',  # Houston Texans abbreviation changed
    'BLT': 'BAL',  # Baltimore Ravens abbreviation changed
    'SL': 'LAR',  # St. Louis Rams became Los Angeles Rams
    'CLV': 'CLE',  # Cleveland Browns abbreviation changed
    'SD': 'LAC',  # San Diego Chargers became Los Angeles Chargers
    'LA': 'LAR',
    'STL': 'LAR',
}

def team_id_repl(df):
    """
    Replaces fastr team ids with a legacy nfelo ids.
    """
    ## if a col with team names exists, replace it ##
    for col in [
        'home_team', 'away_team', 'team_abbr',
        'posteam', 'defteam', 'penalty_team',
        'side_of_field', 'timeout_team', 'td_team',
        'return_team', 'possession_team',
        'recent_team', 'opponent_team', 'team1', 'team2',
        'latest_team', 'draft_team','team'
    ]:
        if col in df.columns:
            df[col] = df[col].replace(repl)
    return df

manual_clean_dict = {
    "2009_17_IND_BUF": {
        "home_score": 30,
        "away_score": 7
    },
    "2013_07_CIN_DET": {
        "home_score": 24,
        "away_score": 27
    },
    "2015_06_ARI_PIT": {
        "home_score": 25,
        "away_score": 13
    },
    "2015_09_PHI_DAL": {
        "home_score": 27,
        "away_score": 33
    },
    "2015_15_KC_BAL": {
        "home_score": 14,
        "away_score": 34
    },
    "2016_01_MIN_TEN": {
        "home_score": 16,
        "away_score": 25
    },
    "2016_05_NE_CLE": {
        "home_score": 13,
        "away_score": 33
    }
}

def score_clean(df):
    '''
    Manually clean scores that are incorrect in the
    game file
    '''
    ## create a home and away map ##
    home_map = {k:manual_clean_dict[k]['home_score'] for k in manual_clean_dict.keys()}
    away_map = {k:manual_clean_dict[k]['away_score'] for k in manual_clean_dict.keys()}
    ## apply ##
    df['home_score'] = df['game_id'].map(home_map).combine_first(df['home_score'])
    df['away_score'] = df['game_id'].map(away_map).combine_first(df['away_score'])
    ## return
    return df