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