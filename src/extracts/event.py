import pandas as pd

from src.formatters import team_id_repl, score_clean
from src.utils import df_rename_fold


def get_event_infos(season):
    df = pd.read_csv('http://www.habitatring.com/games.csv')
    df = score_clean(df)
    df = df[df.season == season].copy()
    df = team_id_repl(df)
    df['datetime'] = df['gameday'] + ' ' + df['gametime']
    df.datetime = pd.to_datetime(df.datetime)
    df = df[[
        'game_id',
        'season',
        'game_type',
        'week',
        'away_team',
        'home_team',
        'datetime'
    ]]
    df['game_id'] = df['season'].astype(str) + '_' + df['week'].astype(str) + '_' + df['home_team'] + '_' + df['away_team']
    return df_rename_fold(df, 'away_','home_')