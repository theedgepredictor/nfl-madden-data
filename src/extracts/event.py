import pandas as pd

from src.formatters import team_id_repl
from src.utils import df_rename_fold


def get_event_infos(season):
    df = pd.read_csv('http://www.habitatring.com/games.csv')
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
    return df_rename_fold(df, 'away_','home_')