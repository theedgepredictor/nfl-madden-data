import random
import re
import time

import numpy as np
import pandas as pd
import datetime
import os
from typing import List
import pyarrow as pa
from bs4 import BeautifulSoup


def get_dataframe(path: str, columns: List = None):
    """
    Read a DataFrame from a parquet file.

    Args:
        path (str): Path to the parquet file.
        columns (List): List of columns to select (default is None).

    Returns:
        pd.DataFrame: Read DataFrame.
    """
    try:
        return pd.read_parquet(path, engine='pyarrow', dtype_backend='numpy_nullable', columns=columns)
    except Exception as e:
        print(e)
        return pd.DataFrame()


def put_dataframe(df: pd.DataFrame, path: str):
    """
    Write a DataFrame to a parquet file.

    Args:
        df (pd.DataFrame): DataFrame to write.
        path (str): Path to the parquet file.
        schema (dict): Schema dictionary.

    Returns:
        None
    """
    key, file_name = path.rsplit('/', 1)
    if file_name.split('.')[1] != 'parquet':
        raise Exception("Invalid Filetype for Storage (Supported: 'parquet')")
    os.makedirs(key, exist_ok=True)
    df.to_parquet(f"{key}/{file_name}",engine='pyarrow', schema=pa.Schema.from_pandas(df))


def create_dataframe(obj, schema: dict):
    """
    Create a DataFrame from an object with a specified schema.

    Args:
        obj: Object to convert to a DataFrame.
        schema (dict): Schema dictionary.

    Returns:
        pd.DataFrame: Created DataFrame.
    """
    df = pd.DataFrame(obj)
    for column, dtype in schema.items():
        df[column] = df[column].astype(dtype)
    return df

def get_seasons_to_update(root_path, feature_store_name):
    """
    Get a list of seasons to update based on the root path and sport.

    Args:
        root_path (str): Root path for the sport data.
        sport (ESPNSportTypes): Type of sport.

    Returns:
        List: List of seasons to update.
    """
    current_season = find_year_for_season()
    if os.path.exists(f'{root_path}/{feature_store_name}'):
        seasons = os.listdir(f'{root_path}/{feature_store_name}')
        fs_season = -1
        for season in seasons:
            temp = int(season.split('.')[0])
            if temp > fs_season:
                fs_season = temp
    else:
        fs_season = 2001
    if fs_season == -1:
        fs_season = 2001
    return list(range(fs_season, current_season + 1))


def find_year_for_season( date: datetime.datetime = None):
    """
    Find the year for a specific season based on the league and date.

    Args:
        league (ESPNSportTypes): Type of sport.
        date (datetime.datetime): Date for the sport (default is None).

    Returns:
        int: Year for the season.
    """
    SEASON_START_MONTH = {

        "NFL": {'start': 8, 'wrap': False},
    }
    if date is None:
        today = datetime.datetime.utcnow()
    else:
        today = date
    start = SEASON_START_MONTH["NFL"]['start']
    wrap = SEASON_START_MONTH["NFL"]['wrap']
    if wrap and start - 1 <= today.month <= 12:
        return today.year + 1
    elif not wrap and start == 1 and today.month == 12:
        return today.year + 1
    elif not wrap and not start - 1 <= today.month <= 12:
        return today.year - 1
    else:
        return today.year


def pfr_request(url, session=None):
    """
    PFR requests will require a session to avoid rate limiting.
    Be polite to their server
    """
    rand_int_sleep = random.randint(1, 8)
    rand_float_sleep = round(random.random(), 2)
    print(f"Schleeping for {rand_int_sleep + rand_float_sleep}")
    time.sleep(rand_int_sleep + rand_float_sleep)
    if session:
        r = session.get(url)
    else:
        raise Exception('No session passed. PFR requests will require a session to avoid rate limiting.')
    return r

def get_webpage_soup(html, html_attr=None, attr_key_val=None):
    if html:
        soup = BeautifulSoup(html, 'html.parser')
        if html_attr:
            soup = soup.find(html_attr, attr_key_val)
        return soup
    return None

def clean_string(s):
    if isinstance(s, str):
        return re.sub("[\W_]+",'',s)
    else:
        return s

def re_alphanumspace(s):
    if isinstance(s, str):
        return re.sub("^[a-zA-Z0-9 ]*$",'',s)
    else:
        return s

def re_braces(s):
    if isinstance(s, str):
        return re.sub("[\(\[].*?[\)\]]", "", s)
    else:
        return s

def re_numbers(s):
    if isinstance(s, str):
        n = ''.join(re.findall(r'\d+', s))
        return int(n) if n != '' else n
    else:
        return s


def name_filter(s):
    s = clean_string(s)
    s = re_braces(s)
    s = str(s)
    s = s.replace(' ', '').lower()
    return s


def df_rename_fold(df, t1_prefix, t2_prefix):
    '''
    The reverse of a df_rename_pivot
    Fold two prefixed column types into one generic type
    Ex: away_team_id and home_team_id -> team_id
    '''
    try:
        t1_all_cols = [i for i in df.columns if t2_prefix not in i]
        t2_all_cols = [i for i in df.columns if t1_prefix not in i]

        t1_cols = [i for i in df.columns if t1_prefix in i]
        t2_cols = [i for i in df.columns if t2_prefix in i]
        t1_new_cols = [i.replace(t1_prefix, '') for i in df.columns if t1_prefix in i]
        t2_new_cols = [i.replace(t2_prefix, '') for i in df.columns if t2_prefix in i]

        t1_df = df[t1_all_cols].rename(columns=dict(zip(t1_cols, t1_new_cols)))
        t2_df = df[t2_all_cols].rename(columns=dict(zip(t2_cols, t2_new_cols)))

        df_out = pd.concat([t1_df, t2_df]).reset_index().drop(columns='index')
        return df_out
    except Exception as e:
        print("--df_rename_fold-- " + str(e))
        print(f"columns in: {df.columns}")
        print(f"shape: {df.shape}")
        return df

