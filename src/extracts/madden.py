import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from rapidfuzz import process, fuzz

from src.consts import POSITION_MAPPER, HIGH_POSITION_MAPPER
from src.extracts.player_stats import collect_roster
from src.utils import name_filter, find_year_for_season


def apply_merge_id(df):
    first = name_filter(df['name'].split(' ')[0])
    last = name_filter(df['name'].split(' ')[1])
    birth_date = df['birth_date']
    df['merge_id'] = f'{last[0:7].lower()}{first[0:3]}{birth_date.month}{birth_date.day}{birth_date.year}'
    return df

def madden_link_scraper(year, keyword):
    year = year + 1
    madden_ratings = f"https://maddenratings.weebly.com/madden-nfl-{str(year)[2:]}.html"

    url = madden_ratings
    grab = requests.get(url)
    soup = BeautifulSoup(grab.text, 'html.parser')
    sub_url = '/uploads/'
    #Get all sub pages
    internalLinks = [
        a.get('href') for a in soup.find_all('a')
        if a.get('href') and a.get('href').startswith(sub_url)]
    ##Try to pull the full player rating sheet
    links = [i for i in internalLinks if keyword in i]
    return links

def get_madden_ratings_from_web(year):
    try:
        season = year
        if year == 2024:
            return pd.DataFrame() # We pulled this externally due to naming conflict
        if year == 2013:
            year = 2024 # Madden 25 for 2013 season (25th year EA Sports has released the game)

        base = 'https://maddenratings.weebly.com'
        links = madden_link_scraper(year,'full_player_ratings')
        if len(links) == 0:
            # Try to grab individual team rosters and join them
            print('grabbing madden from team rosters')
            links = madden_link_scraper(year, '_madden_nfl_')
            dfs = []
            for link in links:
                df = pd.read_excel(base+link)
                if 'Team' not in df.columns:
                    team = link.split('/')[-1].split('_madden_nfl_')[0]
                    df['Team'] = team
                dfs.append(df)
            if dfs != []:
                dfs = pd.concat(dfs)
                if year == 2011:
                    dfs = dfs.loc[pd.to_numeric(dfs['Overall'], errors='coerce').notnull()].copy()
                return dfs
            print(f'Failed to get madden {year+1} ratings for season: {year} ')
            return pd.DataFrame()
        else:
            df= pd.read_excel(base+links[0])
            df['season'] = season
            return df
    except Exception as e:
        print(e)
        return pd.DataFrame()


def make_raw_madden(load_seasons):
    frames = {}
    for season in load_seasons:
        if season != 2024: # manual fill for naming issue on site
            frames[season] = get_madden_ratings_from_web(season)
    return frames

