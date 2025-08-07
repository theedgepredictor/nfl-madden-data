import time

import pandas as pd
import requests
from nfl_data_loader.utils.utils import get_webpage_soup

from src.utils import  pfr_request


def get_approximate_values(update_seasons):
    NFL_SR_ABBR_LIST = ['kan', 'jax', 'car', 'rav', 'buf', 'min', 'det', 'atl', 'nwe', 'was',
                        'cin', 'nor', 'sfo', 'ram', 'nyg', 'den', 'cle', 'clt', 'oti', 'nyj', 'htx',
                        'tam', 'mia', 'pit', 'phi', 'gnb', 'chi', 'dal', 'crd', 'sdg',
                        'sea', 'rai']
    WINDOWS_CHROME_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    session = requests.Session()
    session.headers.update(WINDOWS_CHROME_HEADERS)
    frames = {}
    for season in update_seasons:
        datas = []
        for team_str in NFL_SR_ABBR_LIST:
            try:
                html = pfr_request(f'https://www.pro-football-reference.com/teams/{team_str}/{season}_roster.htm', session=session)
                page = get_webpage_soup(html.text)
                page = get_webpage_soup(str(page).replace('<!--', '').replace('-->', ''))
                page = get_webpage_soup(str(page), 'table', {'id': 'roster'})

                for i in page.find_all('tr'):
                    player = i.find('td', {'data-stat': 'player'})
                    if player is None:
                        continue
                    approx_value = i.find('td', {'data-stat': 'av'})
                    if approx_value.text == '':
                        approx_value = None
                    a = player.find('a')
                    player_id = a.get('href').split('/')[-1].replace('.htm', '') if a is not None else None
                    name = player.text

                    if player_id is None and name == 'Team Total':
                        continue
                    data = {
                        'player_id': player_id,
                        'name': name,
                        'team': team_str,
                        'season': season,
                        'approximate_value': approx_value.text if approx_value is not None else 0
                    }

                    datas.append(data)
            except Exception as e:
                print(e)
                print(f"Requires manual intervention: {f'https://www.pro-football-reference.com/teams/{team_str}/{season}_roster.htm'}")
        df = pd.DataFrame(datas)
        df['approximate_value'] = df['approximate_value'].astype(int)
        frames[season] = df
    return frames