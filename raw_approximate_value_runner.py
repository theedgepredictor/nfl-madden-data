import os

import pandas as pd
from nfl_data_loader.utils.utils import get_seasons_to_update, find_year_for_season

from src.extracts.pfr import get_approximate_values

approximate_value_meta = {
    "name":'approximate_value',
    "start_season": 2000,
    "raw_obj": get_approximate_values
    }
FEATURE_STORE_METAS = [
    approximate_value_meta
]



def raw_pfr_runner():
    """
    At the start of a new season we will pull the previous year's rosters to get the approximate values. Acts as an
    initial rating for the next season for the player. Rookie and missing player av's will be imputed using a KNNImputer in a later step.

    :return:
    """
    root_path = './data/pfr'
    for fs_meta_obj in FEATURE_STORE_METAS:
        feature_store_name = fs_meta_obj['name']
        start_season = fs_meta_obj['start_season']
        ## Determine pump mode
        update_seasons = get_seasons_to_update(root_path, feature_store_name)
        current_season = find_year_for_season()
        skip_raw = False
        if len(update_seasons) == 1 and update_seasons[0] == current_season:
            try:
                a = pd.read_csv(f"{root_path}/{feature_store_name}/{update_seasons[0]}.csv")
                print('Approximate Value Data Up to Date Skipping Raw...')
                skip_raw = True

            except Exception as e:
                skip_raw = False # File does not exist yet
                update_seasons = [update_seasons[0]-1, update_seasons[0]]
        if not skip_raw:
            frames = fs_meta_obj['raw_obj'](update_seasons)
            for season, df in frames.items():
                df.to_csv(f"{root_path}/{feature_store_name}/{season}.csv", index=False)



if __name__ == '__main__':
    raw_pfr_runner()