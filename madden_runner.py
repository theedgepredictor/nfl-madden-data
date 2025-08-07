import os

import pandas as pd
from nfl_data_loader.utils.utils import get_seasons_to_update, find_year_for_season

from src.extracts.madden import make_raw_madden
from src.modeling.imputer import make_dataset_madden
from src.transforms.madden import make_stage_madden
from src.transforms.madden_registry import make_processed_madden

raw_madden_meta = {
    "name":'raw',
    "start_season": 2001,
    "raw_obj": make_raw_madden,
    }
stage_madden_meta = {
    "name":'stage',
    "start_season": 2001,
    "raw_obj": make_stage_madden,
    }
processed_madden_meta = {
    "name":'processed',
    "start_season": 2001,
    "raw_obj": make_processed_madden,
    }

dataset_madden_meta = {
    "name": 'dataset',
    "start_season": 2001,
    "raw_obj": make_dataset_madden,
}

FEATURE_STORE_METAS = [
    #raw_madden_meta,
    #stage_madden_meta,
    processed_madden_meta,
    dataset_madden_meta
]



def madden_runner():
    root_path = './data/madden'
    for fs_meta_obj in FEATURE_STORE_METAS:
        feature_store_name = fs_meta_obj['name']
        start_season = fs_meta_obj['start_season']
        ## Determine pump mode
        update_seasons = get_seasons_to_update(root_path, feature_store_name)
        current_season = find_year_for_season()
        update_seasons = list(range(2001, 2026))
        skip_raw = False
        if len(update_seasons) == 1 and update_seasons[0] == current_season:
            try:
                a = pd.read_csv(f"{root_path}/{feature_store_name}/{update_seasons[0]}.csv")
                print('Madden Data Up to Date Skipping Raw...')
                skip_raw = True

            except Exception as e:
                 skip_raw = False # File does not exist yet
        if not skip_raw:
            frames = fs_meta_obj['raw_obj'](update_seasons)
            for season, df in frames.items():
                if feature_store_name == 'dataset':
                    df.to_parquet(f"{root_path}/{feature_store_name}/{season}.parquet", index=False)
                else:
                    df.to_csv(f"{root_path}/{feature_store_name}/{season}.csv", index=False)

if __name__ == '__main__':
    madden_runner()