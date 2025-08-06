from pathlib import Path

import pandas as pd
from pandas.api.types import is_numeric_dtype
from sklearn.experimental import enable_iterative_imputer  # noqa: F401
from sklearn.impute import IterativeImputer
from sklearn.linear_model import Ridge

from src.consts import HIGH_POSITION_MAPPER
from src.modeling.consts import META, GENERAL_ATTRIBUTES, MADDEN_ATTRIBUTE_MAP, TEAM_MAPPER, RUN_STYLE_MAPPER, ARCHETYPE_POSITION_MAPPERS
from src.transforms.madden_registry import read_missed_madden_data, read_processed_madden_data
from src.utils import find_year_for_season

MADDEN_DIR = (Path(__file__).resolve()          # /project_root/src/my_module.py
              .parents[2]                       # /project_root/
              / "data" / "madden")     # /project_root/data/madden/raw


def read_madden_dataset(year):
    return pd.read_parquet(f'{MADDEN_DIR}/dataset/{year}.parquet')

class MaddenImputationRunner:
    """
    A class to load, preprocess, group, and impute missing Madden player attributes
    over historical seasons using bin-based iterative imputation.
    """

    def __init__(self):
        """Initialize the runner with empty base ratings and grouped ratings."""
        self.base_ratings = None
        self.base_rating_groups = {}

    def load_base_ratings(self):
        """
        Load and preprocess base Madden ratings across all seasons.

        Steps:
        - Load processed Madden data for all seasons from 2001 to the latest.
        - Append missed Madden data.
        - Drop any rows without a madden_id (invalid records).
        - Map 'runningstyle' and 'team' values to their numeric codes.
        """
        self.base_ratings = pd.concat(
            [read_processed_madden_data(season) for season in list(range(2001, find_year_for_season() + 1))]
            + [read_missed_madden_data()]
        )
        ### Going to try full playerverse imputation
        #self.base_ratings = self.base_ratings[self.base_ratings.madden_id.notnull()].copy()
        self.base_ratings['runningstyle'] = self.base_ratings['runningstyle'].map(RUN_STYLE_MAPPER)
        self.base_ratings['team'] = self.base_ratings['team'].map(TEAM_MAPPER)

    def group_base_ratings(self):
        """
        Group base ratings by position group and preprocess each group.

        Steps:
        - Skip 'NA' position group.
        - Sort players within each position group by madden_id and season.
        - Map 'archetype' to its numeric code based on position group.
        - Fill missing 'yearspro' based on record order per player.
        - Fill default values for draft_round (8), draft_pick (258), and set rookie flag.
        - For numerical performance attributes, fill missing values with the mean per season.
        - Ensure Madden attribute columns are nullable integers.
        """
        for position_group in HIGH_POSITION_MAPPER.keys():
            if position_group == 'NA':
                continue

            print(position_group)

            # Filter & sort data for the given position group
            base_group = (
                self.base_ratings[self.base_ratings['position_group'] == position_group]
                .sort_values(by=['madden_id', 'season'], ascending=[True, False])
                .copy()
            )

            # Map archetype to numeric code
            base_group['archetype'] = base_group['archetype'].map(ARCHETYPE_POSITION_MAPPERS[position_group])

            # Fill yearspro if missing (incremental count per player)
            base_group['yearspro'] = base_group['yearspro'].fillna(
                base_group.groupby('madden_id').cumcount()
            ).astype("Int64")

            # Fill defaults for draft info and rookie flag
            base_group['draft_round'] = base_group['draft_round'].fillna(8).astype(int)
            base_group['draft_pick'] = base_group['draft_pick'].fillna(258).astype(int)
            base_group['is_rookie'] = base_group['yearspro'] == 0

            # Fill mean values for numeric columns per season
            mean_fill_cols = [
                'age', 'height', 'weight', 'forty', 'bench', 'vertical', 'broad_jump', 'cone', 'shuttle',
                'last_season_av'
            ]
            for col in mean_fill_cols:
                if col in base_group.columns and is_numeric_dtype(base_group[col]):
                    base_group[col] = base_group.groupby(['season'])[col].transform(
                        lambda x: x.fillna(x.mean())
                    ).astype(float)

            # Convert Madden attribute fields to nullable integers
            for col, dtype in MADDEN_ATTRIBUTE_MAP.items():
                if col in base_group.columns:
                    base_group[col] = (
                        pd.to_numeric(base_group[col], errors="coerce")
                        .astype("Int64")
                    )

            # Store the cleaned group
            self.base_rating_groups[position_group] = base_group

    def find_optimal_bins(self, df, bin_col="last_season_av", min_bins=4, max_bins=8):
        """
        Find the optimal number of quantile-based bins for imputation so that
        bin sizes are as evenly distributed as possible.

        Parameters:
            df (pd.DataFrame): Data containing the column to bin.
            bin_col (str): Column name to bin (default='last_season_av').
            min_bins (int): Minimum number of bins to test.
            max_bins (int): Maximum number of bins to test.

        Returns:
            int: Optimal number of bins for most even distribution.
        """
        best_bins = min_bins
        smallest_diff = float('inf')

        for bins in range(min_bins, max_bins + 1):
            try:
                bin_assignments = pd.qcut(
                    df[bin_col],
                    q=bins,
                    labels=False,
                    duplicates='drop'
                )

                counts = bin_assignments.value_counts()
                diff = counts.max() - counts.min()

                if diff < smallest_diff:
                    smallest_diff = diff
                    best_bins = bins

            except ValueError:
                # Occurs if there are too few unique values to split into bins
                continue

        print(f"Optimal bins for {bin_col}: {best_bins} (min diff = {smallest_diff})")
        return best_bins

    def impute_by_last_season_bin(
        self,
        df,
        meta_cols,
        target_cols,
        bin_col="last_season_av",
        n_bins=None,
        season_range=None,
        estimator=None,
        random_state=42,
        max_iter=20
    ):
        """
        Perform iterative imputation within bins of players grouped by `bin_col`.

        Parameters:
            df (pd.DataFrame): Data containing both meta and target columns.
            meta_cols (list): Columns preserved and not imputed.
            target_cols (list): Columns to be imputed.
            bin_col (str): Column used to create bins (default='last_season_av').
            n_bins (int): Number of quantile-based bins to split the data into.
            season_range (tuple): Optional (start, end) season filtering.
            estimator (sklearn estimator): Estimator for IterativeImputer. Defaults to Ridge.
            random_state (int): Random seed for reproducibility.
            max_iter (int): Maximum iterations for imputation.

        Returns:
            pd.DataFrame: DataFrame with imputed values for target_cols.
        """
        if estimator is None:
            estimator = Ridge(alpha=1.0)

        df = df.copy()

        # Optional filtering by season
        if season_range:
            start, end = season_range
            df = df[(df['season'] >= start) & (df['season'] <= end)].copy()

        # Dynamically find optimal bins if not provided
        if n_bins is None:
            n_bins = self.find_optimal_bins(df, bin_col=bin_col)

        # Create quantile-based bins for imputation groups
        df[f"{bin_col}_bin"] = pd.qcut(
            df[bin_col],
            q=n_bins,
            labels=False,
            duplicates='drop'
        )

        print(df[f"{bin_col}_bin"].value_counts())

        imputed_parts = []
        for bin_id, bin_df in df.groupby(f"{bin_col}_bin"):
            bin_index = bin_df.index
            meta_part = bin_df[meta_cols]
            target_part = bin_df[target_cols]

            # Convert to float for sklearn (handles Int64 + pd.NA)
            target_part = target_part.apply(pd.to_numeric, errors="coerce").astype(float)

            # Iterative imputation using chosen estimator
            imputer = IterativeImputer(
                estimator=estimator,
                random_state=random_state,
                max_iter=max_iter
            )
            imputed_array = imputer.fit_transform(target_part)

            # Combine metadata and imputed results
            imputed_parts.append(
                pd.concat(
                    [meta_part, pd.DataFrame(imputed_array, index=bin_index, columns=target_cols)],
                    axis=1
                )
            )

        return pd.concat(imputed_parts)

    def run(self):
        """
        Execute the full Madden imputation pipeline for all position groups.

        Steps:
        1. Load and preprocess base ratings.
        2. Group ratings by position group.
        3. For each position group:
            - Split and sort data into archetype/runningstyle subset.
            - Prepare attributes dataset without style-related columns.
            - Impute missing Madden attributes for different year ranges.
            - Merge imputed datasets back together.
            - Impute archetype/runningstyle/changeofdirection for specific ranges.
        4. Return a single DataFrame containing all processed position groups.
        """
        # Step 1: Load and preprocess data
        self.load_base_ratings()

        # Step 2: Group data by position
        self.group_base_ratings()

        all_groups = []

        # Step 3: Loop through all position groups and process
        for position_group, group_df in self.base_rating_groups.items():
            print(f"\n=== Processing position group: {position_group} ===")

            # Keep archetype/runningstyle/changeofdirection for later merge
            arch_split = group_df[META + ['season', 'archetype', 'runningstyle', 'changeofdirection']] \
                .sort_values(by=['season'], ascending=[False])

            # Dataset without style-related fields
            attrs_dataset = group_df[META + GENERAL_ATTRIBUTES + list(MADDEN_ATTRIBUTE_MAP.keys())] \
                .sort_values(by=['season', 'overallrating'], ascending=[False, False]) \
                .drop(columns=['archetype', 'runningstyle', 'changeofdirection'])

            print(f"{position_group} initial attrs_dataset shape:", attrs_dataset.shape)

            # Columns to impute (exclude style columns)
            madden_attrs = [i for i in list(MADDEN_ATTRIBUTE_MAP.keys())
                            if i not in ['archetype', 'runningstyle', 'changeofdirection']]

            # Impute attributes for 2010–latest
            attrs_2010_latest = self.impute_by_last_season_bin(
                df=attrs_dataset,
                meta_cols=META,
                target_cols=GENERAL_ATTRIBUTES + madden_attrs,
                season_range=(2010, find_year_for_season())
            )
            attrs_dataset = pd.concat([attrs_dataset[attrs_dataset.season < 2010].copy(), attrs_2010_latest])

            # Impute attributes for 2001–latest
            attrs_dataset = self.impute_by_last_season_bin(
                df=attrs_dataset,
                meta_cols=META,
                target_cols=GENERAL_ATTRIBUTES + madden_attrs,
                season_range=(2001, find_year_for_season())
            )

            # Merge back style-related fields
            dataset = pd.merge(attrs_dataset, arch_split, on=['madden_id', 'season','player_id','position_group','fullname'])

            # Impute style-related attributes for 2020–latest
            style_2020_latest = self.impute_by_last_season_bin(
                df=dataset,
                meta_cols=META,
                target_cols=GENERAL_ATTRIBUTES + list(MADDEN_ATTRIBUTE_MAP.keys()),
                season_range=(2020, find_year_for_season())
            )
            dataset = pd.concat([dataset[dataset.season < 2020].copy(), style_2020_latest])

            # Final imputation for full 2001–latest range
            dataset = self.impute_by_last_season_bin(
                df=dataset,
                meta_cols=META,
                target_cols=GENERAL_ATTRIBUTES + list(MADDEN_ATTRIBUTE_MAP.keys()),
                season_range=(2001, find_year_for_season())
            )

            # Round categorical integer-like columns
            for col in ["archetype", "runningstyle"]:
                if col in dataset.columns:
                    dataset[col] = dataset[col].round().astype("Int64")

            # Add to master list
            all_groups.append(dataset)

        # Step 4: Return all groups combined
        combined_df = pd.concat(all_groups, ignore_index=True)

        combined_df['season'] = combined_df['season'].astype(int)
        combined_df['team'] = combined_df['team'].astype(int)

        base_meta = self.base_ratings[[
            'player_id',
            'season',
            'fullname',
            'team',
            'high_pos_group',
            'position_group',
            'position',
            'jerseynumber',
            #'yearspro',
            'birthdate',
            #'age',
            'madden_id',
            'pfr_id'
        ]].copy()
        combined_df = pd.merge(combined_df, base_meta, on=['madden_id','season', 'player_id','team','fullname','position_group'], how='left')
        team_mapper_inverse = {v: k for k, v in TEAM_MAPPER.items()}
        # Map numeric codes back to team abbreviations
        combined_df['team'] = combined_df['team'].map(team_mapper_inverse)
        out_cols = ['player_id','madden_id','pfr_id','fullname','high_pos_group','position_group', 'position', 'season', 'team', 'last_season_av'] + list(MADDEN_ATTRIBUTE_MAP.keys())
        combined_df = combined_df[out_cols]

        return combined_df

def make_dataset_madden(s):
    frames = {}

    madden_imputation_runner = MaddenImputationRunner()
    dataset = madden_imputation_runner.run()
    for season in dataset.season.unique():
        frame = dataset[dataset.season==season].copy()
        frames[season] = frame
    return frames

