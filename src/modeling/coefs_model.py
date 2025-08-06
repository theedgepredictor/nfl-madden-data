import pandas as pd
import numpy as np
from sklearn.linear_model import Ridge


class MaddenCategoryAdjuster:
    def __init__(self, category_map, av_col="last_season_av", ovr_col="overallrating", scale=2.0):
        """
        category_map: dict of {category_name: [list_of_attributes]}
        av_col: column name for last season's AV
        ovr_col: column name for overall rating
        scale: how strongly AV z-score influences redistribution
        """
        self.category_map = category_map
        self.av_col = av_col
        self.ovr_col = ovr_col
        self.scale = scale
        self.position_attr_weights = {}  # {pos: {category: {attr: weight}}}
        self.position_cat_importance = {}  # {pos: Series of category importance}

    def _fit_category_attr_weights(self, df, alpha=1.0):
        """Fit within-category attribute weights for a position group."""
        cat_weights = {}
        for cat, attrs in self.category_map.items():
            X = df[attrs]
            y = df[self.ovr_col]
            model = Ridge(alpha=alpha)
            model.fit(X, y)
            weights = pd.Series(model.coef_, index=attrs).abs()
            weights = weights / weights.sum()
            cat_weights[cat] = weights.to_dict()
        return cat_weights

    def _fit_category_importance(self, df, cat_weights, alpha=1.0):
        """Fit category-to-OVR model for a position group."""
        category_features = pd.DataFrame(index=df.index)
        for cat, attrs in self.category_map.items():
            weights = pd.Series(cat_weights[cat])
            category_features[cat] = (df[attrs] * weights).sum(axis=1)

        model = Ridge(alpha=alpha)
        model.fit(category_features, df[self.ovr_col])
        importance = pd.Series(model.coef_, index=category_features.columns)
        importance = importance / importance.abs().sum()
        return importance, category_features

    def _adjust_position_group(self, df, cat_weights, cat_importance):
        """Apply bounded redistribution for one position group with diff tracking."""
        df = df.copy()
        before_df = df.copy()

        # Compute category scores before adjustment
        category_scores_before = pd.DataFrame(index=df.index)
        for cat, attrs in self.category_map.items():
            weights = pd.Series(cat_weights[cat])
            category_scores_before[cat] = (df[attrs] * weights).sum(axis=1)

        # Compute AV z-score
        df['av_z'] = (df[self.av_col] - df[self.av_col].mean()) / df[self.av_col].std()

        # Adjust category totals
        category_scores_after = category_scores_before.copy()
        for cat in self.category_map.keys():
            total_cat_points = category_scores_before[cat].sum()
            weights = category_scores_before[cat] + (df['av_z'] * self.scale * cat_importance[cat])
            weights = weights - weights.min() + 1e-6
            category_scores_after[cat] = total_cat_points * (weights / weights.sum())

        # Redistribute category totals back into attributes
        for cat, attrs in self.category_map.items():
            weights = pd.Series(cat_weights[cat])
            for attr in attrs:
                df[attr] = category_scores_after[cat] * weights[attr]

        df.drop(columns=['av_z'], inplace=True)

        # Track differences
        diff_df = df[self.ovr_col].to_frame(name=f"{self.ovr_col}_after")
        for attr_list in self.category_map.values():
            for attr in attr_list:
                diff_df[f"{attr}_diff"] = df[attr] - before_df[attr]
        for cat in self.category_map.keys():
            diff_df[f"{cat}_diff"] = category_scores_after[cat] - category_scores_before[cat]

        return df, diff_df

    def run(self, df, pos_col="position_group"):
        """Run full pipeline for all position groups and return adjusted df + diff tracking."""
        adjusted_dfs = []
        diff_dfs = []

        for pos, pos_df in df.groupby(pos_col):
            # Skip if not enough players
            if len(pos_df) < 2:
                continue

            # 1. Fit within-category weights
            cat_weights = self._fit_category_attr_weights(pos_df)
            self.position_attr_weights[pos] = cat_weights

            # 2. Fit category importance
            cat_importance, _ = self._fit_category_importance(pos_df, cat_weights)
            self.position_cat_importance[pos] = cat_importance

            # 3. Adjust ratings for this position
            adjusted_pos_df, diff_df = self._adjust_position_group(pos_df, cat_weights, cat_importance)

            adjusted_dfs.append(adjusted_pos_df)
            diff_dfs.append(diff_df)

        adjusted_main = pd.concat(adjusted_dfs).sort_index()
        diff_main = pd.concat(diff_dfs).sort_index()

        return adjusted_main, diff_main