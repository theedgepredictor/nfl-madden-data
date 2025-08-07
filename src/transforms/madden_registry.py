from pathlib import Path

import pandas as pd
from nfl_data_loader.api.sources.players.rosters.rosters import collect_roster
from nfl_data_loader.utils.utils import find_year_for_season
from nfl_data_loader.workflows.transforms.players.player import get_static_players, apply_rookie_av

from rapidfuzz import process, fuzz

# ---------------------------------------------------------------
# NFL season–start lookup  (you already built it earlier)
# ---------------------------------------------------------------
from src.extracts.madden import get_approximate_value
from src.transforms.madden import read_stage_madden_data

NFL_SEASON_OPENERS = {
    2000: "2000-09-03", 2001: "2001-09-09", 2002: "2002-09-05", 2003: "2003-09-04",
    2004: "2004-09-09", 2005: "2005-09-08", 2006: "2006-09-07", 2007: "2007-09-06",
    2008: "2008-09-04", 2009: "2009-09-10", 2010: "2010-09-09", 2011: "2011-09-08",
    2012: "2012-09-05", 2013: "2013-09-05", 2014: "2014-09-04", 2015: "2015-09-10",
    2016: "2016-09-08", 2017: "2017-09-07", 2018: "2018-09-06", 2019: "2019-09-05",
    2020: "2020-09-10", 2021: "2021-09-09", 2022: "2022-09-08", 2023: "2023-09-07",
    2024: "2024-09-05", 2025: "2025-09-04",
}

MADDEN_ATTRIBUTES = [
        #'overallrating',
        # Pace
        'agility',
        'acceleration',
        'speed',
        'stamina',
        # Strength / Fitness / General
        #'importance',
        'strength',
        'toughness',
        'injury',
        'awareness',
        'jumping',
        'trucking',
        'archetype',
        'runningstyle',
        'changeofdirection',
        #'elusiveness',
        'playrecognition',

        # Passing
        'throwpower',
        'throwaccuracyshort',
        'throwaccuracymid',
        'throwaccuracydeep',
        'playaction',
        'throwonrun',
        # Rushing
        'carrying',
        'ballcarriervision',
        'stiffarm',
        'spinmove',
        'jukemove',
        # Receiving
        'catching',
        'shortrouterunning',
        'midrouterunning',
        'deeprouterunning',
        'spectacularcatch',
        'catchintraffic',
        'release',
        # Blocking
        'runblocking',
        'passblocking',
        'impactblocking',
        # Coverage / Defense
        'mancoverage',
        'zonecoverage',
        'tackle',
        'hitpower',
        'press',
        'pursuit',
        # Special Teams
        'kickaccuracy',
        'kickpower',
        'return',
    ]

MADDEN_DIR = (Path(__file__).resolve()          # /project_root/src/my_module.py
              .parents[2]                       # /project_root/
              / "data" / "madden")     # /project_root/data/madden/raw

def read_processed_madden_data(year):
    return pd.read_csv(f'{MADDEN_DIR}/processed/{year}.csv')

def read_missed_madden_data():
    return pd.read_csv(f'{MADDEN_DIR}/missed/missed.csv')
# ---------------------------------------------------------------
# helper → age on season opener
# ---------------------------------------------------------------
def _age_on_season_start(birthdate: pd.Timestamp, season: int):
    if pd.isna(birthdate) or season not in NFL_SEASON_OPENERS:
        return pd.NA
    start = pd.to_datetime(NFL_SEASON_OPENERS[season])
    return int((start - birthdate.normalize()).days // 365)


class MaddenRegistry:
    def __init__(self):
        self.base_ratings = pd.concat([read_stage_madden_data(season) for season in list(range(2001, find_year_for_season() + 1))])
        self.base_ratings = self.base_ratings.sort_values(by=['madden_id', 'season'], ascending=[True, False]).drop_duplicates(subset=['madden_id', 'season'], keep='first').copy()
        self.base_ratings = self.base_ratings[self.base_ratings['fullname'].notna()].copy()
        self.staged_madden_ratings = self.base_ratings[['madden_id', 'season', 'fullname', 'team', 'high_pos_group', 'position_group', 'position', 'jerseynumber', 'yearspro', 'age', 'birthdate', 'overallrating']].copy()
        self.nflverse_player_rosters = pd.concat([collect_roster(season) for season in list(range(2001,find_year_for_season()+1))])
        self.nflverse_player_rosters = self.nflverse_player_rosters.sort_values(by=['player_id', 'season'], ascending=[True, False]).rename(
            columns={
                'birth_date': 'birthdate',
                'jersey_number': 'jerseynumber',
                'name': 'fullname',
                'years_exp': 'yearspro'
            }
        )
        self.player_registry = None
        self.pre_season_registry = None
        self.missed = None

    def apply(self):
        self.apply_birthdate_pool()
        self.apply_age_pool()
        self.apply_madden_uid()

    def mapper(self):
        full_matches = []
        full_unmatches = []
        for season in list(range(find_year_for_season(), 2001 - 1, -1)):
            matches, unmatched, nfl_unmapped = self.fuzzy_match_nflverse_to_madden(season)
            full_matches.append(matches)
            full_unmatches.append(unmatched)
        return pd.concat(full_matches), pd.concat(full_unmatches)

    def define_registry(self):
        self.apply()
        full_matches, full_unmatched = self.mapper()

        registry = full_matches.groupby("player_id")['madden_id'].agg(lambda s: s.mode().iloc[0]).reset_index().drop_duplicates(['madden_id'])
        MANUAL_MAPPER = {
            "DOMANICKDAVIS_o_rush": "00-0021979",
            "ANTHONYSIMMONS_d_lb": "00-0014889",
            # "JESSETUGGLE_d_lb": "00-0021979",
            "SAMCOWART_d_lb": "00-0003563",
            "ROCKYCALMUS_d_lb": "00-0021202",
            "RODNEYWILLIAMS_special_teams": "00-0017923",
            "PACMANJONES_19830930": "00-0023441",
            "KENNORTON_d_lb": "00-0034434",
            "RT#79_o_line": "00-0022896",
        }
        manual_map = pd.Series(MANUAL_MAPPER, name="player_id").reset_index().rename(columns={"index": "madden_id"})
        registry = pd.concat([
            registry,
            manual_map
        ])
        registry_meta = self.nflverse_player_rosters.drop_duplicates(["player_id"])[["player_id", "fullname", "birthdate"]]
        player_registry = registry.merge(registry_meta, how="left", on=["player_id"])
        self.player_registry = player_registry
        pre_season_player_registry_meta = self.nflverse_player_rosters.drop_duplicates(["player_id", 'season'])[["player_id", 'season', 'fullname', 'team', 'high_pos_group', 'position_group', 'position', 'jerseynumber', 'yearspro','birthdate']]
        pre_season_player_registry_meta["age"] = pre_season_player_registry_meta.apply(lambda r: _age_on_season_start(pd.to_datetime(r.birthdate), r.season), axis=1)
        pre_season_registry = pre_season_player_registry_meta.merge(registry, on=['player_id'], how="left")
        pre_season_registry = pd.merge(pre_season_registry, get_static_players(), on='player_id', how='left')

        registries = []
        ### Add AV column to preseason registry
        for season in pre_season_registry.season.unique():
            #pre_season_registry_unmatched = pre_season_registry[pre_season_registry.madden_id.isnull()].copy()
            #pre_season_registry_matched = pre_season_registry[pre_season_registry.madden_id.notnull()].copy()
            df = pre_season_registry[pre_season_registry.season==season].copy()
            df['is_rookie'] = (df['rookie_season'] == season) & (df.yearspro == 0)

            ## ADD (Previous Season) AWARDS, SEASON BASED HIGHLIGHTS HERE

            ### AV Extractor (Previous Season)
            av_df = get_approximate_value(season - 1)[[
                'player_id',
                'approximate_value'
            ]].rename(columns={'player_id': 'pfr_id', 'approximate_value': 'last_season_av'})
            df = pd.merge(df, av_df, on='pfr_id', how='left')

            rookie_approx_value_df = df[df['is_rookie'] == True].copy()
            rookie_approx_value_df.draft_round = rookie_approx_value_df.draft_round.fillna(8)
            rookie_approx_value_df.draft_pick = rookie_approx_value_df.draft_pick.fillna(rookie_approx_value_df.draft_pick.max() + 1)
            rookie_approx_value_df = rookie_approx_value_df.apply(apply_rookie_av, axis=1)

            df = df[df['is_rookie'] == False].copy()
            df = pd.concat([df, rookie_approx_value_df], ignore_index=True).drop_duplicates(subset=['player_id'], keep='first')
            registries.append(df)

        self.pre_season_registry = pd.concat(registries,ignore_index=True)
        self.pre_season_registry = pd.merge(self.pre_season_registry, self.base_ratings[['season', 'fullname', 'team', 'position_group','overallrating']+MADDEN_ATTRIBUTES], on=['season', 'fullname', 'team', 'position_group'], how='left')
        self.missed = pd.merge(full_unmatched.copy().drop(columns=['overallrating','fullname_clean']), self.base_ratings[['season', 'fullname', 'team', 'position_group','overallrating']+MADDEN_ATTRIBUTES], on=['season', 'fullname', 'team', 'position_group'], how='left')

    def _madden_imputer(self):
        pass
        #### for each position_group determine how overallrating is calculated using a simple multivariate linear function. Leverage those weights to fill in / estimate
        #### new attributes that were made in later maddens. We can then apply these to old ratings
        #### Once all attributes are filled in for the dataset we can take the pre_season_registry_matched and look to impute each position_group of the pre_season_registry_unmatched
        #### We can leverage AV as our rule of best fit for estimating a player, can also add in static stats like combine, draft, etc. Once these are all imputed and defined they should be a better set of data to pass to our rating system

    def _compute_new_madden_id(self, row):
        name = str(row['fullname']).strip().upper().replace(' ', '')

        if pd.notna(row.get("birthdate")):
            bday = pd.to_datetime(row["birthdate"]).strftime("%Y%m%d")
            uid_input = f"{name}_{bday}"
        else:
            uid_input = f"{name}_{row['position_group']}"
        return uid_input

    def _fuzzy_match(self, name, pool, threshold):
        if pool.empty:
            return None
        best = process.extractOne(
            query=name,
            choices=pool["fullname_clean"],
            scorer=fuzz.token_sort_ratio
        )
        if best and best[1] >= threshold:
            return pool.loc[pool["fullname_clean"] == best[0]].iloc[0]
        return None

    def apply_madden_uid(self):
        print("Applying new madden id map")
        self.staged_madden_ratings['madden_id'] = self.staged_madden_ratings.apply(self._compute_new_madden_id, axis=1)
        self.base_ratings['madden_id'] = self.base_ratings.apply(self._compute_new_madden_id, axis=1)

    def apply_birthdate_pool(self):
        print("Applying birthdate pool")
        stage = self.staged_madden_ratings.copy()
        birthdate_pool = stage[stage['birthdate'].notna()].copy()
        birthdate_pool['birthdate'] = pd.to_datetime(birthdate_pool['birthdate'], format='mixed')
        birthdate_pool['birthdate'] = birthdate_pool['birthdate'].dt.strftime('%Y-%m-%d')
        # choose the modal (most common) date for each id ---------------
        pool = (
            birthdate_pool.groupby("madden_id")["birthdate"]
                .agg(lambda s: s.mode().iloc[0])  # mode() always sorted ⇒ .iloc[0] OK
        )

        # fill ONLY the NaNs: keep original non-null dates untouched ----
        self.staged_madden_ratings["birthdate"] = (
            self.staged_madden_ratings["birthdate"]
                .fillna(self.staged_madden_ratings["madden_id"].map(pool))
        )

        # if you want uniform string format afterwards:
        self.staged_madden_ratings["birthdate"] = (
            pd.to_datetime(self.staged_madden_ratings["birthdate"])
                .dt.strftime("%Y-%m-%d")
        )

    def apply_age_pool(self):
        print("Applying age pool")
        stage = self.staged_madden_ratings.copy()

        # Step 1: Use birthdate to fill age
        stage["birthdate"] = pd.to_datetime(stage["birthdate"], errors='coerce')
        missing_age_with_birthdate = stage['age'].isna() & stage['birthdate'].notna()

        stage.loc[missing_age_with_birthdate, 'age'] = stage.loc[missing_age_with_birthdate].apply(
            lambda row: _age_on_season_start(row.birthdate, row.season),
            axis=1
        )

        # Step 2: Use known age + season delta
        known_ages = (
            stage[stage['age'].notna()]
                .sort_values('season')
                .groupby('madden_id')[['season', 'age']]
                .first()
                .reset_index()
        )
        age_lookup = {
            row['madden_id']: (row['season'], row['age'])
            for _, row in known_ages.iterrows()
        }

        def infer_age_from_known(row):
            if pd.notna(row['age']):
                return row['age']
            data = age_lookup.get(row['madden_id'])
            if not data:
                return pd.NA
            known_season, known_age = data
            return known_age + (row['season'] - known_season)

        stage['age'] = stage.apply(infer_age_from_known, axis=1)

        # Final clean-up
        stage['age'] = pd.to_numeric(stage['age'], errors='coerce').astype('Int64')
        self.staged_madden_ratings['age'] = stage['age']


    def fuzzy_match_nflverse_to_madden(self, season):
        """

        :param season:
        :return:
        """
        print(f"Fuzzy matching for {season}")

        # ---------- Prep Data ----------
        madden_df = self.staged_madden_ratings.query("season == @season").copy()
        nfl_df = self.nflverse_player_rosters.query("season == @season").copy()

        def clean_name(s):
            return (
                s.str.replace('.', '', regex=False)
                    .str.replace(' II', '', regex=False)
                    .str.replace(' III', '', regex=False)
                    .str.replace(' IV', '', regex=False)
                    .str.strip()
            )

        madden_df["fullname_clean"] = clean_name(madden_df["fullname"])
        nfl_df["fullname_clean"] = clean_name(nfl_df["fullname"])

        matched_rows = []
        unmatched = madden_df.copy()

        # ============================================================
        # 1. Exact match on season + position_group + fullname_clean
        # ============================================================
        exact = madden_df.merge(
            nfl_df,
            on=["season", "position_group", "fullname_clean"],
            how="inner",
            suffixes=("", "_nfl")
        )
        matched_rows.append(exact[["madden_id", "overallrating", "player_id"]])
        unmatched = unmatched[~unmatched["madden_id"].isin(exact["madden_id"])].copy()
        matches = pd.concat(matched_rows).drop_duplicates(["player_id", "madden_id"])
        nfl_unmapped = nfl_df[~nfl_df["player_id"].isin(matches["player_id"])].copy()

        # ============================================================
        # 2. Exact Jersey number + team + season + fullname_clean
        # ============================================================
        jmatch = unmatched[unmatched["jerseynumber"].notna()].copy()
        jmatch["jerseynumber"] = jmatch["jerseynumber"].astype(int).astype(str)
        j_candidates = nfl_unmapped[nfl_unmapped["jerseynumber"].notna()].copy()
        j_candidates["jerseynumber"] = j_candidates["jerseynumber"].astype(int).astype(str)

        jersey_matches = jmatch.merge(
            j_candidates,
            on=["team", "jerseynumber", "season", "fullname_clean"],
            how="inner",
            suffixes=("", "_nfl")
        )
        matched_rows.append(jersey_matches[["madden_id", "overallrating", "player_id"]])
        unmatched = unmatched[~unmatched["madden_id"].isin(jersey_matches["madden_id"])].copy()
        matches = pd.concat(matched_rows).drop_duplicates(["player_id", "madden_id"])
        nfl_unmapped = nfl_df[~nfl_df["player_id"].isin(matches["player_id"])].copy()

        # ============================================================
        # 3. Birthdate + fuzzy name
        # ============================================================
        birth_pool = nfl_unmapped[nfl_unmapped["birthdate"].notna()].drop_duplicates("player_id").copy()
        birth_pool["birthdate"] = pd.to_datetime(birth_pool["birthdate"]).dt.strftime('%Y-%m-%d')
        unmatch_pool = unmatched[unmatched["birthdate"].notna()].copy()
        unmatch_pool["birthdate"] = pd.to_datetime(unmatch_pool["birthdate"]).dt.strftime('%Y-%m-%d')

        bmatch = []
        for bdate, group in unmatch_pool.groupby("birthdate"):
            candidates = birth_pool[birth_pool["birthdate"] == bdate]
            for _, row in group.iterrows():
                match = self._fuzzy_match(row.fullname_clean, candidates, 80)
                if match is not None:
                    bmatch.append({"madden_id": row.madden_id, "overallrating": row.overallrating, "player_id": match.player_id})
        matched_rows.append(pd.DataFrame(bmatch))
        unmatched = unmatched[~unmatched["madden_id"].isin(pd.DataFrame(bmatch)["madden_id"])].copy()
        matches = pd.concat(matched_rows).drop_duplicates(["player_id", "madden_id"])
        nfl_unmapped = nfl_df[~nfl_df["player_id"].isin(matches["player_id"])].copy()

        # ============================================================
        # 4. Age + fuzzy name
        # ============================================================
        age_pool = nfl_unmapped[nfl_unmapped["birthdate"].notna()].copy()
        age_pool["age"] = age_pool.apply(lambda r: _age_on_season_start(pd.to_datetime(r.birthdate), r.season), axis=1)

        amatch = []
        for age, group in unmatched[unmatched["age"].notna()].groupby("age"):
            candidates = age_pool[age_pool["age"] == age]
            for _, row in group.iterrows():
                match = self._fuzzy_match(row.fullname_clean, candidates, 87)
                if match is not None:
                    amatch.append({"madden_id": row.madden_id, "overallrating": row.overallrating, "player_id": match.player_id})
        matched_rows.append(pd.DataFrame(amatch))
        unmatched = unmatched[~unmatched["madden_id"].isin(pd.DataFrame(amatch)["madden_id"])].copy()
        matches = pd.concat(matched_rows).drop_duplicates(["player_id", "madden_id"])
        nfl_unmapped = nfl_df[~nfl_df["player_id"].isin(matches["player_id"])].copy()

        # ============================================================
        # 5. YearsPro + fuzzy name
        # ============================================================
        yp_match = []
        yp_frame = unmatched[unmatched["yearspro"].notna()].copy()
        yp_frame['yearspro'] = yp_frame['yearspro'].astype(int).astype(str)
        if yp_frame.shape[0] !=0:
            for yp, group in yp_frame.groupby("yearspro"):
                candidates = nfl_unmapped[nfl_unmapped["yearspro"] == yp]
                for _, row in group.iterrows():
                    match = self._fuzzy_match(row.fullname_clean, candidates, 80)
                    if match is not None:
                        yp_match.append({"madden_id": row.madden_id, "overallrating": row.overallrating, "player_id": match.player_id})
            yp_df = pd.DataFrame(yp_match)
            if not yp_df.empty:
                matched_rows.append(yp_df)
                unmatched = unmatched[~unmatched["madden_id"].isin(pd.DataFrame(yp_match)["madden_id"])].copy()
                matches = pd.concat(matched_rows).drop_duplicates(["player_id", "madden_id"])
                nfl_unmapped = nfl_df[~nfl_df["player_id"].isin(matches["player_id"])].copy()

        # ============================================================
        # 6. Exact match within same season on fullname_clean (At this point with the values left we can join straight on name most likely)
        # ============================================================
        same_season_match = unmatched.merge(
            nfl_unmapped,
            on=["season", "fullname_clean"],
            how="inner",
            suffixes=("", "_nfl")
        )[["madden_id", "overallrating", "player_id"]]

        matched_rows.append(same_season_match)
        unmatched = unmatched[~unmatched["madden_id"].isin(same_season_match["madden_id"])].copy()
        matches = pd.concat(matched_rows).drop_duplicates(["player_id", "madden_id"])
        nfl_unmapped = nfl_df[~nfl_df["player_id"].isin(matches["player_id"])].copy()

        # ============================================================
        # 7. Final Fuzzy (fallback) - append position group
        # ============================================================
        unmatched['fullname_clean'] = unmatched['fullname_clean'] + " " + unmatched['position_group']
        nfl_unmapped['fullname_clean'] = nfl_unmapped['fullname_clean'] + " " + nfl_unmapped['position_group']
        final_match = []
        for _, row in unmatched.iterrows():
            match = self._fuzzy_match(row.fullname_clean, nfl_unmapped, 70)
            if match is not None:
                final_match.append({"madden_id": row.madden_id, "overallrating": row.overallrating, "player_id": match.player_id})
        matched_rows.append(pd.DataFrame(final_match))
        unmatched = unmatched[~unmatched["madden_id"].isin(pd.DataFrame(final_match)["madden_id"])].copy()

        # ============================================================
        # Final outputs
        # ============================================================
        matches = pd.concat(matched_rows).drop_duplicates(["player_id", "madden_id"])
        nfl_unmapped = nfl_df[~nfl_df["player_id"].isin(matches["player_id"])].copy()
        print(f"Successfully mapped {matches.madden_id.nunique()} madden players; {unmatched.madden_id.nunique()} unmatched.")

        return matches, unmatched, nfl_unmapped

def make_processed_madden(load_seasons):
    frames = {}
    madden_registry = MaddenRegistry()
    madden_registry.define_registry()
    full_unmatched = madden_registry.missed
    #player_registry = madden_registry.player_registry
    pre_season_registry = madden_registry.pre_season_registry
    full_unmatched.to_csv(f'{MADDEN_DIR}/missed/missed.csv', index=False)
    for season in pre_season_registry.season.unique():
        frame = pre_season_registry[pre_season_registry.season==season].copy()
        frames[season] = frame
    return frames


if __name__ == '__main__':
    #### Rerun w base madden being joined correctly with the new madden_ids right now no madden stats are added
    #### also need to make sure we add madden stats the the missing category madden stats too
    a = make_processed_madden(list(range(2001, 2026)))