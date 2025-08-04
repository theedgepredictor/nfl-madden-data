import calendar
import re
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from src.consts import HIGH_POSITION_MAPPER, POSITION_MAPPER
from src.extracts.player_stats import collect_roster, collect_players
from src.transforms.madden_registry import MaddenRegistry
from src.utils import name_filter, find_year_for_season
import os

# ---------- 1.  Month lookup (names & abbreviations) -----------------------
MONTH_TO_NUM = {m.lower(): i for i, m in enumerate(calendar.month_name) if m}
MONTH_TO_NUM.update({m.lower(): i for i, m in enumerate(calendar.month_abbr) if m})
# e.g. "july" → 7, "jul" → 7

def _month_to_int(m) -> int | None:
    """
    Return 1-12 or None.
    Accepts:
      • "July" / "jul" / "JUL"
      • 3          (int)
      • "3" / "03" (str)
      • 3.0000     (float)
    """
    if pd.isna(m):
        return None
    # 1-a  string names / abbreviations
    key = str(m).strip().lower()
    if key in MONTH_TO_NUM:
        return MONTH_TO_NUM[key]
    # 1-b  digits: "3" / "03" / 3 / 3.0000
    try:
        num = int(float(key))        # handles "3.0000" too
        if 1 <= num <= 12:
            return num
    except ValueError:
        pass
    return None

MADDEN_DIR = (Path(__file__).resolve()          # /project_root/src/my_module.py
              .parents[2]                       # /project_root/
              / "data" / "madden")     # /project_root/data/madden/raw

def read_stage_madden_data(year):
    return pd.read_csv(f'{MADDEN_DIR}/stage/{year}.csv')

def read_raw_madden_data(year):
    df = pd.read_csv(f'{MADDEN_DIR}/raw/{year}.csv')

    if year == 2024:
        df = df.drop(columns=[i for i in df.columns if '/diff' in i] + ['Overall'])
        df.columns = [i.split('/')[1] if '/value' in i else i for i in df.columns]

    if year == 2025:
        df = df.drop(columns=[
            'overall'
        ])

    df.columns = [_madden_column_normalizer(i) for i in df.columns]

    # Handle throw accuracy columns
    if 'throwaccuracy' in df.columns:
        if 'throwaccuracyshort' in df.columns:
            df['throwaccuracyshort'] = df['throwaccuracyshort'].fillna(df['throwaccuracy'])
        else:
            df['throwaccuracyshort'] = df['throwaccuracy']

        if 'throwaccuracymid' in df.columns:
            df['throwaccuracymid'] = df['throwaccuracymid'].fillna(df['throwaccuracy'])
        else:
            df['throwaccuracymid'] = df['throwaccuracy']

        if 'throwaccuracydeep' in df.columns:
            df['throwaccuracydeep'] = df['throwaccuracydeep'].fillna(df['throwaccuracy'])
        else:
            df['throwaccuracydeep'] = df['throwaccuracy']

    # Handle route running columns
    if 'routerunning' in df.columns:
        if 'shortrouterunning' in df.columns:
            df['shortrouterunning'] = df['shortrouterunning'].fillna(df['routerunning'])
        else:
            df['shortrouterunning'] = df['routerunning']

        if 'midrouterunning' in df.columns:
            df['midrouterunning'] = df['midrouterunning'].fillna(df['routerunning'])
        else:
            df['midrouterunning'] = df['routerunning']

        if 'deeprouterunning' in df.columns:
            df['deeprouterunning'] = df['deeprouterunning'].fillna(df['routerunning'])
        else:
            df['deeprouterunning'] = df['routerunning']

    if 'season' not in df.columns:
        df['season'] = year

    if 'firstname' in df.columns:
        df['fullname'] = df['firstname'] + ' ' + df['lastname']
        df = df.drop(columns=['firstname', 'lastname'])

    if 'first' in df.columns:
        df['fullname'] = df['first'] + ' ' + df['last']
        df = df.drop(columns=['first', 'last'])

    if 'overall\nrating' in df.columns:
        df['overallrating'] = df['overallrating'].fillna(df['overall\nrating'])
        df = df.drop(columns=['overall\nrating'])

    if 'birthday' in df.columns:
        def _row_to_date(r):
            d_raw, m_raw, y_raw = r['birthday'], r['birthmonth'], r['birthyear']

            if pd.isna(d_raw) or pd.isna(m_raw) or pd.isna(y_raw):
                return pd.NaT

            try:
                day = int(float(d_raw))  # 23.000 → 23
                year = int(float(y_raw))  # 1976.000 → 1976
                month = _month_to_int(m_raw)
                if month is None:
                    return pd.NaT
                return pd.Timestamp(year=year, month=month, day=day)
            except Exception:
                return pd.NaT
        df['birthdate'] = df.apply(_row_to_date, axis=1)
        df = df.drop(columns=['birthyear', 'birthmonth', 'birthday'])


    if 'birthdate' in df.columns:
        def _century_fix(yy: int) -> int:
            return 2000 + yy if yy < 35 else 1900 + yy
        def _safe_ts(y, m, d):
            try:
                return pd.Timestamp(year=y, month=m, day=d)
            except ValueError:
                return pd.NaT

        def _parse_cell(x):
            if pd.isna(x):
                return pd.NaT

            # --- digits patterns -------------------------------------
            x_str = str(x).strip()
            if x_str.isdigit():
                n = len(x_str)

                if n == 6:  # MMDDYY
                    mm, dd, yy = int(x_str[:2]), int(x_str[2:4]), int(x_str[4:])
                    return _safe_ts(_century_fix(yy), mm, dd)

                if n == 5:  # MDDYY
                    mm, dd, yy = int(x_str[0]), int(x_str[1:3]), int(x_str[3:])
                    return _safe_ts(_century_fix(yy), mm, dd)

                if n == 4:  # MDYY  ← NEW branch
                    mm, dd, yy = int(x_str[0]), int(x_str[1]), int(x_str[2:])
                    return _safe_ts(_century_fix(yy), mm, dd)

            # --- fast path: already a date-like string ----------------
            try:
                ts = pd.to_datetime(x, errors="raise")
                return ts.normalize()
            except Exception:
                return pd.NaT

        if year in [2018, 2019]: # 32589 like datetimes
            df['birthdate'] = df['birthdate'].apply(_parse_cell)
        elif year in [2021, 2023]:
            df['birthdate'] = pd.NaT ## Unparseable datetime...
        else:
            df['birthdate'] = pd.to_datetime(df['birthdate'], format='mixed')


    df['team'] = df['team'].str.replace("_", " ").str.strip()
    df['team'] = df['team'].str.strip().str.split(' ').str[-1]

    team_mapper = {
        'Bucs': 'Buccaneers',
        'Team': 'Commanders',
        'Redskins': 'Commanders',
        'Agents': 'FREEAGENT',
        'Agent': 'FREEAGENT',
        'free agents': 'FREEAGENT',

    }
    df['team'] = df.team.str.title().replace(team_mapper)

    nflverse_team_mapping = {
        'Bears': 'CHI', 'Bengals': 'CIN', 'Bills': 'BUF', 'Broncos': 'DEN', 'Browns': 'CLE',
        'Buccaneers': 'TB', 'Cardinals': 'ARI', 'Chargers': 'LAC', 'Chiefs': 'KC', 'Colts': 'IND',
        'Cowboys': 'DAL', 'Dolphins': 'MIA', 'Eagles': 'PHI', 'Falcons': 'ATL', '49Ers': 'SF',
        'Giants': 'NYG', 'Jaguars': 'JAX', 'Jets': 'NYJ', 'Lions': 'DET', 'Packers': 'GB',
        'Panthers': 'CAR', 'Patriots': 'NE', 'Raiders': 'LV', 'Rams': 'LA', 'Ravens': 'BAL',
        'Commanders': 'WAS', 'Saints': 'NO', 'Seahawks': 'SEA', 'Steelers': 'PIT', 'Texans': 'HOU',
        'Titans': 'TEN', 'Vikings': 'MIN','Jagaurs':'JAX'
    }
    df['team'] = df.team.replace(nflverse_team_mapping)

    df['position_group'] = df.position
    df.position_group = df.position_group.map(POSITION_MAPPER)

    df['high_pos_group'] = df.position_group
    df.high_pos_group = df.high_pos_group.map(HIGH_POSITION_MAPPER)

    df = df.replace('n/a', 0)
    # DE that should be OLB (normalizes start of career to end of career position)
    df.loc[df.fullname == 'John Abraham', 'position_group'] = 'd_field'
    df.loc[df.fullname == 'Chike Okeafor', 'position_group'] = 'd_field'
    df.loc[((df.fullname == 'Bertrand Berry')), 'position_group'] = 'd_field'
    df.loc[((df.fullname == 'Fred Wakefield')), 'position_group'] = 'o_line'
    df.loc[((df.fullname == 'Will Overstreet')), 'position_group'] = 'd_field'
    df.loc[((df.fullname == 'R-Kal Truluck')), 'position_group'] = 'd_field'
    df.loc[((df.fullname == 'Eric Ogbogu')), 'position_group'] = 'd_field'
    df.loc[((df.fullname == 'Adalius Thomas')), 'position_group'] = 'd_field'
    df.loc[((df.fullname == 'Terrell Suggs')), 'position_group'] = 'd_field'
    df.loc[((df.fullname == 'Jason Gildon')), 'position_group'] = 'd_field'
    df.loc[((df.fullname == 'Robert Mathis')), 'position_group'] = 'd_field'
    df.loc[((df.fullname == 'Shawne Merriman')), 'position_group'] = 'd_line'
    df.loc[((df.fullname == 'Jason Babin')), 'position_group'] = 'd_line'
    df.loc[((df.fullname == 'Kenard Lang')), 'position_group'] = 'd_line'
    df.loc[((df.fullname == 'OJ Atogwe')), 'position_group'] = 'd_line'
    df.loc[((df.fullname == 'Damane Duckett')), 'position_group'] = 'd_field'
    df.loc[((df.fullname == 'Brock Lesnar')), 'position_group'] = 'd_line'
    df.loc[((df.fullname == 'Antwan Peek')), 'position_group'] = 'd_field'
    df.loc[df.fullname == 'Dan Klecko', 'position_group'] = 'o_rush'
    df.loc[df.fullname == 'Tamba Hali', 'position_group'] = 'd_field'
    df.loc[df.fullname == 'Mike Sellers', 'position_group'] = 'o_pass'
    df.loc[df.fullname == 'Ben Utecht', 'position_group'] = 'o_pass'
    df.loc[df.fullname == 'Casey Fitzsimmons', 'position_group'] = 'o_pass'
    df.loc[df.fullname == 'Brad Cieslak', 'position_group'] = 'o_pass'
    df.loc[df.fullname == 'Mathias Kiwanuka', 'position_group'] = 'd_line'
    df.loc[df.fullname == 'Quentin Moses', 'position_group'] = 'd_field'
    df.loc[df.fullname == 'Patrick Chukwurah', 'position_group'] = 'd_field'
    df.loc[df.fullname == 'Ikaika Alama-Francis', 'position_group'] = 'd_field'
    df.loc[df.fullname == 'Rob Ninkovich', 'position_group'] = 'd_field'
    df.loc[df.fullname == 'Akbar Gbaja Biamila', 'position_group'] = 'd_line'
    df.loc[df.fullname == 'B.J. Sams', 'position_group'] = 'o_rush'

    df.loc[((df.fullname == 'Jim Kleinsasser')), 'fullname'] = 'Jimmy Kleinsasser'
    df.loc[((df.fullname == 'Jimmy Kleinsasser')), 'position_group'] = 'o_pass'
    df.loc[((df.fullname == 'Jimmy Kleinsasser')), 'position'] = 'TE'
    df.loc[((df.fullname == 'Eric Beverly')), 'position_group'] = 'o_pass'
    df.loc[((df.fullname == 'Michael Moore')), 'position_group'] = 'o_pass'

    name_fixes = {
        'Justin Madubuike': 'Nnamdi Madubuike',
        'R.Webb': 'Richmond Webb',
        'Ndukwe Kalu': 'ND Kalu',
        'B.Cox': 'Byron Cox',
        'H.Ford': 'Henry Ford',
        'W.Walls': 'Wesley Walls',
        'B.Stai': 'Brenden Stai',
        'T.Fair': 'Terry Fair',
        'D.Greer': 'Donovan Greer',
        'G.Crowell': 'Germane Crowell',
        'G.Brown': 'Gilbert Brown',
        'T.Mathis': 'Terance Mathis',
        'K.Lyle': 'Keith Lyle',
        'I.Byrd': 'Isaac Byrd',
        'R.Mealey': 'Rondell Mealey',
        'A.Dorsett': 'Anthony Dorsett',
        'K.Office': 'Kendrick Office',
        'C.Peter': 'Christian Peter',
        'J.Boyd': 'James Boyd',  ##
        # 'RTate': 'Ray Tate',
        # 'JRomero': 'Jesse Romero',
        'D.Patmon': 'DeWayne Patmon',
        'T.Sawyer': 'Talance Sawyer',
        'R.Bean': 'Robert Bean',
        'M.Fulcher': 'Mondriel Fulcher',
        'O.J. Atogwe': 'Oshiomogho Atogwe',
        'Michael Jennings': 'Mike Jennings',
        'Akbar Gbaja Biamila': 'Akbar Gbaja-Biamila',
    }
    df['fullname'] = df['fullname'].replace(name_fixes)

    df['madden_id'] = df['fullname'].apply(name_filter) + '_' + df['position_group'].astype(str)
    df['madden_id'] = df['madden_id'].str.upper()

    madden_cols = [
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
        # Meta
        'jerseynumber',
        'yearspro',
        'age',
        'birthdate',
    ]
    for col in madden_cols:
        if col not in df.columns:
            df[col] = None

    df.jerseynumber = df.jerseynumber.astype(str)
    df.loc[df.jerseynumber.notnull(), 'jerseynumber'] = df.loc[df.jerseynumber.notnull(), 'jerseynumber'].astype(str).str.replace('#','')

    df = df[
    [
        'madden_id',
        'team',
        'season',
        'fullname',
        'high_pos_group',
        'position_group',
        'position',
        'overallrating',
    ] + madden_cols
    ]
    return df


def _madden_column_normalizer(col):
    new_col = col.lower().replace(' ', '')
    new_col = new_col.replace(' ', '')
    new_col = new_col.replace('plyr_', '')
    new_col = new_col.replace('_', '')
    new_col = new_col.replace('throwontherun', 'throwonrun')
    # Throw Accuracy Short
    new_col = new_col.replace('shortthrowaccuracy', 'throwaccuracyshort')
    new_col = new_col.replace('throwaccshort', 'throwaccuracyshort')
    new_col = new_col.replace('shortaccuracy', 'throwaccuracyshort')
    # Throw Accuracy Mid
    new_col = new_col.replace('midthrowaccuracy', 'throwaccuracymid')
    new_col = new_col.replace('throwaccmid', 'throwaccuracymid')
    new_col = new_col.replace('throwaccuracymiddle', 'throwaccuracymid')
    new_col = new_col.replace('mediumthrowaccuracy', 'throwaccuracymid')
    new_col = new_col.replace('middleaccuracy', 'throwaccuracymid')
    new_col = new_col.replace('throwaccuracymedium', 'throwaccuracymid')
    # Throw Accuracy Deep
    new_col = new_col.replace('deepthrowaccuracy', 'throwaccuracydeep')
    new_col = new_col.replace('throwaccdeep', 'throwaccuracydeep')
    new_col = new_col.replace('deepthrowaccruacy', 'throwaccuracydeep')
    new_col = new_col.replace('deepaccuracy', 'throwaccuracydeep')
    # Short Route Running
    new_col = new_col.replace('shortrouteruning', 'shortrouterunning')
    new_col = new_col.replace('shortrr', 'shortrouterunning')
    # Medium Route Running
    new_col = new_col.replace('mediumrr', 'midrouterunning')
    # Deep Route Running
    new_col = new_col.replace('deeprr', 'deeprouterunning')
    new_col = new_col.replace('speccatch', 'spectacularcatch')
    new_col = new_col.replace('spectactularcatch', 'spectacularcatch')
    new_col = new_col.replace('stength', 'strength')
    # new_col = new_col.replace('overall\nrating', 'overallrating')
    if new_col == 'overall':
        new_col = new_col.replace('overall', 'overallrating')
    if new_col == 'ovr':
        new_col = new_col.replace('ovr', 'overallrating')
    if new_col == 'mancover':
        new_col = new_col.replace('mancover', 'mancoverage')
    if new_col == 'zonecover':
        new_col = new_col.replace('zonecover', 'zonecoverage')
    if new_col == 'changeofdir':
        new_col = new_col.replace('changeofdir', 'changeofdirection')
    new_col = new_col.replace('accleration', 'acceleration')
    if new_col == 'runstyle':
        new_col = new_col.replace('runstyle', 'runningstyle')
    if new_col == 'handedness':
        new_col = new_col.replace('handedness', 'playerhandedness')
    if new_col == 'playerhandness':
        new_col = new_col.replace('playerhandness', 'playerhandedness')
    if new_col == 'handed':
        new_col = new_col.replace('handed', 'playerhandedness')
    if 'position' in new_col or 'pos' == new_col:
        new_col = new_col.replace(new_col, 'position')
    if 'jers' in new_col:
        new_col = new_col.replace(new_col, 'jerseynumber')
    if new_col == 'number':
        new_col = new_col.replace(new_col, 'jerseynumber')
    if new_col == 'impactblock':
        new_col = new_col.replace('impactblock', 'impactblocking')
    if new_col == 'runblock':
        new_col = new_col.replace('runblock', 'runblocking')
    if new_col == 'passblock':
        new_col = new_col.replace('passblock', 'passblocking')
    if new_col == 'leadblock':
        new_col = new_col.replace('leadblock', 'leadblocking')
    if new_col == 'catch':
        new_col = new_col.replace('catch', 'catching')
    if new_col == 'name':
        new_col = new_col.replace('name', 'fullname')
    if new_col == 'powermove':
        new_col = new_col.replace('powermove', 'powermoves')
    if new_col == 'finnessemoves':
        new_col = new_col.replace('finnessemoves', 'finessemoves')
    if new_col == 'finesseemove':
        new_col = new_col.replace('finesseemove', 'finessemoves')
    new_col = new_col.replace('dratfpick', 'draftpick')
    new_col = new_col.replace('bcvision', 'ballcarriervision')
    new_col = new_col.replace('kickreturn', 'return')
    if new_col == 'experience':
        new_col = new_col.replace('experience', 'yearspro')
    if new_col == 'teamname':
        new_col = new_col.replace('teamname', 'team')
    if new_col == 'jersey#':
        new_col = new_col.replace('jersey#', 'jerseynumber')
    new_col = new_col.replace(' ', '')



    return new_col


from rapidfuzz import process, fuzz
def fuzzy_match_names(row, player_df, threshold=70):
    # Filter Madden players by team, position_group, and season first
    filtered_players = player_df[
        (player_df['position_group'] == row['position_group']) &
        (player_df['season'] == row['season'])
        ]

    # If there's no match based on position_group and season, return None
    if filtered_players.empty:
        return None

    # Use rapidfuzz's process to get the best match for fullname
    best_match = process.extractOne(row['fullname'], filtered_players['fullname'], scorer=fuzz.ratio)

    # Only return the match if it meets the threshold
    if best_match and best_match[1] >= threshold:
        return best_match[0]
    else:
        return None


def join_with_fuzzy_matching(madden_df, player_df):
    # 1. Perform an exact match first based on season, fullname, and position_group
    exact_match = pd.merge(madden_df, player_df, how='left', on=['season', 'fullname', 'position_group'], suffixes=('', '_player'))

    # 1.b. Perform an exact match first based on season and fullname and drop duplicates after concatenation of 1.
    sub_exact_match = pd.merge(madden_df, player_df.drop(columns=['position_group']), how='left', on=['season', 'fullname'], suffixes=('', '_player'))

    exact_match = pd.concat([exact_match, sub_exact_match]).drop_duplicates(subset=['season', 'player_id'], keep='first')

    matched = exact_match[~exact_match['player_id'].isna()].copy()  # Rows where an exact match was found

    unmatched = madden_df[~madden_df['madden_id'].isin(matched['madden_id'])].copy()  # Rows where no match was found in player dataframe
    unmatched['player_id'] = np.nan
    unmatched['status_abbr'] = np.nan

    # 3. Apply fuzzy matching only on the unmatched rows
    if not unmatched.empty:
        unmatched['matched_fullname'] = unmatched.apply(fuzzy_match_names, axis=1, player_df=player_df)

        # 4. Perform another merge with the fuzzy-matched fullnames
        fuzzy_match = pd.merge(unmatched, player_df, how='left', left_on=['position_group', 'season', 'matched_fullname'],
                               right_on=['position_group', 'season', 'fullname'], suffixes=('', '_player'))
        # fuzzy_match['overallrating'] = fuzzy_match['overallrating'].fillna(fuzzy_match['overallrating_player'])
        fuzzy_match['player_id'] = fuzzy_match['player_id'].fillna(fuzzy_match['player_id_player'])

        # 5. Concatenate the exact matches and the fuzzy matches
        final_df = pd.concat([matched, fuzzy_match]).reset_index(drop=True)
    else:
        final_df = exact_match.copy()

    return final_df


def interpolate_column(group, col):
    # Sort by season to ensure proper interpolation/extrapolation

    # Fill missing values by interpolating
    group[col] = group[col].interpolate(method='linear', limit_direction='both')

    return group


def impute_madden_ratings_for_all_columns(final_df, columns_to_impute):
    # Apply the interpolation and extrapolation for each player and each column
    for col in columns_to_impute:
        final_df = final_df.groupby('player_id', group_keys=False).apply(lambda x: interpolate_column(x, col))
    return final_df

def stage_madden_season_data(season):
    """
    Clean madden data and normalize madden columns into standardized format

    :param season:
    :return:
    """

    madden_df = read_raw_madden_data(season)
    return madden_df

def _stage_validation_set(frames):
    team_counts = []
    column_counts = []
    for season, frame in frames.items():
        team_counts.extend(frame.team.values)
        column_counts.extend(frame.columns)
    column_counts = pd.Series(column_counts).value_counts().to_dict()
    for i,j in column_counts.items():
        if j != len(frames.items()):
            raise Exception(f"Stage Validation failed on column count mis match: {column_counts}")
    team_counts = pd.Series(team_counts).value_counts().to_dict()
    if len(team_counts.items()) != 33:
        raise Exception(f"Stage Validation failed on team count mismatch: {column_counts}")



def make_stage_madden(load_seasons):
    frames = {}
    column_list = []
    for season in load_seasons:
        frame = stage_madden_season_data(season)
        frames[season] = frame
        column_list.extend(frame.team.values)
    madden_counts = pd.Series(column_list).value_counts().to_dict()
    _stage_validation_set(frames)
    return frames




def normalize_madden_data(season, debug=False):
    """
    Collect all players from nflverse, normalize madden attributes for the given year and fuzzy match
    madden names to nflverse names
    :param season:
    :param debug:
    :return:
    """
    print("Running for season {}".format(season))
    players_df = collect_players()
    players_df['season'] = season
    # Team Frame for specific cols for merge
    base_players_df = players_df[['player_id', 'season', 'position_group', 'name', 'status_abbr']].copy().rename(columns={'name': 'fullname'})
    base_players_df['fullname'] = base_players_df['fullname'].str.replace('.', '').str.replace(' II', '').str.replace(' III', '').str.replace(' IV', '')

    madden_df = read_raw_madden_data(season)

    # Madden Join Frame for specific cols
    madden_join_df = madden_df[[
        'madden_id',
        'team',
        'season',
        'fullname',
        'position_group',
        'overallrating',
    ]].copy()
    madden_join_df['fullname'] = madden_join_df['fullname'].str.replace('.', '').str.replace(' II', '').str.replace(' III', '').str.replace(' IV', '')
    wth = madden_join_df[madden_join_df.fullname.str.contains('#')].copy()
    if wth.shape[0] != 0:
        wth['fullname'] = wth['fullname'].astype(str)
        wth['fullname'] = wth['fullname'].str.extract('(\d+)')
        wth = wth.rename(columns={"fullname":"jersey_number"})
        for _, row in wth.iterrows():

            ### Finish this out for fullname (jersey number) and team to determine actual name and add back into raw madden
            roster_check = collect_roster(season)
            a = roster_check[((roster_check.jersey_number==row['jersey_number'])&(roster_check.position_group==row['position_group'])&(roster_check.team == row['team']))]
            print(a)

    final_df = join_with_fuzzy_matching(madden_join_df.copy(), base_players_df.copy())
    final_df = final_df[['player_id', 'season', 'madden_id', 'overallrating', 'position_group']].copy()
    final_df = final_df[final_df.player_id.notnull()].copy()
    a = final_df.merge(madden_join_df[['madden_id', 'team', 'fullname']].rename(columns={'fullname': 'madden_name'}), how='left', on=['madden_id'])
    b = a.merge(players_df[['player_id', 'name']], how='left', on=['player_id'])
    b = b.drop_duplicates(['player_id', 'season'], keep='first').drop_duplicates(['madden_id', 'team', 'season'], keep='first')
    if debug:
        return b, madden_join_df, players_df
    madden_source_ids = list(set(madden_join_df.madden_id))

    joined_madden_ids = list(set(b.madden_id))
    missed_madden_join_df = madden_join_df[~madden_join_df.madden_id.isin(joined_madden_ids)]
    print(f"-- Total Madden IDs: {len(madden_source_ids)}")
    print(f"-- Missing Madden IDs: {len(missed_madden_join_df)}")

    final_merge_cols = [
        'player_id',
        'madden_id',
        'season',
        'overallrating',
        'position_group',
    ]
    final_df = b[final_merge_cols]
    # Join columns from madden
    madden_cols = [
        # Pace
        'agility',
        'acceleration',
        'speed',
        'stamina',
        # Strength / Fitness
        'strength',
        'toughness',
        'injury',
        'awareness',
        'jumping',
        'trucking',
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
    for col in madden_cols:
        if col not in madden_df.columns:
            madden_df[col] = None
    final_df = pd.merge(final_df, madden_df[['season', 'madden_id'] + madden_cols], how='left', on=['madden_id', 'season'])
    return final_df, madden_join_df[~madden_join_df.madden_id.isin(joined_madden_ids)].copy()



def make_processed_madden(load_seasons):
    frames = {}
    madden_registry = MaddenRegistry()
    madden_registry.define_registry()
    full_unmatched = madden_registry.missed
    #player_registry = madden_registry.player_registry
    pre_season_registry = madden_registry.pre_season_registry

    for season in pre_season_registry.season.unique():
        frame = pre_season_registry[pre_season_registry.season==season].copy()
        frames[season] = frame
    return frames


if __name__ == '__main__':
    a = make_processed_madden(list(range(2001, 2026)))
