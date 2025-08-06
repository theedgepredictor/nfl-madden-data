"""
Streamlit Madden Data Cleaner
================================
Interactive utility for quickly assessing player‐rating coverage in the Madden dataset.
Streamlit Madden Data Cleaner

Features
--------
* **Season ↠ Team** selector in the sidebar.
* Three main tabs:
  1. **Roster** – split by *position_group* (each its own sub‑tab) and sorted by **overallrating**.
  2. **Team Stats** – high‑level summary for the selected team.
  3. **Season Stats** – coverage / averages per team for the whole season, including a pivot of average rating by position group.

Folder layout assumed:
```
project_root/
├─ data/madden/raw/2024.csv
└─ src/streamlit_madden_app.py   ← this file
```
Run with `streamlit run streamlit_madden_app.py`.
"""
from pathlib import Path
from typing import List

import pandas as pd
import streamlit as st

###############################################################################
# Configuration
###############################################################################
from pathlib import Path
from typing import List
import pandas as pd
import streamlit as st
from src.consts import HIGH_POSITION_MAPPER, POSITION_MAPPER
from src.modeling.imputer import read_madden_dataset
from src.modeling.consts import CATEGORY_MAP
from src.extracts.player_stats import collect_roster

###############################################################################
# Configuration
###############################################################################
DATA_DIR = (Path(__file__).resolve().parents[0] / "data" / "madden" / "dataset").expanduser()
# Provided mapping dictionaries ------------------------------------------------


COLUMNS: List[str] = [
    # Identity / Meta
    "madden_id", 'player_id', "team", "season", "fullname",
    # Position info
    "position", "position_group", "overallrating",
    # Pace
    "agility", "acceleration", "speed", "stamina",
    # Strength / Fitness / General
    "strength", "toughness", "injury", "awareness", "jumping",
    "trucking", "archetype", "runningstyle", "changeofdirection", "playrecognition",
    # Passing
    "throwpower", "throwaccuracyshort", "throwaccuracymid", "throwaccuracydeep", "playaction", "throwonrun",
    # Rushing
    "carrying", "ballcarriervision", "stiffarm", "spinmove", "jukemove",
    # Receiving
    "catching", "shortrouterunning", "midrouterunning", "deeprouterunning", "spectacularcatch", "catchintraffic", "release",
    # Blocking
    "runblocking", "passblocking", "impactblocking",
    # Coverage / Defense
    "mancoverage", "zonecoverage", "tackle", "hitpower", "press", "pursuit",
    # Special Teams
    "kickaccuracy", "kickpower", "return",
    # Meta
    "jerseynumber", "yearspro", "age", "birthdate",
]


###############################################################################
# Helpers
###############################################################################

def available_seasons() -> List[int]:
    return sorted(int(p.stem) for p in DATA_DIR.glob("*.parquet") if p.stem.isdigit())


@st.cache_data(show_spinner=False)
def load_madden_data(season: int) -> pd.DataFrame:
    try:
        # Read the Madden dataset
        df = read_madden_dataset(season)

        # Ensure required cols exist
        cols_present = [c for c in COLUMNS if c in df.columns]
        df = df[cols_present].copy()
        df = df[df.player_id.notnull()].copy()
        # df['team'] = df['team'].replace({"LA":"LAR"})

        # Derive position_group if missing using POSITION_MAPPER
        if "position_group" not in df.columns and "position" in df.columns:
            df["position_group"] = df["position"].map(POSITION_MAPPER)

        # Add high_pos_group
        df["high_pos_group"] = df["position_group"].map(HIGH_POSITION_MAPPER).fillna("NA")
        return df

    except Exception as e:
        st.error(f"Error loading data for season {season}: {str(e)}")
        st.stop()


def get_starters(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to only include starters based on position_group and overall rating."""
    starters = pd.DataFrame()

    for team in df['team'].unique():
        team_df = df[df['team'] == team].copy()

        # Offense (11 players)
        offense = pd.concat([
            team_df[team_df['position_group'] == 'quarterback'].nlargest(1, 'overallrating'),  # 1 QB
            team_df[team_df['position_group'] == 'o_pass'].nlargest(3, 'overallrating'),  # 3 WR
            team_df[team_df['position_group'] == 'o_te'].nlargest(1, 'overallrating'),  # 1 TE
            team_df[team_df['position_group'] == 'o_rush'].nlargest(1, 'overallrating'),  # 1 RB
            team_df[team_df['position_group'] == 'o_line'].nlargest(5, 'overallrating')  # 5 OL
        ])

        # Defense (11 players)
        defense = pd.concat([
            team_df[team_df['position_group'] == 'd_line'].nlargest(4, 'overallrating'),  # 4 DL
            team_df[team_df['position_group'] == 'd_lb'].nlargest(3, 'overallrating'),  # 3 LB
            team_df[team_df['position_group'] == 'd_field'].nlargest(4, 'overallrating')  # 4 DB
        ])

        # Special Teams (3 players)
        special_teams = pd.concat([
            team_df[team_df['position_group'] == 'special_teams'].nlargest(3, 'overallrating')  # K, P, LS
        ])

        starters = pd.concat([starters, offense, defense, special_teams])

    return starters.reset_index(drop=True)


def group_stats(df: pd.DataFrame, group_col: str, starters_only: bool = True) -> pd.DataFrame:
    """Generic summary by group_col."""
    data = get_starters(df) if starters_only else df
    return (
        data.groupby(group_col)[["overallrating"]]
            .agg(players=("overallrating", "size"), avg_rating=("overallrating", "mean"))
            .sort_values("avg_rating", ascending=False)
            .reset_index()
    )


def season_pivot(df_season: pd.DataFrame, col: str, starters_only: bool = True) -> pd.DataFrame:
    data = get_starters(df_season) if starters_only else df_season
    pivot = (
        data.pivot_table(index="team", columns=col, values="overallrating", aggfunc="mean")
            .round(1)
            .sort_index()
    )
    return pivot


def get_attribute_rankings(df: pd.DataFrame, player_id: str, position_group: str) -> pd.DataFrame:
    """Get attribute rankings for a player within their position group."""
    pos_df = df[df['position_group'] == position_group].copy()

    # Get all relevant attributes
    all_attrs = []
    for attrs in CATEGORY_MAP.values():
        all_attrs.extend(attrs)
    all_attrs = list(set(all_attrs))

    # Calculate rankings and percentiles for each attribute
    rankings = {}
    for attr in all_attrs:
        if attr in pos_df.columns:
            # Get rank (1-based)
            rank = pos_df[attr].rank(method='min', ascending=False)
            total = len(pos_df)
            rankings[attr] = {
                'value': pos_df.loc[pos_df['player_id'] == player_id, attr].iloc[0],
                'rank': int(rank[pos_df['player_id'] == player_id].iloc[0]),
                'total': total
            }

    return rankings


def calculate_category_rating(player_attrs: dict, category: str) -> float:
    """Calculate average rating for a category of attributes."""
    attrs = CATEGORY_MAP[category]
    values = [player_attrs[attr]['value'] for attr in attrs if attr in player_attrs]
    return sum(values) / len(values) if values else 0


def format_attribute_cell(value: float, rank: int, total: int) -> str:
    """Format an attribute cell with color scaling and ranking."""
    # Calculate color based on value (0-99 scale)
    r = max(0, min(255, int(255 * (1 - value / 99))))
    g = max(0, min(255, int(255 * (value / 99))))

    return f"""
        <div style="
            background-color: rgba({r}, {g}, 0, 0.2);
            padding: 4px;
            border-radius: 4px;
            display: flex;
            justify-content: space-between;
        ">
            <span style="font-weight: bold;">{int(value)}</span>
            <span style="color: gray;">#{rank}/{total}</span>
        </div>
    """


###############################################################################
# Streamlit UI
###############################################################################

st.set_page_config(
    page_title="NFL Madden App",
    page_icon="🏈",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://github.com/theedgepredictor/nfl-madden-data',
        'Report a bug': 'https://github.com/theedgepredictor/nfl-madden-data/issues',
        'About': 'NFL Madden Data Analytics Platform'
    },
    theme="dark"  # This sets dark mode as default
)

st.title("🏈 Madden Data Analysis")

# ---------------- Sidebar ---------------------------------------------------
with st.sidebar:
    st.header("Filters")
    seasons = available_seasons()
    if not seasons:
        st.error(f"No CSVs found in {DATA_DIR}")
        st.stop()
    season_choice = st.selectbox("Season", seasons, index=len(seasons) - 1)
    df_season = load_madden_data(season_choice)
    teams_available = sorted(df_season["team"].dropna().unique())
    team_choice = st.selectbox("Team", teams_available)

# ---------------- Main Tabs -------------------------------------------------
main_tabs = st.tabs(["Roster", "Player Analysis", "Team Stats", "Season Stats"])

# === 1. Roster tab ==========================================================
with main_tabs[0]:
    st.subheader(f"{team_choice} Roster – {season_choice}")
    df_team = df_season[df_season["team"] == team_choice].sort_values("overallrating", ascending=False).reset_index(drop=True)

    if df_team.empty:
        st.warning("No players found for this team.")
    else:
        # High‑level tabs: off / def / st / NA
        high_groups = df_team["high_pos_group"].dropna().unique().tolist()
        high_tabs = st.tabs(high_groups) if len(high_groups) > 1 else [st.container()]
        for htab, high in zip(high_tabs, high_groups):
            with htab:
                df_high = df_team[df_team["high_pos_group"] == high]
                st.markdown(f"### {high.upper()} – {len(df_high)} players")
                # Nested tabs by position_group
                groups   = df_high["position_group"].dropna().unique().tolist()
                pg_tabs = st.tabs(groups) if len(groups) > 1 else [st.container()]
                for ptab, pg in zip(pg_tabs, groups):
                    with ptab:
                        df_pg = df_high[df_high["position_group"] == pg]
                        st.markdown(f"**{pg}** – {len(df_pg)} players")
                        st.dataframe(df_pg, hide_index=True, use_container_width=True)

# === 2. Player Analysis tab ===================================================
with main_tabs[1]:
    st.subheader(f"Player Analysis – {team_choice} {season_choice}")

    if df_team.empty:
        st.warning("No players found for this team.")
    else:
        # Player selector
        player_choice = st.selectbox(
            "Select Player",
            df_team['fullname'].tolist(),
            format_func=lambda x: f"{x} ({df_team[df_team['fullname'] == x]['position'].iloc[0]} - {int(df_team[df_team['fullname'] == x]['overallrating'].iloc[0])} OVR)"
        )

        player = df_team[df_team['fullname'] == player_choice].iloc[0]
        position_group = player['position_group']

        # Get attribute rankings
        rankings = get_attribute_rankings(df_season, player['player_id'], position_group)

        # Display overall rating and position rank
        col1, col2 = st.columns(2)
        with col1:
            pos_df = df_season[df_season['position_group'] == position_group]
            pos_rank = pos_df['overallrating'].rank(ascending=False)
            player_idx = pos_df[pos_df['player_id'] == player['player_id']].index[0]
            total_players = len(pos_df)
            st.metric(
                "Overall Rating",
                f"{int(player['overallrating'])}",
                f"#{int(pos_rank[player_idx])}/{total_players} {position_group}"
            )

        # Display attribute categories in rows of 3
        categories = [cat for cat, attrs in CATEGORY_MAP.items() 
                     if any(attr in rankings for attr in attrs)]
        
        # Create rows of 3 columns, always maintaining equal width
        for i in range(0, len(categories), 3):
            row_categories = categories[i:i+3]
            # Always create 3 columns for consistent width, even if some are empty
            cols = st.columns([1, 1, 1])
            
            for j, (col, category) in enumerate(zip(cols[:len(row_categories)], row_categories)):
                with col:
                    attrs = CATEGORY_MAP[category]
                    category_attrs = {k: v for k, v in rankings.items() if k in attrs}
                    
                    st.markdown(f"### {category}")
                    avg_rating = calculate_category_rating(rankings, category)
                    st.caption(f"Category Rating: {int(avg_rating)}")
                    
                    # Create attribute display
                    for attr, data in category_attrs.items():
                        value = data['value']
                        rank = data['rank']
                        total = data['total']
                        
                        # Determine color based on value ranges
                        if value >= 80:
                            color = "var(--success-color, #2E7D32)"  # Green for high values
                        elif value >= 60:
                            color = "var(--warning-color, #F57C00)"  # Orange for medium values
                        else:
                            color = "var(--error-color, #D32F2F)"    # Red for low values
                        
                        st.markdown(
                            f"""<div style="
                                margin: 4px 0;
                                font-family: 'Inter', sans-serif;
                                background-color: #262730;
                                padding: 8px;
                                border-radius: 4px;
                            ">
                                <div style="
                                    display: flex;
                                    justify-content: space-between;
                                    margin-bottom: 2px;
                                    font-size: 14px;
                                    color: #E6E6E6;
                                ">
                                    <span>{attr.title()}</span>
                                    <span style="font-weight: bold;">{int(value)}</span>
                                </div>
                                <div style="
                                    width: 100%;
                                    height: 8px;
                                    background-color: #2C2C2C;
                                    border-radius: 4px;
                                    overflow: hidden;
                                ">
                                    <div style="
                                        width: {value}%;
                                        height: 100%;
                                        background-color: {color};
                                        transition: width 0.3s ease;
                                    "></div>
                                </div>
                                <div style="
                                    text-align: right;
                                    font-size: 12px;
                                    color: #9E9E9E;
                                    margin-top: 2px;
                                ">
                                    #{rank}/{total}
                                </div>
                            </div>""",
                            unsafe_allow_html=True
                        )
                    value = data['value']
                    rank = data['rank']
                    total = data['total']
                    
                    # Determine color based on value ranges
                    if value >= 80:
                        color = "var(--success-color, #2E7D32)"  # Green for high values
                    elif value >= 60:
                        color = "var(--warning-color, #F57C00)"  # Orange for medium values
                    else:
                        color = "var(--error-color, #D32F2F)"    # Red for low values
                    
                    st.markdown(
                        f"""<div style="
                            margin: 4px 0;
                            font-family: 'Inter', sans-serif;
                        ">
                            <div style="
                                display: flex;
                                justify-content: space-between;
                                margin-bottom: 2px;
                                font-size: 14px;
                            ">
                                <span>{attr.title()}</span>
                                <span style="font-weight: bold;">{int(value)}</span>
                            </div>
                            <div style="
                                width: 100%;
                                height: 8px;
                                background-color: #E0E0E0;
                                border-radius: 4px;
                                overflow: hidden;
                            ">
                                <div style="
                                    width: {value}%;
                                    height: 100%;
                                    background-color: {color};
                                    transition: width 0.3s ease;
                                "></div>
                            </div>
                            <div style="
                                text-align: right;
                                font-size: 12px;
                                color: #666;
                                margin-top: 2px;
                            ">
                                #{rank}/{total}
                            </div>
                        </div>""",
                        unsafe_allow_html=True
                    )

# === 2. Team Stats tab ======================================================
with main_tabs[2]:
    st.subheader(f"Summary – {team_choice} {season_choice}")
    if df_team.empty:
        st.info("Select a team with player data to view statistics.")
    else:
        # Toggle for starters only
        starters_only = st.checkbox("Show Starters Only", value=True, key="team_starters")
        # High position group summary
        st.markdown("### By High Position Group (Off / Def / ST)")
        st.dataframe(group_stats(df_team, "high_pos_group", starters_only), hide_index=True, use_container_width=True)

        # Position group summary
        st.markdown("### By Position Group")
        st.dataframe(group_stats(df_team, "position_group", starters_only), hide_index=True, use_container_width=True)

        # Snapshot
        st.markdown("### Overall Snapshot")
        st.metric("Total Players", len(df_team))
        st.metric("Average Overall Rating", round(df_team["overallrating"].mean(), 2))

# === 3. Season Stats tab ====================================================
with main_tabs[3]:
    st.subheader(f"Season Overview – {season_choice}")
    if df_season.empty:
        st.info("No player data available for this season.")
    else:
        # Toggle for starters only
        starters_only = st.checkbox("Show Starters Only", value=True, key="season_starters")

        # High position group summary
        st.markdown("### By High Position Group (Off / Def / ST)")
        st.dataframe(group_stats(df_season, "high_pos_group", starters_only), hide_index=True, use_container_width=True)

        # Position group summary
        st.markdown("### By Position Group")
        st.dataframe(group_stats(df_season, "position_group", starters_only), hide_index=True, use_container_width=True)

        # Pivot table of average rating by position group
        st.markdown("### Average Rating by Position Group")
        pivot = season_pivot(df_season, "position_group", starters_only)
        st.dataframe(pivot, use_container_width=True)

# ---------------- Downloads -------------------------------------------------
with st.sidebar:
    st.markdown("---")
    st.download_button(
        label="Download Selected Team CSV",
        data=df_team.to_csv(index=False).encode("utf-8"),
        file_name=f"{team_choice}_{season_choice}_roster.csv",
        mime="text/csv",
        disabled=df_team.empty,
    )
    st.download_button(
        label="Download Season Overview CSV",
        data=group_stats(df_season, "team").to_csv(index=False).encode("utf-8"),
        file_name=f"season_{season_choice}_team_overview.csv",
        mime="text/csv",
    )

st.caption("Built for rapid data‑quality checks while cleaning Madden ratings ✨")
