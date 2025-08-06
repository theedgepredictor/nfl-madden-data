from enum import Enum

# ---------- Running Style ----------
class RunStyleEnum(Enum):
    DEFAULT = 1
    DEFAULT_LOOSE = 2
    DEFAULT_HIGHANDTIGHT = 3
    DEFAULT_AWKWARD = 4
    DEFAULT_BREADLOAF = 5
    LONG_LOOSE = 6
    LONG_DEFAULT = 7
    LONG_HIGHANDTIGHT = 8
    LONG_AWKWARD = 9
    LONG_BREADLOAF = 10
    SHORT_DEFAULT = 11
    SHORT_LOOSE = 12
    SHORT_HIGHANDTIGHT = 13
    SHORT_AWKWARD = 14
    SHORT_BREADLOAF = 15

RUN_STYLE_MAPPER = {
    'Default': RunStyleEnum.DEFAULT.value,
    'Default Stride Loose': RunStyleEnum.DEFAULT_LOOSE.value,
    'Default Stride High and Tight': RunStyleEnum.DEFAULT_HIGHANDTIGHT.value,
    'Default Stride Awkward': RunStyleEnum.DEFAULT_AWKWARD.value,
    'Default Stride Bread Loaf': RunStyleEnum.DEFAULT_BREADLOAF.value,
    'Long Stride Loose': RunStyleEnum.LONG_LOOSE.value,
    'Long Stride Default': RunStyleEnum.LONG_DEFAULT.value,
    'Long Stride High and Tight': RunStyleEnum.LONG_HIGHANDTIGHT.value,
    'Long Stride Awkward': RunStyleEnum.LONG_AWKWARD.value,
    'Long Stride Bread Loaf': RunStyleEnum.LONG_BREADLOAF.value,
    'Short Stride Default': RunStyleEnum.SHORT_DEFAULT.value,
    'Short Stride Loose': RunStyleEnum.SHORT_LOOSE.value,
    'Short Stride High and Tight': RunStyleEnum.SHORT_HIGHANDTIGHT.value,
    'Short Stride Awkward': RunStyleEnum.SHORT_AWKWARD.value,
    'Short Stride Bread Loaf': RunStyleEnum.SHORT_BREADLOAF.value,
    None: RunStyleEnum.DEFAULT.value,
}

# ---------- Defensive Line ----------
class DLArchetype(Enum):
    DL_DEFAULT = 0
    DL_RUNSTOPPER = 1
    DL_POWERRUSHER = 2
    DL_SPEEDRUSHER = 3
    DL_SMALLSPEEDRUSHER = 4

DL_ARCHETYPE_MAPPER = {
    'Run Stopper - DE': DLArchetype.DL_RUNSTOPPER.value,
    'DE_RunStopper': DLArchetype.DL_RUNSTOPPER.value,
    'DT_RunStopper': DLArchetype.DL_RUNSTOPPER.value,
    'Run Stopper - DT': DLArchetype.DL_RUNSTOPPER.value,
    'Power Rusher - DE': DLArchetype.DL_POWERRUSHER.value,
    'DE_PowerRusher': DLArchetype.DL_POWERRUSHER.value,
    'DT_PowerRusher': DLArchetype.DL_POWERRUSHER.value,
    'Power Rusher - DT': DLArchetype.DL_POWERRUSHER.value,
    'DE_SmallerSpeedRusher': DLArchetype.DL_SMALLSPEEDRUSHER.value,
    'Smaller Speed Rusher - DE': DLArchetype.DL_SMALLSPEEDRUSHER.value,
    'Speed Rusher - DT': DLArchetype.DL_SPEEDRUSHER.value,
    'DT_SpeedRusher': DLArchetype.DL_SPEEDRUSHER.value,
    None: DLArchetype.DL_DEFAULT.value,
}

# ---------- Linebackers ----------
class LBArchetype(Enum):
    LB_DEFAULT = 0
    LB_RUNSTOPPER = 1
    LB_POWERRUSHER = 2
    LB_SPEEDRUSHER = 3
    LB_PASSCOVERAGE = 4
    LB_FIELDGENERAL = 5

LB_ARCHETYPE_MAPPER = {
    'OLB_RunStopper': LBArchetype.LB_RUNSTOPPER.value,
    'Run Stopper - OLB': LBArchetype.LB_RUNSTOPPER.value,
    'MLB_RunStopper': LBArchetype.LB_RUNSTOPPER.value,
    'Run Stopper - MLB': LBArchetype.LB_RUNSTOPPER.value,
    'OLB_PowerRusher': LBArchetype.LB_POWERRUSHER.value,
    'Power Rusher - OLB': LBArchetype.LB_POWERRUSHER.value,
    'OLB_SpeedRusher': LBArchetype.LB_SPEEDRUSHER.value,
    'Speed Rusher - OLB': LBArchetype.LB_SPEEDRUSHER.value,
    'OLB_PassCoverage': LBArchetype.LB_PASSCOVERAGE.value,
    'Pass Coverage - OLB': LBArchetype.LB_PASSCOVERAGE.value,
    'MLB_PassCoverage': LBArchetype.LB_PASSCOVERAGE.value,
    'Pass Coverage - MLB': LBArchetype.LB_PASSCOVERAGE.value,
    'MLB_FieldGeneral': LBArchetype.LB_FIELDGENERAL.value,
    'Field General - MLB': LBArchetype.LB_FIELDGENERAL.value,
    None: LBArchetype.LB_DEFAULT.value,
}

# ---------- Defensive Backs ----------
class DBArchetype(Enum):
    DB_DEFAULT = 0
    DB_ZONE = 1
    DB_MANTOMAN = 2
    DB_RUNSUPPORT = 3
    DB_HYBRID = 4
    DB_SLOT = 5

DB_ARCHETYPE_MAPPER = {
    'CB_Zone': DBArchetype.DB_ZONE.value,
    'Zone - CB': DBArchetype.DB_ZONE.value,
    'S_Zone': DBArchetype.DB_ZONE.value,
    'Zone - S': DBArchetype.DB_ZONE.value,
    'CB_MantoMan': DBArchetype.DB_MANTOMAN.value,
    'Manto Man - CB': DBArchetype.DB_MANTOMAN.value,
    'S_RunSupport': DBArchetype.DB_RUNSUPPORT.value,
    'Run Support - S': DBArchetype.DB_RUNSUPPORT.value,
    'S_Hybrid': DBArchetype.DB_HYBRID.value,
    'Hybrid - S': DBArchetype.DB_HYBRID.value,
    'CB_Slot': DBArchetype.DB_SLOT.value,
    'Slot - CB': DBArchetype.DB_SLOT.value,
    None: DBArchetype.DB_DEFAULT.value,
}

# ---------- Offensive Line ----------
class OLArchetype(Enum):
    OL_DEFAULT = 0
    OL_POWER = 1
    OL_AGILE = 2
    OL_PASSPROTECTOR = 3

OL_ARCHETYPE_MAPPER = {
    'G_Power': OLArchetype.OL_POWER.value,
    'Power - G': OLArchetype.OL_POWER.value,
    'C_Power': OLArchetype.OL_POWER.value,
    'Power - C': OLArchetype.OL_POWER.value,
    'OT_Power': OLArchetype.OL_POWER.value,
    'Power - OT': OLArchetype.OL_POWER.value,
    'G_Agile': OLArchetype.OL_AGILE.value,
    'Agile - G': OLArchetype.OL_AGILE.value,
    'C_Agile': OLArchetype.OL_AGILE.value,
    'Agile - C': OLArchetype.OL_AGILE.value,
    'OT_Agile': OLArchetype.OL_AGILE.value,
    'Agile - OT': OLArchetype.OL_AGILE.value,
    'G_PassProtector': OLArchetype.OL_PASSPROTECTOR.value,
    'Pass Protector - G': OLArchetype.OL_PASSPROTECTOR.value,
    'C_PassProtector': OLArchetype.OL_PASSPROTECTOR.value,
    'Pass Protector - C': OLArchetype.OL_PASSPROTECTOR.value,
    'OT_PassProtector': OLArchetype.OL_PASSPROTECTOR.value,
    'Pass Protector - OT': OLArchetype.OL_PASSPROTECTOR.value,
    None: OLArchetype.OL_DEFAULT.value,
}

# ---------- Wide Receivers ----------
class WRArchetype(Enum):
    WR_DEFAULT = 0
    WR_PHYSICAL = 1
    WR_PLAYMAKER = 2
    WR_DEEPTHREAT = 3
    WR_SLOT = 4
    WR_ROUTERUNNER = 5

WR_ARCHETYPE_MAPPER = {
    'WR_Physical': WRArchetype.WR_PHYSICAL.value,
    'Physical - WR': WRArchetype.WR_PHYSICAL.value,
    'WR_Playmaker': WRArchetype.WR_PLAYMAKER.value,
    'WR_DeepThreat': WRArchetype.WR_DEEPTHREAT.value,
    'Deep Threat - WR': WRArchetype.WR_DEEPTHREAT.value,
    'WR_Slot': WRArchetype.WR_SLOT.value,
    'Slot - WR': WRArchetype.WR_SLOT.value,
    'WR_RouteRunner': WRArchetype.WR_ROUTERUNNER.value,
    None: WRArchetype.WR_DEFAULT.value,
}

# ---------- Running Backs ----------
class RBArchetype(Enum):
    RB_DEFAULT = 0
    RB_POWERBACK = 1
    RB_ELUSIVEBACK = 2
    RB_RECEIVINGBACK = 3

RB_ARCHETYPE_MAPPER = {
    'HB_PowerBack': RBArchetype.RB_POWERBACK.value,
    'Power Back - HB': RBArchetype.RB_POWERBACK.value,
    'HB_ElusiveBack': RBArchetype.RB_ELUSIVEBACK.value,
    'Elusive Back - HB': RBArchetype.RB_ELUSIVEBACK.value,
    'HB_ReceivingBack': RBArchetype.RB_RECEIVINGBACK.value,
    'Receiving Back - HB': RBArchetype.RB_RECEIVINGBACK.value,
    None: RBArchetype.RB_DEFAULT.value,
}

# ---------- Tight Ends / Fullbacks ----------
class TEArchetype(Enum):
    TE_DEFAULT = 0
    TE_POSSESSION = 1
    TE_VERTICALTHREAT = 2
    TE_BLOCKING = 3
    FB_UTILITY = 4

TE_ARCHETYPE_MAPPER = {
    'TE_Possession': TEArchetype.TE_POSSESSION.value,
    'Possession - TE': TEArchetype.TE_POSSESSION.value,
    'TE_VerticalThreat': TEArchetype.TE_VERTICALTHREAT.value,
    'Vertical Threat - TE': TEArchetype.TE_VERTICALTHREAT.value,
    'TE_Blocking': TEArchetype.TE_BLOCKING.value,
    'Blocking - TE': TEArchetype.TE_BLOCKING.value,
    'FB_Utility': TEArchetype.FB_UTILITY.value,
    'Utility - FB': TEArchetype.FB_UTILITY.value,
    None: TEArchetype.TE_DEFAULT.value,
}

# ---------- Quarterbacks ----------
class QBArchetype(Enum):
    QB_DEFAULT = 0
    QB_FIELDGENERAL = 1
    QB_IMPROVISER = 2
    QB_SCRAMBLER = 3
    QB_STRONGARM = 4

QB_ARCHETYPE_MAPPER = {
    'QB_FieldGeneral': QBArchetype.QB_FIELDGENERAL.value,
    'Field General - QB': QBArchetype.QB_FIELDGENERAL.value,
    'QB_Improviser': QBArchetype.QB_IMPROVISER.value,
    'Improviser - QB': QBArchetype.QB_IMPROVISER.value,
    'QB_Scrambler': QBArchetype.QB_SCRAMBLER.value,
    'Scrambler - QB': QBArchetype.QB_SCRAMBLER.value,
    'QB_StrongArm': QBArchetype.QB_STRONGARM.value,
    'Strong Arm - QB': QBArchetype.QB_STRONGARM.value,
    None: QBArchetype.QB_DEFAULT.value,
}

# ---------- Special Teams ----------
class STArchetype(Enum):
    ST_DEFAULT = 0
    LS_ACCURATE = 1
    LS_POWER = 2
    KP_POWER = 3
    KP_ACCURATE = 4

ST_ARCHETYPE_MAPPER = {
    'LS_Accurate': STArchetype.LS_ACCURATE.value,
    'LS_Power': STArchetype.LS_POWER.value,
    'KP_Power': STArchetype.KP_POWER.value,
    'Power - KP': STArchetype.KP_POWER.value,
    'KP_Accurate': STArchetype.KP_ACCURATE.value,
    'Accurate - KP': STArchetype.KP_ACCURATE.value,
    None: STArchetype.ST_DEFAULT.value,
}

TEAMS = [
    'TB', 'NO', 'DAL', 'BAL', 'LAR', 'PHI', 'GB', 'WAS', 'DET', 'PIT',
    'KC', 'LV', 'CHI', 'ATL', 'NYG', 'MIN', 'NYJ', 'CIN', 'NE', 'IND',
    'JAX', 'SF', 'LAC', 'DEN', 'CAR', 'TEN', 'ARI', 'MIA', 'SEA',
    'CLE', 'BUF', 'HOU', 'LA', 'FREEAGENT'
]

TEAM_MAPPER = {'FREEAGENT': -1}
TEAM_MAPPER.update({team: idx for idx, team in enumerate(TEAMS) if team != 'FREEAGENT'})

ARCHETYPE_POSITION_MAPPERS = {
    'd_line': DL_ARCHETYPE_MAPPER,
    'd_lb': LB_ARCHETYPE_MAPPER,
    'd_field': DB_ARCHETYPE_MAPPER,
    'quarterback': QB_ARCHETYPE_MAPPER,
    'o_rush': RB_ARCHETYPE_MAPPER,
    'o_pass': WR_ARCHETYPE_MAPPER,
    'o_te': TE_ARCHETYPE_MAPPER,
    'special_teams': ST_ARCHETYPE_MAPPER,
    'o_line': OL_ARCHETYPE_MAPPER,
}

META = [
    'madden_id','fullname','position_group','player_id'
]

GENERAL_ATTRIBUTES = [
    'season', 'team', 'yearspro', 'age', 'height', 'weight', 'draft_round', 'draft_pick',
    'forty', 'bench', 'vertical', 'broad_jump', 'cone', 'shuttle', 'is_rookie', 'last_season_av'
]

MADDEN_ATTRIBUTE_MAP = {
    "overallrating": "int",
    "agility": "int",
    "acceleration": "int",
    "speed": "int",
    "stamina": "int",
    "strength": "int",
    "toughness": "int",
    "injury": "int",
    "awareness": "int",
    "jumping": "int",
    "trucking": "int",
    "archetype": "int",
    "runningstyle": "int",
    "changeofdirection": "int",
    "playrecognition": "int",
    "throwpower": "int",
    "throwaccuracyshort": "int",
    "throwaccuracymid": "int",
    "throwaccuracydeep": "int",
    "playaction": "int",
    "throwonrun": "int",
    "carrying": "int",
    "ballcarriervision": "int",
    "stiffarm": "int",
    "spinmove": "int",
    "jukemove": "int",
    "catching": "int",
    "shortrouterunning": "int",
    "midrouterunning": "int",
    "deeprouterunning": "int",
    "spectacularcatch": "int",
    "catchintraffic": "int",
    "release": "int",
    "runblocking": "int",
    "passblocking": "int",
    "impactblocking": "int",
    "mancoverage": "int",
    "zonecoverage": "int",
    "tackle": "int",
    "hitpower": "int",
    "press": "int",
    "pursuit": "int",
    "kickaccuracy": "int",
    "kickpower": "int",
    "return": "int",
}

CATEGORY_MAP = {
    "General": [
        "speed", "acceleration", "strength", "agility", "awareness",
        "jumping", "injury", "stamina", "toughness"
    ],
    "Ballcarrier": [
        "carrying", "ballcarriervision", "trucking", "changeofdirection",
        "stiffarm", "spinmove", "jukemove"
    ],
    "Blocking": [
        "runblocking", "passblocking", "impactblocking"
    ],
    "Passing": [
        "throwpower", "throwaccuracyshort", "throwaccuracymid",
        "throwaccuracydeep", "throwonrun", "playaction"
    ],
    "Defense": [
        "tackle", "mancoverage", "zonecoverage", "hitpower",
        "pursuit", "playrecognition", "press"
    ],
    "Receiving": [
        "catching", "shortrouterunning", "midrouterunning",
        "deeprouterunning", "spectacularcatch", "catchintraffic", "release"
    ],
    "Kicking": [
        "kickaccuracy", "kickpower", "return"
    ]
}
