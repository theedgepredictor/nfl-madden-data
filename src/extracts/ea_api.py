
from __future__ import annotations
import pandas as pd
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional

import requests

__all__ = [
    "MaddenRatingsClient",
    "Player",
    "Ability",
    "AbilityType",
    "Team",
    "Position",
]

from src.utils import find_year_for_season

BASE_URL = "https://drop-api.ea.com"
JSON_ACCEPT = "application/json"

# ---------------------------------------------------------------------------
# Required X‑Feature header payload (taken from browser network tab)
# ---------------------------------------------------------------------------

_DEFAULT_X_FEATURE = (
    "{"  # keep formatted as one‑liner to avoid header breaks
    "\"enable_addon_bundle_sections\":true,\"enable_age_gate\":true,"
    "\"enable_age_gate_refactor\":true,\"enable_bf2042_glacier_theme\":false,"
    "\"enable_checkout_page\":false,\"enable_college_football_ratings\":true,"
    "\"enable_currency\":false,\"enable_events_page\":true,"
    "\"enable_fc_mobile_game_languages\":true,\"enable_franchise_newsletter\":false,"
    "\"enable_im_resize_query_param\":true,\"enable_language_redirection\":true,"
    "\"enable_legal_disclaimer_page\":false,\"enable_mobile_download_flow_optimization\":true,"
    "\"enable_newsletter_with_incentive\":false,\"enable_next_ratings_release\":true,"
    "\"enable_player_stats\":false,\"enable_portal\":false,\"enable_postlaunch_webstore_focus\":false,"
    "\"enable_postlaunch_webstore_image_link\":false,\"enable_ratings_up_down_vote\":false,"
    "\"enable_spotlight_carousel\":true,\"enable_translations_api_route\":false,"
    "\"enable_ugc_page\":false,\"enable_ugx\":false}"
)

# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class AbilityType:
    id: str
    label: str
    image_url: str | None = None
    icon_url: str | None = None

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "AbilityType":
        return cls(
            id=data.get("id"),
            label=data.get("label"),
            image_url=data.get("imageUrl"),
            icon_url=data.get("iconUrl"),
        )


@dataclass(slots=True)
class Ability:
    id: str
    label: str
    description: str
    image_url: str | None
    type_: AbilityType

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Ability":
        return cls(
            id=data.get("id"),
            label=data.get("label"),
            description=data.get("description"),
            image_url=data.get("imageUrl"),
            type_=AbilityType.from_json(data.get("type", {})),
        )


@dataclass(slots=True)
class Team:
    id: int
    name: str
    image_url: str | None = None
    is_popular: bool | None = None

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Team":
        return cls(
            id=data.get("id"),
            name=data.get("label"),
            image_url=data.get("imageUrl"),
            is_popular=data.get("isPopular"),
        )


@dataclass(slots=True)
class PositionType:
    id: str
    name: str

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "PositionType":
        return cls(id=data.get("id"), name=data.get("name"))


@dataclass(slots=True)
class Position:
    id: str
    short_label: str
    label: str
    position_type: PositionType

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Position":
        return cls(
            id=data.get("id"),
            short_label=data.get("shortLabel"),
            label=data.get("label"),
            position_type=PositionType.from_json(data.get("positionType", {})),
        )


@dataclass(slots=True)
class Player:
    id: int
    first_name: str
    last_name: str
    birthdate: str
    height: str
    weight: str
    overall_rating: int
    age: int
    jersey_number: int
    team: Team
    position: Position
    stats: Dict[str, Any]
    archetype: Dict[str,str]
    abilities: List[Ability] = field(default_factory=list)
    avatar_url: str | None = None

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Player":
        return cls(
            id=data.get("id"),
            first_name=data.get("firstName"),
            last_name=data.get("lastName"),
            birthdate=data.get("birthdate"),
            height=data.get("height"),
            weight=data.get("weight"),
            overall_rating=data.get("overallRating"),
            age=data.get("age"),
            jersey_number=data.get("jerseyNum"),
            team=Team.from_json(data.get("team", {})),
            position=Position.from_json(data.get("position", {})),
            stats={k: (v.get("value") if isinstance(v, dict) else v) for k, v in data.get("stats", {}).items()},
            archetype=data.get('archetype'),
            abilities=[Ability.from_json(a) for a in data.get("playerAbilities", [])],
            avatar_url=data.get("avatarUrl"),
        )




class MaddenRatingsClient:
    def __init__(
            self,
            *,
            locale: str = "en",
            timeout: float | tuple = 30,
            session: Optional[requests.Session] = None,
    ) -> None:
        self.locale = locale
        self.timeout = timeout
        self.session = session or requests.Session()

        # Base headers
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (compatible; MaddenRatingsPython/0.4)",
                "Accept": JSON_ACCEPT,
                "X-Feature": _DEFAULT_X_FEATURE,
            }
        )

    # ---------------- Low‑level ---------------- #

    def _request(self, path: str, **params):
        params.setdefault("locale", self.locale)
        url = f"{BASE_URL}{path}"
        resp = self.session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    # ---------------- High‑level ---------------- #

    def list_players(
            self,
            *,
            limit: int = 100,
            iteration: str = "1-base",
            offset: int = 0,
            filters: Optional[Dict[str, Any]] = None,
            **extra_query,
    ) -> List[Player]:
        payload = self._request(
            "/rating/madden-nfl",
            limit=limit,
            iteration=iteration,
            offset=offset,
            **(filters or {}),
            **extra_query,
        )
        return [Player.from_json(i) for i in payload.get("items", [])]

    def iter_players(
            self,
            *,
            limit: int = 100,
            iteration: str = "1-base",
            filters: Optional[Dict[str, Any]] = None,
            **extra_query,
    ) -> Iterator[Player]:
        offset = 0
        while True:
            page = self.list_players(
                limit=limit, iteration=iteration, offset=offset, filters=filters, **extra_query
            )
            if not page:
                break
            yield from page
            offset += limit

    def get_player(self, player_id: int) -> Player:
        return Player.from_json(self._request(f"/rating/madden-nfl/{player_id}"))

    def flatten_players(self, players: List[Player]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Flatten a list of Player objects into two DataFrames:
        • `df`           – one row per player, every field + every stat + metadata
        • `abilities_df` – one row per ability (player_id repeated)
        Both include a UTC `last_updated` timestamp column.
        """
        ts = datetime.now(timezone.utc)
        flat_rows: List[Dict[str, Any]] = []
        ability_rows: List[Dict[str, Any]] = []

        for p in players:
            # --- base player row ------------------------------------------------
            ability_ids = '||'.join([a.id for a in p.abilities])
            if p.archetype is not None:
                archetype = p.archetype['id'] if 'id' in p.archetype else None
            else:
                archetype = None
            row = {
                "player_id":            p.id,
                "first_name":           p.first_name,
                "last_name":            p.last_name,
                "birthdate":            p.birthdate,
                "height":            p.height,
                "weight":            p.weight,
                "overall_rating":       p.overall_rating,
                "age":                  p.age,
                "jersey_number":                  p.jersey_number,
                "avatar_url":           p.avatar_url,

                # team
                "team_id":              p.team.id,
                "team_name":            p.team.name,
                "team_image_url":       p.team.image_url,
                "team_is_popular":      p.team.is_popular,

                # position
                #"position_id":          p.position.id,
                "position_short_label": p.position.short_label,
               # "position_label":       p.position.label,
                #"position_type_id":     p.position.position_type.id,
                #"position_type_name":   p.position.position_type.name,
                "archetype": archetype,
            }

            # add *every* stat as its own column
            row.update(p.stats)

            row["last_updated"] = ts
            flat_rows.append(row)

            # --- ability rows ---------------------------------------------------
            for a in p.abilities:
                ability_rows.append({
                    "player_id":              p.id,
                    "ability_id":             a.id,
                    "ability_label":          a.label,
                    "ability_description":    a.description,
                    "ability_image_url":      a.image_url,
                    "ability_type_id":        a.type_.id,
                    "ability_type_label":     a.type_.label,
                    "ability_type_image_url": a.type_.image_url,
                    "ability_type_icon_url":  a.type_.icon_url,
                    "last_updated":           ts,
                })

        df           = pd.DataFrame(flat_rows)
        abilities_df = pd.DataFrame(ability_rows)
        return df, abilities_df

    # Context manager helpers
    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

if __name__ == '__main__':
    mrc = MaddenRatingsClient()
    root_path = '../../data/madden'
    feature_store_name='raw'
    current_season = find_year_for_season()
    players = []
    for p in mrc.iter_players(limit=100, iteration="1-base"):
        players.append(p)
    df, b = mrc.flatten_players(players)
    df.to_csv(f"{root_path}/{feature_store_name}/{current_season}.csv", index=False)