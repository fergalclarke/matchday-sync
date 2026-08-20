import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

VALID_STRATEGIES = {"teams", "event_round"}
VALID_TIE_BREAKS = {"earliest_time", None}


class ConfigError(ValueError):
    """Raised when enrichment.yaml is malformed. Always fatal."""


@dataclass
class SourceConfig:
    url: str
    max_chars: int
    min_extractions: int
    # Free-text guidance appended to the extraction prompt, for pages whose
    # layout needs explaining (where the channel lives, which listings to
    # ignore, how dates are formatted). Keeping this in config is what lets a
    # new source with an awkward layout be added without touching code.
    hint: str = ""


@dataclass
class SportConfig:
    key: str
    aliases: list[str]
    source: SourceConfig
    match_strategy: str
    writes: list[str]
    channel_map: dict[str, str]
    # Used when a listing's channel is not in channel_map. Set it for sports
    # where the specific channel doesn't matter (golf is always "Sky Sports",
    # whichever Sky channel carries it), so a newly added channel can't break
    # the run. Leave unset where the channel is meaningful -- LoI needs to
    # tell vmone from vmtwo, so an unmapped channel there must be flagged.
    channel_fallback: str | None
    # Substring rules tried after channel_map, in order. For sports where a
    # whole family of channels collapses to one value: every "Sky Sports
    # Something" is just "Sky Sports". Stored pre-normalised.
    channel_patterns: list[tuple[str, str]]
    # Drop listings whose channel matches nothing, instead of flagging them.
    # EPL is carried by broadcasters we don't track (HBO Max, Amazon Prime);
    # those listings are noise, not something a human needs to review.
    ignore_unmatched_channels: bool
    # Name equivalences, as normalised-variant -> normalised-canonical. Sources
    # abbreviate inconsistently and fuzzy matching alone cannot close the gap:
    # "Manchester United" vs "Man Utd" scores 0.46, "Tottenham" vs "Tottenham
    # Hotspur" 0.69 -- both well under the threshold.
    name_aliases: dict[str, str]
    select_all: bool
    select_tv_is: list[str]
    default_tv: str | None
    default_tv_max_days: int | None
    tie_break: str | None
    model: str
    max_tokens: int
    name_match_threshold: float
    date_tolerance_days: int
    ambiguity_margin: float
    max_default_writes: int
    request_timeout: int

    def matches_sport_value(self, value: str) -> bool:
        """Airtable's Sport values are inconsistently cased ('LoI' vs 'loi')."""
        return (value or "").strip().lower() in self.aliases


@dataclass
class Config:
    base_id: str
    table: str
    window_days: int
    sports: list[SportConfig] = field(default_factory=list)

    def sport_for(self, sport_value: str) -> SportConfig | None:
        for sport in self.sports:
            if sport.matches_sport_value(sport_value):
                return sport
        return None


def _require(mapping: dict, key: str, where: str):
    if key not in mapping:
        raise ConfigError(f"{where}: missing required key '{key}'")
    return mapping[key]


def _parse_select(raw, where: str) -> tuple[bool, list[str]]:
    """`select: all` -> every row; `select: {tv_is: [...]}` -> filtered."""
    if raw == "all":
        return True, []
    if isinstance(raw, dict) and "tv_is" in raw:
        values = raw["tv_is"]
        if not isinstance(values, list) or not values:
            raise ConfigError(f"{where}: select.tv_is must be a non-empty list")
        return False, [str(v).strip().lower() for v in values]
    raise ConfigError(f"{where}: select must be 'all' or a mapping with 'tv_is'")


def _parse_channel_patterns(raw, where: str) -> list[tuple[str, str]]:
    """[{match: "sky sports", value: "Sky Sports"}, ...] -> ordered pairs."""
    if not raw:
        return []
    if not isinstance(raw, list):
        raise ConfigError(f"{where}: channel_patterns must be a list")

    # Imported here to keep config free of a module-level dependency on match.
    from .match import normalise_name

    patterns = []
    for entry in raw:
        if not isinstance(entry, dict) or "match" not in entry or "value" not in entry:
            raise ConfigError(
                f"{where}: each channel_patterns entry needs 'match' and 'value'"
            )
        needle = normalise_name(entry["match"])
        if not needle:
            raise ConfigError(f"{where}: channel_patterns 'match' cannot be empty")
        patterns.append((needle, str(entry["value"])))
    return patterns


def _parse_name_aliases(raw, where: str) -> dict[str, str]:
    """{canonical: [variant, ...]} -> {normalised variant: normalised canonical}."""
    if not raw:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(f"{where}: name_aliases must be a mapping")

    from .match import normalise_name

    index: dict[str, str] = {}
    for canonical, variants in raw.items():
        target = normalise_name(canonical)
        if isinstance(variants, str):
            variants = [variants]
        if not isinstance(variants, list):
            raise ConfigError(f"{where}: name_aliases['{canonical}'] must be a list")
        for variant in variants:
            index[normalise_name(variant)] = target
    return index


def load_config(path: str | Path = "enrichment.yaml") -> Config:
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    airtable = _require(raw, "airtable", "config")
    defaults = raw.get("defaults") or {}
    sports_raw = _require(raw, "sports", "config")
    if not isinstance(sports_raw, dict) or not sports_raw:
        raise ConfigError("config: 'sports' must be a non-empty mapping")

    # Env wins so this stage and the sync scripts can never target different bases.
    base_id = os.getenv("AIRTABLE_BASE_ID") or _require(airtable, "base_id", "airtable")

    sports = []
    for key, body in sports_raw.items():
        where = f"sports.{key}"
        if not isinstance(body, dict):
            raise ConfigError(f"{where}: must be a mapping")

        source_raw = _require(body, "source", where)
        source = SourceConfig(
            url=_require(source_raw, "url", f"{where}.source"),
            max_chars=int(source_raw.get("max_chars", 40000)),
            min_extractions=int(source_raw.get("min_extractions", 1)),
            hint=str(source_raw.get("hint", "") or "").strip(),
        )

        strategy = _require(body, "match_strategy", where)
        if strategy not in VALID_STRATEGIES:
            raise ConfigError(
                f"{where}: match_strategy '{strategy}' is not one of {sorted(VALID_STRATEGIES)}"
            )

        writes = _require(body, "writes", where)
        if not isinstance(writes, list) or not writes:
            raise ConfigError(f"{where}: writes must be a non-empty list")

        tie_break = body.get("tie_break")
        if tie_break not in VALID_TIE_BREAKS:
            raise ConfigError(f"{where}: unknown tie_break '{tie_break}'")

        select_all, select_tv_is = _parse_select(_require(body, "select", where), where)

        default_tv = body.get("default_tv")
        default_tv_max_days = body.get("default_tv_max_days")
        if default_tv and default_tv_max_days is None:
            raise ConfigError(
                f"{where}: default_tv is set but default_tv_max_days is not. "
                "The horizon is required so far-out fixtures aren't defaulted by accident."
            )

        aliases = body.get("aliases") or [key]
        channel_map = body.get("channel_map") or {}
        if not isinstance(channel_map, dict):
            raise ConfigError(f"{where}: channel_map must be a mapping")

        sports.append(
            SportConfig(
                key=key,
                aliases=[str(a).strip().lower() for a in aliases],
                source=source,
                match_strategy=strategy,
                writes=[str(w) for w in writes],
                channel_map={str(k): str(v) for k, v in channel_map.items()},
                channel_fallback=(
                    str(body["channel_fallback"])
                    if body.get("channel_fallback") is not None
                    else None
                ),
                channel_patterns=_parse_channel_patterns(body.get("channel_patterns"), where),
                ignore_unmatched_channels=bool(body.get("ignore_unmatched_channels", False)),
                name_aliases=_parse_name_aliases(body.get("name_aliases"), where),
                select_all=select_all,
                select_tv_is=select_tv_is,
                default_tv=default_tv,
                default_tv_max_days=(
                    int(default_tv_max_days) if default_tv_max_days is not None else None
                ),
                tie_break=tie_break,
                model=body.get("model", defaults.get("model", "claude-haiku-4-5")),
                max_tokens=int(body.get("max_tokens", defaults.get("max_tokens", 8000))),
                name_match_threshold=float(
                    body.get(
                        "name_match_threshold", defaults.get("name_match_threshold", 0.82)
                    )
                ),
                date_tolerance_days=int(
                    body.get("date_tolerance_days", defaults.get("date_tolerance_days", 3))
                ),
                ambiguity_margin=float(
                    body.get("ambiguity_margin", defaults.get("ambiguity_margin", 0.05))
                ),
                max_default_writes=int(
                    body.get("max_default_writes", defaults.get("max_default_writes", 12))
                ),
                request_timeout=int(
                    body.get("request_timeout", defaults.get("request_timeout", 30))
                ),
            )
        )

    return Config(
        base_id=base_id,
        table=airtable.get("table", "Fixtures"),
        window_days=int(defaults.get("window_days", 10)),
        sports=sports,
    )
