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
    """
    One page a sport is looked up in.

    A sport can have several, tried in listed order: UCL checks the Virgin
    Media guide first (free-to-air here, so it wins) and only falls back to
    live-footballontv for fixtures Virgin Media isn't showing. Channel rules
    live here rather than on the sport because each page names channels its
    own way -- "Channel two" on one, "TNT Sports 2" on the other.
    """

    name: str
    url: str
    max_chars: int
    min_extractions: int
    channel_map: dict[str, str]
    channel_patterns: list[tuple[str, str]]
    channel_fallback: str | None
    ignore_unmatched_channels: bool
    # Free-text guidance appended to the extraction prompt, for pages whose
    # layout needs explaining (where the channel lives, which listings to
    # ignore, how dates are formatted). Keeping this in config is what lets a
    # new source with an awkward layout be added without touching code.
    hint: str = ""


@dataclass
class SportConfig:
    key: str
    aliases: list[str]
    sources: list[SourceConfig]
    match_strategy: str
    writes: list[str]
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


def _parse_sources(body: dict, where: str) -> list[SourceConfig]:
    """
    `sources:` is a list, tried in listed order. A single-source sport still
    writes it as a one-entry list, so there is only one shape to reason about.
    """
    raw = _require(body, "sources", where)
    if not isinstance(raw, list) or not raw:
        raise ConfigError(f"{where}: sources must be a non-empty list")

    sources = []
    for index, entry in enumerate(raw):
        spot = f"{where}.sources[{index}]"
        if not isinstance(entry, dict):
            raise ConfigError(f"{spot}: must be a mapping")

        channel_map = entry.get("channel_map") or {}
        if not isinstance(channel_map, dict):
            raise ConfigError(f"{spot}: channel_map must be a mapping")

        sources.append(
            SourceConfig(
                name=str(entry.get("name") or f"source{index + 1}"),
                url=_require(entry, "url", spot),
                max_chars=int(entry.get("max_chars", 40000)),
                # Default 1, but a fallback source legitimately lists nothing
                # some weeks -- the Europa League page currently says "No
                # Upcoming TV Fixtures". Set 0 there so an empty result reads
                # as "nothing to match" rather than a broken scrape. Only safe
                # because those sports have no default_tv to mass-apply.
                min_extractions=int(entry.get("min_extractions", 1)),
                channel_map={str(k): str(v) for k, v in channel_map.items()},
                channel_patterns=_parse_channel_patterns(
                    entry.get("channel_patterns"), spot
                ),
                channel_fallback=(
                    str(entry["channel_fallback"])
                    if entry.get("channel_fallback") is not None
                    else None
                ),
                ignore_unmatched_channels=bool(
                    entry.get("ignore_unmatched_channels", False)
                ),
                hint=str(entry.get("hint", "") or "").strip(),
            )
        )
    return sources


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

        sources = _parse_sources(body, where)

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

        sports.append(
            SportConfig(
                key=key,
                aliases=[str(a).strip().lower() for a in aliases],
                sources=sources,
                match_strategy=strategy,
                writes=[str(w) for w in writes],
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
