import datetime as dt
import re
import unicodedata
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from enum import Enum

from .config import SourceConfig, SportConfig

# Values that mean "nothing meaningful is here yet".
EMPTY_VALUES = {"", "tbc", "none", "n/a"}

# Dropped before comparing names so "Bohemian FC" matches "Bohemians".
# Club designators, dropped before comparing names so "Bohemian FC" matches
# "Bohemians" and "VIking FK" matches "Viking".
NOISE_TOKENS = {
    "fc", "afc", "cf", "sc", "sk", "fk", "tc", "aif",
    "club", "the", "utd", "united",
}


class Outcome(str, Enum):
    WRITE = "write"          # matched in source, value changes
    DEFAULT = "default"      # absent from source => the configured default applies
    FLAG = "flag"            # we think we know something, but not confidently enough to write
    NO_CHANGE = "no_change"  # already correct, or nothing to say


@dataclass
class Decision:
    record_id: str
    fixture_id: str
    label: str
    date: str
    sport_key: str
    outcome: Outcome
    reason: str
    fields: dict = field(default_factory=dict)     # only fields whose value actually changes
    previous: dict = field(default_factory=dict)
    overwrites: list[str] = field(default_factory=list)  # changed fields that held a real value
    confidence: float = 0.0
    source_note: str = ""
    # True when *this* source actually had the fixture, whatever the outcome.
    # Distinguishes "the source has it and nothing needs changing" from "the
    # source doesn't have it" -- only the latter may fall through to a
    # lower-priority source.
    found: bool = False

    @property
    def writable(self) -> bool:
        return self.outcome in (Outcome.WRITE, Outcome.DEFAULT) and bool(self.fields)


def normalise_name(value: str) -> str:
    if not value:
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = text.encode("ascii", "ignore").decode("ascii").lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    tokens = [t for t in text.split() if t not in NOISE_TOKENS]
    return " ".join(tokens) or text.strip()


def similarity(a: str, b: str) -> float:
    """
    How alike are two names, 0..1.

    A plain character ratio is not enough on its own. Broadcasters pad event
    names with series prefixes and round suffixes -- Airtable holds "BMW
    Championship" while Sky lists "Fedex Playoffs BMW Championship Day 1 PGA
    Tour Golf" -- and SequenceMatcher punishes that length gap hard enough
    (0.68) to fall under the match threshold. So when every token of the
    shorter name appears in the longer one, treat it as a strong match.

    The two-token floor stops a single common word ("Rovers", "Championship")
    from carrying a match on its own, and the score stays just under 1.0 so an
    exact match still outranks a containment one.
    """
    na, nb = normalise_name(a), normalise_name(b)
    if not na or not nb:
        return 0.0

    ratio = SequenceMatcher(None, na, nb).ratio()

    tokens_a, tokens_b = set(na.split()), set(nb.split())
    shorter, longer = sorted((tokens_a, tokens_b), key=len)
    if len(shorter) >= 2 and shorter <= longer:
        return max(ratio, 0.95)

    return ratio


def is_empty(value) -> bool:
    return str(value or "").strip().lower() in EMPTY_VALUES


def parse_date(value: str) -> dt.date | None:
    """ISO date or datetime string -> date. None when unparseable."""
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value).split("T", 1)[0])
    except ValueError:
        return None


_parse_date = parse_date


def canonicalise(sport: SportConfig, value: str) -> str:
    """
    Fold a name onto its canonical form via the sport's alias map.

    Fuzzy matching cannot bridge an abbreviation: "Manchester United" against
    "Man Utd" scores 0.46. Aliases close that gap declaratively, in config.
    """
    normalised = normalise_name(value)
    return sport.name_aliases.get(normalised, normalised)


def _score(sport: SportConfig, fields: dict, listing) -> float:
    team_a = canonicalise(sport, fields.get("TeamA", ""))
    team_b = canonicalise(sport, fields.get("TeamB", ""))

    if sport.match_strategy == "teams":
        home = canonicalise(sport, listing.home)
        away = canonicalise(sport, listing.away)
        # Sources sometimes list the fixture the other way round, so try both.
        direct = min(similarity(team_a, home), similarity(team_b, away))
        swapped = min(similarity(team_a, away), similarity(team_b, home))
        return max(direct, swapped)

    # event_round: the event name identifies it; the date separates the rounds.
    return similarity(team_a, canonicalise(sport, listing.event))


def _listing_label(sport: SportConfig, listing) -> str:
    if sport.match_strategy == "teams":
        return f"{listing.home} v {listing.away}"
    return f"{listing.event}" + (f" ({listing.round})" if listing.round else "")


def _time_sort_key(listing):
    """Listings with no time sort last so a real time always wins the tie-break."""
    return (listing.time is None, listing.time or "")


def resolve_channel(source: SourceConfig, raw_channel: str) -> str | None:
    """
    Channel name -> the value to store, tolerant of casing and whitespace.

    Tried in order: exact channel_map, then channel_patterns (substring, for
    families like "Sky Sports <anything>"), then channel_fallback. Returns None
    when nothing matches, which is the signal to flag -- or, when the sport
    sets ignore_unmatched_channels, to drop the listing.
    """
    target = normalise_name(raw_channel)

    for name, slug in source.channel_map.items():
        if normalise_name(name) == target:
            return slug

    for needle, value in source.channel_patterns:
        if needle in target:
            return value

    return source.channel_fallback




def _build_decision(sport: SportConfig, source: SourceConfig, record: dict, listing, confidence: float) -> Decision:
    fields = record.get("fields", {})
    label = _fixture_label(sport, fields)
    row_date = str(fields.get("Date", ""))

    base = dict(
        record_id=record["id"],
        fixture_id=str(fields.get("FixtureID", "")),
        label=label,
        date=row_date,
        sport_key=sport.key,
        confidence=confidence,
        source_note=f"{_listing_label(sport, listing)} on {listing.channel} ({listing.date})",
    )

    slug = resolve_channel(source, listing.channel)
    if slug is None:
        return Decision(
            outcome=Outcome.FLAG,
            found=True,
            reason=f"channel {listing.channel!r} is not in channel_map",
            **base,
        )

    proposed = {}
    if "TV" in sport.writes:
        proposed["TV"] = slug
    if "Time" in sport.writes and listing.time:
        proposed["Time"] = listing.time

    changed, previous, overwrites = {}, {}, []
    for key, new_value in proposed.items():
        current = fields.get(key)
        if str(current or "").strip() == str(new_value).strip():
            continue
        changed[key] = new_value
        previous[key] = current
        if not is_empty(current):
            overwrites.append(key)

    if not changed:
        return Decision(
            outcome=Outcome.NO_CHANGE, found=True, reason="already correct", **base
        )

    return Decision(
        outcome=Outcome.WRITE,
        found=True,
        reason="matched in source",
        fields=changed,
        previous=previous,
        overwrites=overwrites,
        **base,
    )


def _fixture_label(sport: SportConfig, fields: dict) -> str:
    team_a = fields.get("TeamA", "") or "?"
    team_b = fields.get("TeamB", "") or ""
    if sport.match_strategy == "teams":
        return f"{team_a} v {team_b}".strip()
    return f"{team_a} — {team_b}".strip(" —")


def _base_fields(sport: SportConfig, record: dict, note: str) -> dict:
    fields = record.get("fields", {})
    return dict(
        record_id=record["id"],
        fixture_id=str(fields.get("FixtureID", "")),
        label=_fixture_label(sport, fields),
        date=str(fields.get("Date", "")),
        sport_key=sport.key,
        confidence=0.0,
        source_note=note,
    )


def _absent(sport: SportConfig, record: dict) -> Decision:
    """
    Not in *this* source. Deliberately does not apply the sport's default --
    with several sources that call can only be made once every one has been
    tried, so it lives in decide() instead.
    """
    return Decision(
        outcome=Outcome.NO_CHANGE,
        reason="absent from source",
        **_base_fields(sport, record, "not present in source"),
    )


def _default_decision(sport: SportConfig, record: dict, today: dt.date) -> Decision:
    """Every source has been tried and none had it. Apply the default, if any."""
    fields = record.get("fields", {})
    row_date = _parse_date(fields.get("Date"))
    base = dict(
        record_id=record["id"],
        fixture_id=str(fields.get("FixtureID", "")),
        label=_fixture_label(sport, fields),
        date=str(fields.get("Date", "")),
        sport_key=sport.key,
        confidence=0.0,
        source_note="not present in source",
    )

    if not sport.default_tv or "TV" not in sport.writes:
        return Decision(outcome=Outcome.NO_CHANGE, reason="absent from source", **base)

    # Absence only means "not on TV" out to the configured horizon. Beyond it,
    # a fixture may simply not have had its pick announced yet.
    if row_date is None:
        return Decision(outcome=Outcome.FLAG, reason="row has an unparseable Date", **base)

    days_out = (row_date - today).days
    if days_out > sport.default_tv_max_days:
        return Decision(
            outcome=Outcome.NO_CHANGE,
            reason=f"{days_out}d out, beyond default_tv_max_days={sport.default_tv_max_days}",
            **base,
        )

    current = fields.get("TV")
    if str(current or "").strip() == sport.default_tv:
        return Decision(outcome=Outcome.NO_CHANGE, reason="already correct", **base)

    return Decision(
        outcome=Outcome.DEFAULT,
        reason=f"absent from source => {sport.default_tv}",
        fields={"TV": sport.default_tv},
        previous={"TV": current},
        overwrites=[] if is_empty(current) else ["TV"],
        **base,
    )


# Ranked worst-to-best, so a later source's finding can replace an earlier
# source's silence but never the other way round.
_OUTCOME_RANK = {Outcome.NO_CHANGE: 0, Outcome.FLAG: 1, Outcome.DEFAULT: 2, Outcome.WRITE: 3}


def decide(
    sport: SportConfig,
    listings_by_source: list[tuple[SourceConfig, list]],
    record: dict,
    today: dt.date,
) -> Decision:
    """
    One Airtable row -> one Decision, across every source for the sport.

    Sources are tried in configured order and the first confident match wins:
    UCL checks Virgin Media before live-footballontv, so a game shown
    free-to-air here is recorded as vmone/vmtwo rather than tnt even though
    both list it.

    A source that merely doesn't have the fixture falls through quietly. A
    source that has something to say but not confidently -- an ambiguous match,
    a date that moved -- is kept and reported if nothing better turns up, so a
    real signal is never lost to a later source's silence.
    """
    best = None
    for source, listings in listings_by_source:
        decision = decide_in_source(sport, source, record, listings, today)
        if decision.found:
            # This source has the fixture, so it settles the answer -- including
            # when the value is already correct. Without this a row correctly
            # reading `vmtwo` from Virgin Media would be overwritten with `tnt`
            # by the fallback source, which also lists the same game.
            return decision
        if best is None or _OUTCOME_RANK[decision.outcome] > _OUTCOME_RANK[best.outcome]:
            best = decision

    # No source had it outright. A near-match (moved date, ambiguity) is still
    # a real signal and is reported rather than being lost to the default.
    if best is not None and best.outcome is Outcome.FLAG:
        return best

    return _default_decision(sport, record, today)


def decide_in_source(
    sport: SportConfig, source: SourceConfig, record: dict, listings: list, today: dt.date
) -> Decision:
    """
    One Airtable row -> one Decision.

    Order matters. The near-match pass has to run before anything is declared
    absent: a fixture whose TV pick moved it to another day fails the exact-date
    check, and without the near-match pass it would fall through to "not in the
    source" and get silently stamped with the default.
    """
    fields = record.get("fields", {})
    row_date = _parse_date(fields.get("Date"))

    scored = sorted(
        ((_score(sport, fields, listing), listing) for listing in listings),
        key=lambda pair: pair[0],
        reverse=True,
    )
    candidates = [(s, l) for s, l in scored if s >= sport.name_match_threshold]

    if not candidates:
        return _absent(sport, record)

    exact = [(s, l) for s, l in candidates if _parse_date(l.date) == row_date]

    if exact:
        if len(exact) > 1:
            resolved = _resolve_multiple(sport, source, exact)
            if resolved is None:
                best_score, _ = exact[0]
                return Decision(
                    record_id=record["id"],
                    fixture_id=str(fields.get("FixtureID", "")),
                    label=_fixture_label(sport, fields),
                    date=str(fields.get("Date", "")),
                    sport_key=sport.key,
                    outcome=Outcome.FLAG,
                    reason=f"{len(exact)} source listings match and disagree",
                    confidence=best_score,
                    source_note="; ".join(
                        f"{_listing_label(sport, l)} on {l.channel}" for _, l in exact[:4]
                    ),
                )
            score, listing = resolved
        else:
            score, listing = exact[0]

        return _build_decision(sport, source, record, listing, score)

    # Name matched, date didn't. Inside the tolerance window this is the
    # moved-fixture case -- report it, never write it.
    if row_date is not None:
        near = [
            (s, l)
            for s, l in candidates
            if (d := _parse_date(l.date)) is not None
            and abs((d - row_date).days) <= sport.date_tolerance_days
        ]
        if near:
            score, listing = near[0]
            return Decision(
                record_id=record["id"],
                fixture_id=str(fields.get("FixtureID", "")),
                label=_fixture_label(sport, fields),
                date=str(fields.get("Date", "")),
                sport_key=sport.key,
                outcome=Outcome.FLAG,
                reason=(
                    f"date mismatch: we have {row_date.isoformat()}, "
                    f"source says {listing.date}"
                ),
                confidence=score,
                source_note=f"{_listing_label(sport, listing)} on {listing.channel}",
            )

    return _absent(sport, record)


def _resolve_multiple(sport: SportConfig, source: SourceConfig, exact: list):
    """
    Several listings match the same row on the same date.

    Resolvable when the configured tie-break applies (one event airing several
    times in a day) or when they all agree anyway. Otherwise it's ambiguous and
    the caller flags it.
    """
    channels = {resolve_channel(source, l.channel) or l.channel for _, l in exact}

    if sport.tie_break == "earliest_time":
        return min(exact, key=lambda pair: _time_sort_key(pair[1]))

    if len(channels) == 1:
        return exact[0]

    top = exact[0][0]
    contenders = [pair for pair in exact if top - pair[0] <= sport.ambiguity_margin]
    if len(contenders) == 1:
        return contenders[0]

    return None


def selectable(sport: SportConfig, record: dict) -> bool:
    """Does this row fall into the sport's configured candidate set?"""
    fields = record.get("fields", {})
    if not sport.matches_sport_value(fields.get("Sport", "")):
        return False
    if sport.select_all:
        return True
    return str(fields.get("TV") or "").strip().lower() in sport.select_tv_is
