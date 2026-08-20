import datetime as dt
import re
import unicodedata
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from enum import Enum

from .config import SportConfig

# Values that mean "nothing meaningful is here yet".
EMPTY_VALUES = {"", "tbc", "none", "n/a"}

# Dropped before comparing names so "Bohemian FC" matches "Bohemians".
NOISE_TOKENS = {"fc", "afc", "cf", "sc", "afc", "club", "the", "utd", "united"}


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


def _parse_date(value: str) -> dt.date | None:
    if not value:
        return None
    try:
        return dt.date.fromisoformat(str(value).split("T", 1)[0])
    except ValueError:
        return None


def _score(sport: SportConfig, fields: dict, listing) -> float:
    team_a = fields.get("TeamA", "")
    team_b = fields.get("TeamB", "")

    if sport.match_strategy == "teams":
        # Sources sometimes list the fixture the other way round, so try both.
        direct = min(similarity(team_a, listing.home), similarity(team_b, listing.away))
        swapped = min(similarity(team_a, listing.away), similarity(team_b, listing.home))
        return max(direct, swapped)

    # event_round: the event name identifies it; the date separates the rounds.
    return similarity(team_a, listing.event)


def _listing_label(sport: SportConfig, listing) -> str:
    if sport.match_strategy == "teams":
        return f"{listing.home} v {listing.away}"
    return f"{listing.event}" + (f" ({listing.round})" if listing.round else "")


def _time_sort_key(listing):
    """Listings with no time sort last so a real time always wins the tie-break."""
    return (listing.time is None, listing.time or "")


def _resolve_channel(sport: SportConfig, raw_channel: str) -> str | None:
    """
    Channel name -> the value to store, tolerant of casing and whitespace.

    Returns None only when the channel is unmapped *and* the sport has no
    fallback, which is the signal to flag rather than write.
    """
    target = normalise_name(raw_channel)
    for name, slug in sport.channel_map.items():
        if normalise_name(name) == target:
            return slug
    return sport.channel_fallback


def _build_decision(sport: SportConfig, record: dict, listing, confidence: float) -> Decision:
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

    slug = _resolve_channel(sport, listing.channel)
    if slug is None:
        return Decision(
            outcome=Outcome.FLAG,
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
        return Decision(outcome=Outcome.NO_CHANGE, reason="already correct", **base)

    return Decision(
        outcome=Outcome.WRITE,
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


def _not_found_decision(
    sport: SportConfig, record: dict, today: dt.date, near_misses: list
) -> Decision:
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


def decide(sport: SportConfig, record: dict, listings: list, today: dt.date) -> Decision:
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
        return _not_found_decision(sport, record, today, near_misses=[])

    exact = [(s, l) for s, l in candidates if _parse_date(l.date) == row_date]

    if exact:
        if len(exact) > 1:
            resolved = _resolve_multiple(sport, exact)
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

        return _build_decision(sport, record, listing, score)

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

    return _not_found_decision(sport, record, today, near_misses=candidates)


def _resolve_multiple(sport: SportConfig, exact: list):
    """
    Several listings match the same row on the same date.

    Resolvable when the configured tie-break applies (one event airing several
    times in a day) or when they all agree anyway. Otherwise it's ambiguous and
    the caller flags it.
    """
    channels = {_resolve_channel(sport, l.channel) or l.channel for _, l in exact}

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
